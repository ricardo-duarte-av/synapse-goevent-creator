// Package shadow checks, for every write Synapse served in an allowlisted
// room, that this worker would have built the same event.
//
// It writes nothing. Before the request is proxied it reads what Synapse is
// about to read: the room's forward extremities and stream position, the
// client's transaction, and the state the event would be built on. After
// Synapse answers, it loads the event Synapse persisted and re-derives every
// decision: prev events, depth, auth events, state before and after,
// transaction and state deduplication, the auth rules, the content. Then it
// rebuilds and signs the event. Ed25519 signatures are deterministic, so a
// rebuild that matches Synapse's must reproduce its event ID exactly.
//
// Each comparison is counted as goevent_shadow_checks_total{endpoint,check,
// result}, and every mismatch is logged with both sides.
package shadow

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/crypto/canonicaljson"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/auth"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/build"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdu"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdustore"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pushcalc"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pyjson"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

// Result values.
const (
	Match    = "match"
	Mismatch = "mismatch"
	Skipped  = "skipped"
	Error    = "error"
)

var checks = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goevent_shadow_checks_total",
	Help: "Shadow comparisons of what this worker would have built against what Synapse persisted, by endpoint, check and result.",
}, []string{"endpoint", "check", "result"})

// StickyParam is StickyEvent.QUERY_PARAM_NAME (api/constants.py).
const StickyParam = "org.matrix.msc4354.sticky_duration_ms"

// DelayParam marks an MSC4140 delayed event, which creates no event now.
const DelayParam = "org.matrix.msc4140.delay"

// MaxBody is the largest request body read for checking. Synapse refuses
// events over 64 KiB, so a bigger body cannot become an event as sent.
const MaxBody = 65536

// Store is what the checker reads.
type Store interface {
	PrevEventsForRoom(ctx context.Context, roomID string) ([]string, error)
	RoomMaxStream(ctx context.Context, roomID string) (int64, error)
	RoomVersion(ctx context.Context, roomID string) (string, error)
	TxnEvent(ctx context.Context, roomID, userID, deviceID, txnID string) (string, error)
	GetStoredEvents(ctx context.Context, ids []string) (map[string]*pdustore.StoredEvent, map[string]error, error)
	Depths(ctx context.Context, ids []string) (map[string]int64, error)
	StateGroupForEvent(ctx context.Context, id string) (int64, error)
	StateForGroup(ctx context.Context, group int64) (pdu.StateMap, error)
	StateDelta(ctx context.Context, group int64) (prev int64, hasPrev bool, delta pdu.StateMap, err error)
	NewMembership(ctx context.Context, roomID string, after int64, membership, target, server string) (string, error)
}

// Authenticator says who a request is from.
type Authenticator interface {
	AuthenticateAs(ctx context.Context, creds auth.Credentials) (auth.Verdict, error)
}

// Options configure a Checker.
type Options struct {
	Store Store
	Auth  Authenticator
	Key   *build.Key
	// Push computes push actions; nil skips the push check.
	Push       *pushcalc.Calculator
	ServerName string
	// Sticky is experimental_features.msc4354_enabled: without it Synapse
	// ignores the sticky query parameter.
	Sticky bool
	// Concurrency bounds checks in flight; more are dropped and counted.
	Concurrency int
	Timeout     time.Duration
	Log         zerolog.Logger
}

// Checker runs shadow checks.
type Checker struct {
	o   Options
	sem chan struct{}
}

// New builds a Checker.
func New(o Options) *Checker {
	if o.Concurrency <= 0 {
		o.Concurrency = 8
	}
	if o.Timeout <= 0 {
		o.Timeout = 30 * time.Second
	}
	return &Checker{o: o, sem: make(chan struct{}, o.Concurrency)}
}

// Pending is what was read before the request was proxied.
type Pending struct {
	req   route.Request
	query url.Values
	body  []byte
	// tooLarge: the body was over MaxBody, so content is not compared.
	tooLarge bool

	user, device string

	prevs     []string
	maxStream int64
	rv        *pdu.RoomVersion
	rvID      string

	// txnEvent is the event this transaction already made, "" for none.
	txnEvent string
	// before is the state at prevs, nil if not computed (several prevs).
	before pdu.StateMap

	// skip, when set, is why nothing will be checked.
	skip string
	err  error
}

// Prepare reads the room before the request is proxied. r.Body is replaced
// by an equivalent reader, so the request can be forwarded unchanged.
// Prepare never fails the request: problems are recorded in the result.
func (c *Checker) Prepare(ctx context.Context, r *http.Request, req route.Request) *Pending {
	p := &Pending{req: req, query: r.URL.Query()}

	if r.Body != nil {
		head, err := io.ReadAll(io.LimitReader(r.Body, MaxBody+1))
		if err != nil {
			p.err = fmt.Errorf("read body: %w", err)
		}
		if len(head) > MaxBody {
			p.tooLarge = true
		}
		r.Body = readCloser{io.MultiReader(bytes.NewReader(head), r.Body), r.Body}
		p.body = head
		// As json.loads holds it: a repeated key's last value.
		if d, err := pyjson.Dedupe(head); err == nil {
			p.body = d
		}
	}
	if p.err != nil {
		return p
	}
	if p.query.Has(DelayParam) {
		p.skip = "delayed"
		return p
	}
	if req.Room == "" || req.Room[0] != '!' {
		p.skip = "alias"
		return p
	}

	ctx, cancel := context.WithTimeout(ctx, c.o.Timeout)
	defer cancel()

	v, err := c.o.Auth.AuthenticateAs(ctx, auth.ExtractCredentials(r))
	switch {
	case err != nil:
		p.err = fmt.Errorf("whoami: %w", err)
		return p
	case !v.Valid:
		p.skip = "unauthenticated"
		return p
	}
	p.user, p.device = v.UserID, v.DeviceID

	if p.rvID, err = c.o.Store.RoomVersion(ctx, req.Room); err != nil {
		p.err = err
		return p
	}
	rv, ok := pdu.LookupRoomVersion(p.rvID)
	if !ok {
		p.skip = "room_version"
		return p
	}
	p.rv = rv

	// Synapse checks the transaction first (create_and_send_nonmember_event),
	// then reads the extremities (create_new_client_event).
	if req.TxnID != "" && p.device != "" {
		id, err := c.o.Store.TxnEvent(ctx, req.Room, p.user, p.device, req.TxnID)
		switch {
		case err == nil:
			p.txnEvent = id
		case !errors.Is(err, pdustore.ErrNotFound):
			p.err = err
			return p
		}
	}
	if p.prevs, err = c.o.Store.PrevEventsForRoom(ctx, req.Room); err != nil {
		p.err = err
		return p
	}
	if p.maxStream, err = c.o.Store.RoomMaxStream(ctx, req.Room); err != nil {
		p.err = err
		return p
	}
	if len(p.prevs) == 1 {
		g, err := c.o.Store.StateGroupForEvent(ctx, p.prevs[0])
		if err != nil {
			p.err = err
			return p
		}
		if p.before, err = c.o.Store.StateForGroup(ctx, g); err != nil {
			p.err = err
			return p
		}
	}
	return p
}

type readCloser struct {
	io.Reader
	io.Closer
}

// Check compares, in the background, what Synapse did with what we would
// have done. status and body are Synapse's answer.
func (c *Checker) Check(p *Pending, status int, body []byte) {
	ep := string(p.req.Endpoint)
	if status != http.StatusOK {
		// A refusal is Synapse's decision too; checking that we would refuse
		// needs the native path's validation, which comes with it.
		checks.WithLabelValues(ep, "request", "status_"+strconv.Itoa(status)).Inc()
		return
	}
	select {
	case c.sem <- struct{}{}:
	default:
		checks.WithLabelValues(ep, "request", "dropped").Inc()
		return
	}
	go func() {
		defer func() { <-c.sem }()
		ctx, cancel := context.WithTimeout(context.Background(), c.o.Timeout)
		defer cancel()
		c.check(ctx, p, body)
	}()
}

// run is one check's results, so the summary can list them.
type run struct {
	ep      string
	eventID string
	log     zerolog.Logger
	results map[string]string
}

func (r *run) record(check, result string, detail func(*zerolog.Event)) {
	r.results[check] = result
	checks.WithLabelValues(r.ep, check, result).Inc()
	if result == Mismatch || result == Error {
		e := r.log.Warn().Str("check", check).Str("result", result).Str("event_id", r.eventID)
		if detail != nil {
			detail(e)
		}
		e.Msg("Shadow check disagrees with Synapse")
	}
}

func (r *run) compare(check string, ok bool, detail func(*zerolog.Event)) {
	if ok {
		r.record(check, Match, nil)
	} else {
		r.record(check, Mismatch, detail)
	}
}

func (c *Checker) check(ctx context.Context, p *Pending, resp []byte) {
	r := &run{ep: string(p.req.Endpoint), log: c.o.Log.With().Str("endpoint", string(p.req.Endpoint)).
		Str("room_id", p.req.Room).Logger(), results: map[string]string{}}
	defer func() {
		if r.eventID == "" {
			return
		}
		r.log.Info().Str("event_id", r.eventID).Interface("results", r.results).Msg("Shadow check")
	}()

	switch {
	case p.err != nil:
		r.record("prepare", Error, func(e *zerolog.Event) { e.Err(p.err) })
		return
	case p.skip != "":
		r.record("request", "skip_"+p.skip, nil)
		return
	}

	eventID, err := c.findEvent(ctx, p, resp)
	if errors.Is(err, pdustore.ErrNotFound) && (p.req.Endpoint == route.Membership || p.req.Endpoint == route.Join) {
		// No new event: update_membership_locked's duplicate rule
		// (handlers/room_member.py) returned the existing one.
		c.checkMembershipNoop(ctx, r, p)
		return
	}
	if err != nil {
		r.record("find_event", Error, func(e *zerolog.Event) { e.Err(err).Bytes("response", resp) })
		return
	}
	r.eventID = eventID

	stored, bad, err := c.o.Store.GetStoredEvents(ctx, []string{eventID})
	if err != nil || bad[eventID] != nil || stored[eventID] == nil {
		r.record("load_event", Error, func(e *zerolog.Event) { e.AnErr("err", err).AnErr("parse", bad[eventID]) })
		return
	}
	se := stored[eventID]
	ev := se.Event

	// Deduplication: the event existed before the request.
	existed := se.StreamOrdering <= p.maxStream
	if p.req.TxnID != "" && p.device != "" {
		predicted := p.txnEvent != ""
		r.compare("txn_dedup", predicted == existed && (!predicted || p.txnEvent == eventID), func(e *zerolog.Event) {
			e.Str("predicted_event", p.txnEvent).Bool("existed", existed)
		})
		if predicted {
			return
		}
	}
	if p.req.Endpoint == route.State {
		predicted, prevID := c.predictStateDedup(ctx, p)
		r.compare("state_dedup", predicted == existed && (!predicted || prevID == eventID), func(e *zerolog.Event) {
			e.Bool("predicted", predicted).Str("predicted_event", prevID).Bool("existed", existed)
		})
		if existed {
			return
		}
	}
	if existed {
		r.record("new_event", Mismatch, func(e *zerolog.Event) {
			e.Int64("stream_ordering", se.StreamOrdering).Int64("max_stream_before", p.maxStream)
		})
		return
	}

	r.compare("sender", ev.Sender == p.user, func(e *zerolog.Event) { e.Str("stored", ev.Sender).Str("requester", p.user) })

	if !sameSet(ev.PrevEvents, p.prevs) && c.raced(ctx, ev.PrevEvents, p.maxStream) {
		// Another event was persisted between our read and Synapse's: what
		// follows depends on it, and is not compared.
		r.record("prev_events", Skipped, func(e *zerolog.Event) {
			e.Str("reason", "race").Strs("stored", ev.PrevEvents).Strs("extremities_before", p.prevs)
		})
		return
	}
	r.compare("prev_events", sameSet(ev.PrevEvents, p.prevs), func(e *zerolog.Event) {
		e.Strs("stored", ev.PrevEvents).Strs("extremities_before", p.prevs)
	})

	depths, err := c.o.Store.Depths(ctx, ev.PrevEvents)
	if err != nil {
		r.record("depth", Error, func(e *zerolog.Event) { e.Err(err) })
	} else {
		ds := slices.Collect(maps.Values(depths))
		want := build.Depth(ds)
		r.compare("depth", len(depths) == len(ev.PrevEvents) && want == ev.Depth, func(e *zerolog.Event) {
			e.Int64("stored", ev.Depth).Int64("ours", want)
		})
	}

	// The state before, as we would compute it and as Synapse recorded it.
	ourBefore := p.before
	if !sameSet(ev.PrevEvents, p.prevs) {
		ourBefore = nil
	}
	synBefore, synAfter, shape, err := c.synapseState(ctx, ev)
	if err != nil {
		r.record("state_before", Error, func(e *zerolog.Event) { e.Err(err) })
		return
	}
	if ourBefore == nil {
		r.record("state_before", Skipped, nil)
		ourBefore = synBefore
	} else if synBefore != nil {
		r.compare("state_before", maps.Equal(ourBefore, synBefore), func(e *zerolog.Event) {
			e.Interface("diff", stateDiff(ourBefore, synBefore))
		})
	}

	ourAfter := maps.Clone(ourBefore)
	if ev.IsState() {
		ourAfter[ev.Key()] = ev.ID
	}
	r.compare("state_after", maps.Equal(ourAfter, synAfter), func(e *zerolog.Event) {
		e.Interface("diff", stateDiff(ourAfter, synAfter))
	})
	r.compare("state_group_shape", shape == "", func(e *zerolog.Event) { e.Str("shape", shape) })

	ourAuth, err := pdu.ComputeAuthEvents(p.rv, ev, ourBefore)
	if err != nil {
		r.record("auth_events", Error, func(e *zerolog.Event) { e.Err(err) })
	} else {
		r.compare("auth_events", sameSet(ourAuth, ev.AuthEvents), func(e *zerolog.Event) {
			e.Strs("stored", ev.AuthEvents).Strs("ours", ourAuth)
		})
	}

	if err := c.checkAuthRules(ctx, ev); err != nil {
		r.record("auth_rules", Mismatch, func(e *zerolog.Event) { e.Err(err) })
	} else {
		r.record("auth_rules", Match, nil)
	}

	content, contentOK := c.expectedContent(p, ev)
	switch {
	case !contentOK:
		r.record("content", Skipped, nil)
		content = jsontext.Value(gjson.GetBytes(ev.JSON, "content").Raw)
	default:
		stored := jsontext.Value(gjson.GetBytes(ev.JSON, "content").Raw)
		a, errA := canonicaljson.CanonicalJSON(content)
		b, errB := canonicaljson.CanonicalJSON(stored)
		r.compare("content", errA == nil && errB == nil && bytes.Equal(a, b), func(e *zerolog.Event) {
			e.RawJSON("stored", stored).RawJSON("ours", content)
		})
	}

	if c.o.Push != nil {
		v := c.o.Push.Check(ctx, ev.ID, ourBefore, p.maxStream, nil)
		switch v.Outcome {
		case pushcalc.OutcomeMatch:
			r.record("push", Match, nil)
		case pushcalc.OutcomeMismatch:
			r.record("push", Mismatch, func(e *zerolog.Event) { e.Strs("diff", v.Diff) })
		case pushcalc.OutcomeSkipped:
			r.record("push", Skipped, nil)
			r.results["push"] = "skipped: " + v.Reason
		default:
			r.record("push", Error, func(e *zerolog.Event) { e.Str("outcome", v.Outcome).Str("reason", v.Reason) })
		}
	}

	// The rebuild: our decisions, with the two things we cannot predict --
	// the timestamp, and list orders Synapse leaves to chance -- taken from
	// the stored event.
	built, err := build.Build(c.proto(p, ev, content, ourAuth), c.o.Key)
	if err != nil {
		r.record("rebuild", Error, func(e *zerolog.Event) { e.Err(err) })
		return
	}
	r.compare("rebuild", built.ID == ev.ID, func(e *zerolog.Event) {
		e.Str("ours", built.ID).RawJSON("stored_json", ev.JSON).RawJSON("our_json", built.JSON)
	})
}

// findEvent is the event Synapse made for the request.
func (c *Checker) findEvent(ctx context.Context, p *Pending, resp []byte) (string, error) {
	switch p.req.Endpoint {
	case route.Send, route.State, route.Redact:
		id := gjson.GetBytes(resp, "event_id").Str
		if id == "" {
			return "", errors.New("no event_id in response")
		}
		return id, nil
	}
	membership, target := c.membershipOf(p)
	return c.o.Store.NewMembership(ctx, p.req.Room, p.maxStream, membership, target, c.o.ServerName)
}

// checkMembershipNoop: Synapse made no event, which it does only when the
// target's member event already has this sender, this membership and the
// same content. Membership content is assembled from the profile, so until
// it is built here (M5) only the sender and membership are compared.
func (c *Checker) checkMembershipNoop(ctx context.Context, r *run, p *Pending) {
	membership, target := c.membershipOf(p)
	if p.before == nil {
		r.record("membership_noop", Skipped, nil)
		return
	}
	oldID, ok := p.before[pdu.StateKey{Type: "m.room.member", StateKey: target}]
	if !ok {
		r.record("membership_noop", Mismatch, func(e *zerolog.Event) { e.Str("target", target).Str("why", "no member event before") })
		return
	}
	evs, bad, err := c.o.Store.GetStoredEvents(ctx, []string{oldID})
	if err != nil || bad[oldID] != nil || evs[oldID] == nil {
		r.record("membership_noop", Error, func(e *zerolog.Event) { e.AnErr("err", err) })
		return
	}
	old := evs[oldID].Event
	oldMembership, _ := old.Membership()
	r.eventID = oldID
	r.compare("membership_noop", old.Sender == p.user && oldMembership == membership, func(e *zerolog.Event) {
		e.Str("old_sender", old.Sender).Str("old_membership", oldMembership).Str("requested", membership)
	})
}

// membershipOf is the membership and target a membership request makes.
func (c *Checker) membershipOf(p *Pending) (membership, target string) {
	if p.req.Endpoint == route.Join {
		return "join", p.user
	}
	target = gjson.GetBytes(p.body, "user_id").Str
	switch p.req.Action {
	case "join":
		return "join", p.user
	case "leave":
		return "leave", p.user
	case "kick", "unban":
		return "leave", target
	case "ban":
		return "ban", target
	case "invite":
		return "invite", target
	}
	return p.req.Action, target
}

// predictStateDedup is deduplicate_state_event (handlers/message.py): the
// state already holds an event of this key, by this sender, with the same
// canonical content.
func (c *Checker) predictStateDedup(ctx context.Context, p *Pending) (bool, string) {
	if p.before == nil || p.tooLarge {
		return false, ""
	}
	prevID, ok := p.before[pdu.StateKey{Type: p.req.Action, StateKey: p.req.StateKey}]
	if !ok {
		return false, ""
	}
	evs, _, err := c.o.Store.GetStoredEvents(ctx, []string{prevID})
	if err != nil || evs[prevID] == nil {
		return false, ""
	}
	prev := evs[prevID].Event
	if prev.Sender != p.user {
		return false, ""
	}
	a, errA := canonicaljson.CanonicalJSON(p.body)
	b, errB := canonicaljson.CanonicalJSON([]byte(prev.Content.Raw))
	return errA == nil && errB == nil && bytes.Equal(a, b), prevID
}

// synapseState is the state before and after the event as Synapse stored
// it, and a description of anything unexpected about the group's shape.
//
// For a message event the group is the state before and after alike. For a
// state event Synapse stores a new group as a delta of exactly the event
// from the group before (UnpersistedEventContext.persist).
func (c *Checker) synapseState(ctx context.Context, ev *pdu.Event) (before, after pdu.StateMap, shape string, err error) {
	g, err := c.o.Store.StateGroupForEvent(ctx, ev.ID)
	if err != nil {
		return nil, nil, "", err
	}
	if after, err = c.o.Store.StateForGroup(ctx, g); err != nil {
		return nil, nil, "", err
	}
	if !ev.IsState() {
		return after, after, "", nil
	}
	prev, hasPrev, delta, err := c.o.Store.StateDelta(ctx, g)
	if err != nil {
		return nil, nil, "", err
	}
	if !hasPrev {
		// A full group (Synapse does this every 100 deltas): the state
		// before is unknown from the group alone.
		return nil, after, "", nil
	}
	if len(delta) != 1 || delta[ev.Key()] != ev.ID {
		shape = fmt.Sprintf("delta of %d entries, not just the event", len(delta))
	}
	if before, err = c.o.Store.StateForGroup(ctx, prev); err != nil {
		return nil, nil, "", err
	}
	return before, after, shape, nil
}

// checkAuthRules runs the auth rules on the stored event: Synapse accepted
// it, so our port must too.
func (c *Checker) checkAuthRules(ctx context.Context, ev *pdu.Event) error {
	stored, bad, err := c.o.Store.GetStoredEvents(ctx, ev.AuthEvents)
	if err != nil {
		return err
	}
	byID := map[string]*pdu.Event{}
	list := make([]*pdu.Event, 0, len(ev.AuthEvents))
	for _, id := range ev.AuthEvents {
		if bad[id] != nil {
			return fmt.Errorf("auth event %s: %w", id, bad[id])
		}
		se, ok := stored[id]
		if !ok {
			return fmt.Errorf("auth event %s missing", id)
		}
		byID[id] = se.Event
		list = append(list, se.Event)
	}
	if err := pdu.CheckStateIndependentAuthRules(ev, byID); err != nil {
		return fmt.Errorf("state independent: %w", err)
	}
	if err := pdu.CheckStateDependentAuthRules(ev, list); err != nil {
		return fmt.Errorf("state dependent: %w", err)
	}
	return nil
}

// raced reports a prev event persisted after the room was read.
func (c *Checker) raced(ctx context.Context, prevs []string, maxStream int64) bool {
	evs, _, err := c.o.Store.GetStoredEvents(ctx, prevs)
	if err != nil {
		return false
	}
	for _, se := range evs {
		if se.StreamOrdering > maxStream {
			return true
		}
	}
	return false
}

// expectedContent is the content Synapse builds from the request, when
// that is known: the body as sent for send and state, and for redactions
// the body with redacts set in versions where it moved into content
// (rest/client/room.py, RoomRedactEventRestServlet). Membership content is
// assembled from the profile, reason and more; it is compared from M5.
func (c *Checker) expectedContent(p *Pending, ev *pdu.Event) (jsontext.Value, bool) {
	if p.tooLarge || !gjson.ValidBytes(p.body) || !gjson.ParseBytes(p.body).IsObject() {
		return nil, false
	}
	switch p.req.Endpoint {
	case route.Send, route.State:
		return jsontext.Value(p.body), true
	case route.Redact:
		if gjson.GetBytes(p.body, gjson.Escape("org.matrix.msc3912.with_relations")).Exists() {
			return nil, false
		}
		if !p.rv.UpdatedRedactionRules {
			return jsontext.Value(p.body), true
		}
		obj := map[string]jsontext.Value{}
		gjson.ParseBytes(p.body).ForEach(func(k, v gjson.Result) bool {
			obj[k.Str] = jsontext.Value(v.Raw)
			return true
		})
		q, err := jsontext.AppendQuote(nil, p.req.Action)
		if err != nil {
			return nil, false
		}
		obj["redacts"] = q
		raw, err := canonicaljson.Marshal(obj)
		if err != nil {
			return nil, false
		}
		return jsontext.Value(raw), true
	}
	return nil, false
}

// proto is the event as we would build it.
func (c *Checker) proto(p *Pending, ev *pdu.Event, content jsontext.Value, ourAuth []string) build.Proto {
	pr := build.Proto{
		RoomVersion:    id.RoomVersion(p.rvID),
		RoomID:         p.req.Room,
		Sender:         p.user,
		Type:           ev.Type,
		StateKey:       ev.StateKey,
		Content:        content,
		Depth:          ev.Depth,
		OriginServerTS: ev.OriginServerTS,
		// Orders Synapse leaves to chance: the stored ones, when the sets
		// agree; otherwise ours, so the rebuild fails visibly.
		PrevEvents: p.prevs,
		AuthEvents: ourAuth,
	}
	switch p.req.Endpoint {
	case route.Send, route.Redact:
		if p.req.Endpoint == route.Send {
			pr.Type = p.req.Action
		} else {
			pr.Type = "m.room.redaction"
			redacts := p.req.Action
			pr.Redacts = &redacts
		}
	case route.State:
		pr.Type = p.req.Action
		sk := p.req.StateKey
		pr.StateKey = &sk
	}
	if sameSet(ev.PrevEvents, p.prevs) {
		pr.PrevEvents = ev.PrevEvents
	}
	if sameSet(ev.AuthEvents, ourAuth) {
		pr.AuthEvents = ev.AuthEvents
	}
	if c.o.Sticky && p.req.Endpoint == route.Send {
		if v := p.query.Get(StickyParam); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				pr.StickyDurationMS = &n
			}
		}
	}
	return pr
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(x, y)
}

// stateDiff lists the keys where two state maps differ, for logs.
func stateDiff(ours, theirs pdu.StateMap) map[string][2]string {
	out := map[string][2]string{}
	for k, v := range ours {
		if theirs[k] != v {
			out[k.Type+"|"+k.StateKey] = [2]string{v, theirs[k]}
		}
	}
	for k, v := range theirs {
		if _, ok := ours[k]; !ok {
			out[k.Type+"|"+k.StateKey] = [2]string{"", v}
		}
	}
	return out
}
