// Package creator serves /send natively: it builds the event as Synapse's
// event creator does, writes what the creator writes itself (push actions
// staging, room participation, the state group deletion bump), and hands the
// event to the room's events writer over send_events
// (EventCreationHandler.create_and_send_nonmember_event, handlers/message.py,
// v1.163.0).
//
// Anything it does not reproduce exactly is left to Synapse: the request is
// proxied, untouched, and the reason counted
// (goevent_native_fallback_total). That decision is always made before the
// first write, so a proxied request leaves nothing behind. Errors are
// Synapse's to give too, for the same reason: a request Synapse would refuse
// is proxied, and Synapse refuses it in its own words.
//
// Each native event is verified afterwards against what the events writer
// persisted (goevent_native_verify_total).
package creator

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/auth"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/build"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdu"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdustore"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pushcalc"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/replication"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/writer"
)

var (
	native = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_native_total",
		Help: "Requests served natively, by endpoint and result.",
	}, []string{"endpoint", "result"})
	fallbacks = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_native_fallback_total",
		Help: "Requests in native mode left to Synapse, by endpoint and reason. Nothing was written for them.",
	}, []string{"endpoint", "reason"})
	verifies = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_native_verify_total",
		Help: "Checks of natively created events against what the events writer persisted, by check and result.",
	}, []string{"check", "result"})
	nativeSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goevent_native_seconds",
		Help:    "Time to serve a native request, by endpoint and result.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"endpoint", "result"})
)

// Query parameters that change what /send does (rest/client/room.py).
const (
	StickyParam = "org.matrix.msc4354.sticky_duration_ms"
	DelayParam  = "org.matrix.msc4140.delay"
)

// MaxBody is the largest body read: Synapse refuses events over 64 KiB.
const MaxBody = 65536

// Store is what the creator reads.
type Store interface {
	Token(ctx context.Context, token string) (*pdustore.TokenInfo, error)
	TxnEvent(ctx context.Context, roomID, userID, deviceID, txnID string) (string, error)
	RoomVersion(ctx context.Context, roomID string) (string, error)
	PartialState(ctx context.Context, roomID string) (bool, error)
	PrevEventsForRoom(ctx context.Context, roomID string) ([]string, error)
	RoomMaxStream(ctx context.Context, roomID string) (int64, error)
	Depths(ctx context.Context, ids []string) (map[string]int64, error)
	StateGroupForEvent(ctx context.Context, id string) (int64, error)
	StateForGroup(ctx context.Context, group int64) (pdu.StateMap, error)
	StateDelta(ctx context.Context, group int64) (prev int64, hasPrev bool, delta pdu.StateMap, err error)
	GetStoredEvents(ctx context.Context, ids []string) (map[string]*pdustore.StoredEvent, map[string]error, error)
	RejectionColumn(ctx context.Context, eventID string) (*string, error)
	EventRoom(ctx context.Context, eventID string) (string, error)
	IsRelationTarget(ctx context.Context, eventID string) (bool, error)
	HasRelation(ctx context.Context, eventID string) (bool, error)
	HasAnnotated(ctx context.Context, parent, eventType, sender, key string) (bool, error)
}

// Writer is what the creator writes.
type Writer interface {
	BumpDeletion(ctx context.Context, groups []int64) ([]int64, error)
	SetRoomParticipation(ctx context.Context, userID, roomID string) error
	StagePush(ctx context.Context, eventID string, rows []writer.PushRow) error
	UnstagePush(ctx context.Context, eventID string) error
}

// Persister is the events writer, over replication.
type Persister interface {
	SendClientEvents(ctx context.Context, instance string, entries []json.RawMessage) (int64, string, error)
}

// Authenticator says who a request is from.
type Authenticator interface {
	AuthenticateAs(ctx context.Context, creds auth.Credentials) (auth.Verdict, error)
}

// Options configure a Creator.
type Options struct {
	Store      Store
	Writer     Writer
	Persister  Persister
	Auth       Authenticator
	Key        *build.Key
	Push       *pushcalc.Calculator
	ServerName string
	// EventsWriter is the instance that persists a room's events
	// (stream_writers.events).
	EventsWriter func(roomID string) string
	// Sticky is experimental_features.msc4354_enabled.
	Sticky bool
	// Timeout bounds one request's work, the persister's included.
	Timeout time.Duration
	Log     zerolog.Logger
	Now     func() time.Time
}

// Creator serves native requests.
type Creator struct {
	o     Options
	locks roomLocks
	// verify is replaced in tests.
	verify func(v pending)
}

// New builds a Creator.
func New(o Options) *Creator {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Timeout <= 0 {
		o.Timeout = 60 * time.Second
	}
	c := &Creator{o: o}
	c.verify = func(v pending) { go c.check(v) }
	return c
}

// Outcome is what happened to a request.
type Outcome struct {
	// Fallback is why the request must be proxied; nothing was written to
	// the response or the database. Empty when the request was served.
	Fallback string
	// Result labels a served request.
	Result string
}

func fallback(reason string) Outcome { return Outcome{Fallback: reason} }

// Send serves PUT /rooms/{room}/send/{type}/{txn} and its POST form. r.Body
// is left readable from the start, so a fallback can forward the request.
func (c *Creator) Send(w http.ResponseWriter, r *http.Request, req route.Request) Outcome {
	start := c.o.Now()
	out := c.send(w, r, req)
	ep := string(req.Endpoint)
	if out.Fallback != "" {
		fallbacks.WithLabelValues(ep, out.Fallback).Inc()
	} else {
		native.WithLabelValues(ep, out.Result).Inc()
		nativeSeconds.WithLabelValues(ep, out.Result).Observe(c.o.Now().Sub(start).Seconds())
	}
	return out
}

func (c *Creator) send(w http.ResponseWriter, r *http.Request, req route.Request) Outcome {
	q := r.URL.Query()
	switch {
	case q.Has(DelayParam):
		return fallback("delayed")
	case q.Has("user_id"), q.Has("device_id"), q.Has("ts"):
		// Appservice masquerading and timestamps.
		return fallback("appservice")
	}

	body, tooLarge, err := readBody(r)
	switch {
	case err != nil:
		return fallback("read_body")
	case tooLarge:
		return fallback("too_large")
	}
	if !gjson.ValidBytes(body) || !gjson.ParseBytes(body).IsObject() {
		return fallback("not_object")
	}
	content := gjson.ParseBytes(body)
	if reason := validateSend(req.Action, content); reason != "" {
		return fallback(reason)
	}

	ctx, cancel := context.WithTimeout(r.Context(), c.o.Timeout)
	defer cancel()

	rq, reason := c.requester(ctx, r)
	if reason != "" {
		return fallback(reason)
	}

	unlock := c.locks.lock(req.Room)
	defer unlock()

	// create_and_send_nonmember_event: a transaction already used answers
	// with its event.
	if req.TxnID != "" {
		id, err := c.o.Store.TxnEvent(ctx, req.Room, rq.UserID, rq.DeviceID, req.TxnID)
		switch {
		case err == nil:
			writeEventID(w, id)
			return Outcome{Result: "txn_dedup"}
		case !errors.Is(err, pdustore.ErrNotFound):
			return fallback("db_error")
		}
	}

	p, reason := c.prepare(ctx, req, rq, body, content, q)
	if reason != "" {
		return fallback(reason)
	}

	// Past here, writes. Everything Synapse could refuse has been checked.
	return c.persist(ctx, w, p)
}

// requester identifies the caller as get_user_by_req would, or says why
// the request is Synapse's.
func (c *Creator) requester(ctx context.Context, r *http.Request) (*requester, string) {
	creds := auth.ExtractCredentials(r)
	if creds.Token == "" {
		return nil, "unauthenticated"
	}
	v, err := c.o.Auth.AuthenticateAs(ctx, creds)
	switch {
	case err != nil:
		return nil, "whoami_error"
	case !v.Valid:
		return nil, "unauthenticated"
	case v.IsGuest:
		return nil, "guest"
	case v.DeviceID == "":
		return nil, "no_device"
	}
	t, err := c.o.Store.Token(ctx, creds.Token)
	switch {
	case errors.Is(err, pdustore.ErrNotFound):
		return nil, "token_not_stored"
	case err != nil:
		return nil, "db_error"
	case t.UserID != v.UserID || t.DeviceID != v.DeviceID:
		return nil, "token_mismatch"
	case t.Puppet:
		return nil, "puppet"
	case t.ShadowBan:
		return nil, "shadow_banned"
	case t.Suspended:
		return nil, "suspended"
	case t.Guest, t.AppService:
		return nil, "guest"
	case t.Deactivated, t.Locked:
		return nil, "deactivated"
	}
	return &requester{UserID: v.UserID, DeviceID: v.DeviceID, TokenID: t.ID}, ""
}

// requester is Requester.serialize (rust/src/types/mod.rs) for a user's own
// access token: never a guest, an appservice or a puppet here.
type requester struct {
	UserID   string
	DeviceID string
	TokenID  int64
}

func (rq *requester) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"user_id":              rq.UserID,
		"access_token_id":      rq.TokenID,
		"is_guest":             false,
		"scope":                []string{},
		"shadow_banned":        false,
		"device_id":            rq.DeviceID,
		"app_server_id":        nil,
		"authenticated_entity": rq.UserID,
	})
}

// prepared is an event ready to persist.
type prepared struct {
	req       route.Request
	rq        *requester
	rv        *pdu.RoomVersion
	ev        *pdu.Event
	built     *build.Event
	ts        int64
	group     int64
	deltas    []stateGroupDelta
	before    pdu.StateMap
	maxStream int64
	push      map[string]pushcalc.Row
}

// prepare builds and checks the event. A reason means Synapse's.
func (c *Creator) prepare(ctx context.Context, req route.Request, rq *requester, body []byte, content gjson.Result, q map[string][]string) (*prepared, string) {
	rvID, err := c.o.Store.RoomVersion(ctx, req.Room)
	if err != nil {
		return nil, "unknown_room"
	}
	rv, ok := pdu.LookupRoomVersion(rvID)
	if !ok || rv.EventFormat < 3 {
		return nil, "room_version"
	}
	if partial, err := c.o.Store.PartialState(ctx, req.Room); err != nil {
		return nil, "db_error"
	} else if partial {
		return nil, "partial_state"
	}

	// The stream position first: the push check reads members as of now.
	maxStream, err := c.o.Store.RoomMaxStream(ctx, req.Room)
	if err != nil {
		return nil, "db_error"
	}
	prevs, err := c.o.Store.PrevEventsForRoom(ctx, req.Room)
	if err != nil {
		return nil, "db_error"
	}
	if len(prevs) != 1 {
		// Several extremities need state resolution, and a new state group
		// for the result; Synapse does both.
		return nil, "prev_events_" + strconv.Itoa(min(len(prevs), 2))
	}
	group, err := c.o.Store.StateGroupForEvent(ctx, prevs[0])
	if err != nil {
		return nil, "no_state_group"
	}
	before, err := c.o.Store.StateForGroup(ctx, group)
	if err != nil {
		return nil, "db_error"
	}
	deltas, err := c.contextDeltas(ctx, group)
	if err != nil {
		return nil, "db_error"
	}
	depths, err := c.o.Store.Depths(ctx, prevs)
	if err != nil || len(depths) != len(prevs) {
		return nil, "db_error"
	}

	if reason := c.checkRelation(ctx, req, rq, content); reason != "" {
		return nil, reason
	}

	// validate_canonicaljson, on the content alone first: a float or an
	// out-of-range integer would otherwise fail to build at all.
	if err := pdu.CheckPDUJSON([]byte(`{"type":"","depth":0,"content":`+string(body)+`}`), rv); err != nil {
		return nil, "invalid_json"
	}

	proto := &pdu.Event{Type: req.Action, Sender: rq.UserID, RoomID: req.Room, Content: content, Room: rv}
	authIDs, err := pdu.ComputeAuthEvents(rv, proto, before)
	if err != nil {
		return nil, "auth_events"
	}
	ts := c.o.Now().UnixMilli()
	p := build.Proto{
		RoomVersion:    id.RoomVersion(rvID),
		RoomID:         req.Room,
		Sender:         rq.UserID,
		Type:           req.Action,
		Content:        jsontext.Value(body),
		PrevEvents:     prevs,
		AuthEvents:     authIDs,
		Depth:          build.Depth([]int64{depths[prevs[0]]}),
		OriginServerTS: ts,
	}
	if c.o.Sticky {
		if v, ok := q[StickyParam]; ok && len(v) > 0 {
			// parse_integer: anything else is Synapse's 400.
			n, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				return nil, "bad_sticky"
			}
			p.StickyDurationMS = &n
		}
	}
	built, err := build.Build(p, c.o.Key)
	if err != nil {
		return nil, "build"
	}

	withUnsigned, err := withAgeTS(built.JSON, ts)
	if err != nil {
		return nil, "build"
	}
	// validate_new and _check_size_limits.
	if err := pdu.CheckPDUJSON(withUnsigned, rv); err != nil {
		return nil, "invalid_json"
	}
	if se := pdu.CheckPDUSize(withUnsigned); se != nil {
		return nil, "too_large"
	}
	ev, err := pdu.ParseEvent(built.JSON, built.ID, rv, false)
	if err != nil {
		return nil, "build"
	}
	if se := pdu.CheckFieldSizes(ev); se != nil {
		return nil, "too_large"
	}

	if err := c.checkAuth(ctx, ev); err != nil {
		// Synapse refuses it, with its own words.
		return nil, "auth_rules"
	}

	var rows map[string]pushcalc.Row
	if c.o.Push != nil {
		if rows, err = c.o.Push.Rows(ctx, ev, before); err != nil {
			return nil, "push"
		}
	}

	return &prepared{req: req, rq: rq, rv: rv, ev: ev, built: built, ts: ts, group: group,
		deltas: deltas, before: before, maxStream: maxStream, push: rows}, ""
}

// persist writes what the creator writes, then sends the event to its
// writer: _persist_events.
func (c *Creator) persist(ctx context.Context, w http.ResponseWriter, p *prepared) Outcome {
	room := p.req.Room
	missing, err := c.o.Writer.BumpDeletion(ctx, []int64{p.group})
	if err != nil {
		return fallback("db_error")
	}
	if len(missing) > 0 {
		return fallback("state_group_deleted")
	}
	// handle_new_client_event.
	if p.ev.Type == "m.room.message" || p.ev.Type == "m.room.encrypted" {
		if err := c.o.Writer.SetRoomParticipation(ctx, p.rq.UserID, room); err != nil {
			return fallback("db_error")
		}
	}

	rows := make([]writer.PushRow, 0, len(p.push))
	for _, r := range p.push {
		rows = append(rows, writer.PushRow{UserID: r.UserID, Actions: r.Actions, Notif: r.Notif,
			Highlight: r.Highlight, Unread: r.Unread, ThreadID: r.ThreadID})
	}
	if err := c.o.Writer.StagePush(ctx, p.ev.ID, rows); err != nil {
		_ = c.o.Writer.UnstagePush(context.WithoutCancel(ctx), p.ev.ID)
		return fallback("db_error")
	}

	entry, err := c.entry(p)
	if err != nil {
		_ = c.o.Writer.UnstagePush(context.WithoutCancel(ctx), p.ev.ID)
		return fallback("serialize")
	}
	instance := c.o.EventsWriter(room)
	_, persisted, err := c.o.Persister.SendClientEvents(ctx, instance, []json.RawMessage{entry})
	if err != nil {
		_ = c.o.Writer.UnstagePush(context.WithoutCancel(ctx), p.ev.ID)
		var he *replication.HTTPError
		if errors.As(err, &he) {
			// The writer's refusal, as Synapse relays it (to_synapse_error).
			c.o.Log.Info().Int("status", he.Status).Str("room_id", room).Msg("Events writer refused a native event")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(he.Status)
			_, _ = io.WriteString(w, he.Body)
			return Outcome{Result: "writer_" + strconv.Itoa(he.Status)}
		}
		c.o.Log.Error().Err(err).Str("room_id", room).Str("writer", instance).Msg("Could not persist a native event")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = fmt.Fprintf(w, `{"errcode":"M_UNKNOWN","error":"Failed to talk to %s process"}`, instance)
		return Outcome{Result: "writer_unreachable"}
	}

	if persisted != "" && persisted != p.ev.ID {
		// Deduplicated by the writer: an event of the same transaction
		// was persisted first. That is the answer.
		writeEventID(w, persisted)
		return Outcome{Result: "writer_dedup"}
	}
	writeEventID(w, p.ev.ID)
	c.verify(pending{p: p})
	return Outcome{Result: "created"}
}

// entry is ReplicationSendEventsRestServlet._serialize_payload's entry for
// the event.
func (c *Creator) entry(p *prepared) (json.RawMessage, error) {
	ev, err := withAgeTS(p.built.JSON, p.ts)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{"device_id": p.rq.DeviceID}
	if p.req.TxnID != "" {
		meta["txn_id"] = p.req.TxnID
	}
	return json.Marshal(map[string]any{
		"event":                json.RawMessage(ev),
		"room_version":         p.rv.ID,
		"event_format_version": p.rv.EventFormat,
		"internal_metadata":    meta,
		"outlier":              false,
		"rejected_reason":      nil,
		"context": map[string]any{
			"state_group":              p.group,
			"state_group_before_event": p.group,
			// str | None (events/snapshot.py): None, never False. The
			// writer stores it in events.rejection_reason as it is, and a
			// False there is the text 'false'.
			"rejected":                 nil,
			"state_group_deltas":       p.deltas,
			"state_delta_due_to_event": []any{},
			"app_service_id":           nil,
			"partial_state":            false,
		},
		"requester":   p.rq,
		"ratelimit":   true,
		"extra_users": []string{},
	})
}

// stateGroupDelta is _encode_state_group_delta's (prev, group, [[type,
// state_key, event_id], ...]).
type stateGroupDelta struct {
	From, To int64
	Delta    pdu.StateMap
}

func (d stateGroupDelta) MarshalJSON() ([]byte, error) {
	rows := make([][3]string, 0, len(d.Delta))
	for k, v := range d.Delta {
		rows = append(rows, [3]string{k.Type, k.StateKey, v})
	}
	return json.Marshal([]any{d.From, d.To, rows})
}

// contextDeltas is _build_state_group_deltas for a message event whose state
// is one existing group: the event's own (group, group) with no change, then
// the group's edge from its prev group, as resolve_state_groups_for_events
// returns it (get_state_group_delta), when it has one.
func (c *Creator) contextDeltas(ctx context.Context, group int64) ([]stateGroupDelta, error) {
	out := []stateGroupDelta{{From: group, To: group, Delta: pdu.StateMap{}}}
	prev, hasPrev, delta, err := c.o.Store.StateDelta(ctx, group)
	if err != nil {
		return nil, err
	}
	if hasPrev {
		out = append(out, stateGroupDelta{From: prev, To: group, Delta: delta})
	}
	return out, nil
}

// checkRelation is _validate_event_relation: a reason when Synapse would
// refuse the relation.
func (c *Creator) checkRelation(ctx context.Context, req route.Request, rq *requester, content gjson.Result) string {
	rel := pdu.Get(content, "m.relates_to")
	if !rel.Exists() || !pyTruthy(rel) || !rel.IsObject() {
		return ""
	}
	relType, parent := pdu.Get(rel, "rel_type"), pdu.Get(rel, "event_id")
	if relType.Type != gjson.String || parent.Type != gjson.String {
		return ""
	}
	room, err := c.o.Store.EventRoom(ctx, parent.Str)
	switch {
	case err == nil:
		if room != req.Room {
			return "relation_other_room"
		}
	case errors.Is(err, pdustore.ErrNotFound):
		target, err := c.o.Store.IsRelationTarget(ctx, parent.Str)
		if err != nil {
			return "db_error"
		}
		if !target {
			return "relation_unknown"
		}
	default:
		return "db_error"
	}
	switch relType.Str {
	case "m.annotation":
		key := pdu.Get(rel, "key")
		if key.Type != gjson.String || len([]rune(key.Str)) > 500 {
			return "annotation_key"
		}
		dup, err := c.o.Store.HasAnnotated(ctx, parent.Str, req.Action, rq.UserID, key.Str)
		if err != nil {
			return "db_error"
		}
		if dup {
			return "duplicate_annotation"
		}
	case "m.thread":
		has, err := c.o.Store.HasRelation(ctx, parent.Str)
		if err != nil {
			return "db_error"
		}
		if has {
			return "thread_from_relation"
		}
	}
	return ""
}

// checkAuth runs the auth rules against the event's auth events.
func (c *Creator) checkAuth(ctx context.Context, ev *pdu.Event) error {
	stored, bad, err := c.o.Store.GetStoredEvents(ctx, ev.AuthEvents)
	if err != nil {
		return err
	}
	byID := map[string]*pdu.Event{}
	list := make([]*pdu.Event, 0, len(ev.AuthEvents))
	for _, id := range ev.AuthEvents {
		se := stored[id]
		if bad[id] != nil || se == nil {
			return fmt.Errorf("auth event %s unavailable", id)
		}
		byID[id] = se.Event
		list = append(list, se.Event)
	}
	if err := pdu.CheckStateIndependentAuthRules(ev, byID); err != nil {
		return err
	}
	return pdu.CheckStateDependentAuthRules(ev, list)
}

// validateSend is validate_builder and validate_new for a /send event, plus
// the types the non-member path refuses. A reason means Synapse's.
func validateSend(eventType string, content gjson.Result) string {
	switch eventType {
	case "m.room.member":
		// create_and_send_nonmember_event refuses it.
		return "type_member"
	case "m.room.create", "m.room.topic", "m.room.name", "m.room.tombstone":
		// validate_builder insists these are state events.
		return "type_state_only"
	case "m.room.aliases", "m.room.retention", "m.room.server_acl", "m.room.power_levels":
		// validate_new checks their content.
		return "type_validated"
	case "m.room.message":
		for _, k := range []string{"body", "msgtype"} {
			if pdu.Get(content, k).Type != gjson.String {
				return "invalid_message"
			}
		}
	}
	if m := pdu.Get(content, "m.mentions"); m.Exists() {
		if !m.IsObject() {
			return "invalid_mentions"
		}
		if u := pdu.Get(m, "user_ids"); u.Exists() {
			if !u.IsArray() {
				return "invalid_mentions"
			}
			for _, x := range u.Array() {
				if x.Type != gjson.String {
					return "invalid_mentions"
				}
			}
		}
		if r := pdu.Get(m, "room"); r.Exists() && r.Type != gjson.True && r.Type != gjson.False {
			return "invalid_mentions"
		}
	}
	return ""
}

// withAgeTS adds unsigned.age_ts, as create_local_event_from_event_dict does
// for an event with no age: the event as get_pdu_json sends it.
func withAgeTS(raw []byte, ts int64) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	m["unsigned"] = json.RawMessage(`{"age_ts":` + strconv.FormatInt(ts, 10) + `}`)
	return json.Marshal(m)
}

func writeEventID(w http.ResponseWriter, eventID string) {
	b, _ := json.Marshal(map[string]string{"event_id": eventID})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

// readBody reads up to MaxBody and leaves r.Body readable from the start.
func readBody(r *http.Request) (body []byte, tooLarge bool, err error) {
	if r.Body == nil {
		return nil, false, nil
	}
	head, err := io.ReadAll(io.LimitReader(r.Body, MaxBody+1))
	r.Body = readCloser{io.MultiReader(bytes.NewReader(head), r.Body), r.Body}
	return head, len(head) > MaxBody, err
}

type readCloser struct {
	io.Reader
	io.Closer
}

// pyTruthy is bool(v) for a decoded JSON value.
func pyTruthy(v gjson.Result) bool {
	switch v.Type {
	case gjson.Null, gjson.False:
		return false
	case gjson.True:
		return true
	case gjson.Number:
		return v.Num != 0
	case gjson.String:
		return v.Str != ""
	}
	if v.IsArray() {
		return len(v.Array()) > 0
	}
	if v.IsObject() {
		return len(v.Map()) > 0
	}
	return false
}

// roomLocks serialises event creation per room, as the creator's limiter
// does (_create_and_send_nonmember_event_locked).
type roomLocks struct {
	mu    sync.Mutex
	rooms map[string]*roomLock
}

type roomLock struct {
	mu   sync.Mutex
	refs int
}

func (l *roomLocks) lock(room string) func() {
	l.mu.Lock()
	if l.rooms == nil {
		l.rooms = map[string]*roomLock{}
	}
	rl := l.rooms[room]
	if rl == nil {
		rl = &roomLock{}
		l.rooms[room] = rl
	}
	rl.refs++
	l.mu.Unlock()

	rl.mu.Lock()
	return func() {
		rl.mu.Unlock()
		l.mu.Lock()
		rl.refs--
		if rl.refs == 0 {
			delete(l.rooms, room)
		}
		l.mu.Unlock()
	}
}
