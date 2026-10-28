package creator

// Profile writes (rest/client/profile.py, handlers/profile.py, v1.163.0).
//
// A worker that is not the first events writer hands every profile write to
// it (dispatch_set_profile_field: ReplicationProfileSetField), and that
// writer stores the field, updates the user directory and, when the change
// propagates, schedules _update_join_states_task, which main runs: a join
// event into every room the user is joined to. Here the servlet is ours,
// the writer still stores the field (asked not to propagate), and the join
// events are made natively, in the background, as the task makes them.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdu"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/replication"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

// ProfileWriter is the profile updates writer's replication.
type ProfileWriter interface {
	ProfileSetField(ctx context.Context, instance, userID, requester, field string, value json.RawMessage, byAdmin, propagate bool) error
	ProfileDeleteField(ctx context.Context, instance, userID, requester, field string, byAdmin bool) error
}

// RoomChecker is the allowlist: whether a room may be written to here.
type RoomChecker interface {
	Check(roomID string) bool
}

const (
	maxCustomFieldLen = 255 // MAX_CUSTOM_FIELD_LEN, bytes
	msc4069Propagate  = "org.matrix.msc4069.propagate"
)

// namespacedGrammar is NAMESPACED_GRAMMAR, matched as Python's re.match with
// `$`, which also matches before a final newline.
var namespacedGrammar = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,254}\n?$`)

var (
	profileOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_profile_total",
		Help: "Native profile writes, by method and result.",
	}, []string{"method", "result"})
	propagated = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_profile_propagation_total",
		Help: "Join events a profile change sent into the user's rooms, by result.",
	}, []string{"result"})
)

// ServeProfile serves PUT and DELETE of a profile field. A fallback leaves
// the whole request to main, before anything is written.
func (c *Creator) ServeProfile(w http.ResponseWriter, r *http.Request, req route.Request) Outcome {
	start := c.o.Now()
	out := c.serveProfile(w, r, req)
	if out.Fallback != "" {
		fallbacks.WithLabelValues(string(route.Profile), out.Fallback).Inc()
	} else {
		if out.Refused != "" {
			refusals.WithLabelValues(string(route.Profile), out.Refused).Inc()
		}
		native.WithLabelValues(string(route.Profile), out.Result).Inc()
		nativeSeconds.WithLabelValues(string(route.Profile), out.Result).Observe(c.o.Now().Sub(start).Seconds())
	}
	profileOutcomes.WithLabelValues(r.Method, profileResult(out)).Inc()
	return out
}

func profileResult(out Outcome) string {
	switch {
	case out.Fallback != "":
		return "fallback"
	case out.Refused != "":
		return "refused"
	}
	return out.Result
}

func (c *Creator) serveProfile(w http.ResponseWriter, r *http.Request, req route.Request) Outcome {
	switch {
	case !req.FieldPath:
		return fallback("path")
	case r.Method != http.MethodPut && r.Method != http.MethodDelete:
		return fallback("method")
	case c.o.ProfileWriter == nil || c.o.Rooms == nil:
		return fallback("profile_writer")
	}
	if reason := c.configFallback(); reason != "" {
		return fallback(reason)
	}
	body, tooLarge, err := readBody(r)
	switch {
	case err != nil:
		return fallback("read_body")
	case tooLarge:
		return fallback("too_large")
	}
	ctx, cancel := context.WithTimeout(r.Context(), c.o.Timeout)
	defer cancel()

	// on_PUT / on_DELETE, in order.
	if !validUserIDStrict(req.User) {
		return c.refuse(w, badRequest("invalid_user_id", "Invalid user id", "M_INVALID_PARAM"))
	}
	rq, reason, ref := c.requester(ctx, r)
	switch {
	case ref != nil:
		return c.refuse(w, ref)
	case reason != "":
		return fallback(reason)
	}
	field := req.Action
	if rq.Guest && field != "displayname" {
		// Guest users are able to set their own displayname.
		return c.refuse(w, &refusal{reason: "guest", status: http.StatusForbidden, errcode: "M_GUEST_ACCESS_FORBIDDEN",
			msg: "Guest access not allowed"})
	}
	switch {
	case field == "":
		return c.refuse(w, badRequest("field_short", "Field name too short", "M_INVALID_PARAM"))
	case len(field) > maxCustomFieldLen:
		return c.refuse(w, badRequest("field_long", "Field name too long", "M_KEY_TOO_LARGE"))
	case !namespacedGrammar.MatchString(field):
		return c.refuse(w, badRequest("field_grammar",
			"Field name does not follow Common Namespaced Identifier Grammar", "M_INVALID_PARAM"))
	}

	var value json.RawMessage
	if r.Method == http.MethodPut {
		obj, st := parseJSONObject(body, false)
		if st != nil {
			return c.end(w, st)
		}
		v := gjson.GetBytes(obj, gjson.Escape(field))
		if !v.Exists() {
			return c.refuse(w, badRequest("missing_key", "Missing key '"+field+"'", "M_MISSING_PARAM"))
		}
		value = json.RawMessage(v.Raw)
	}
	propagate, st := c.readPropagate(r)
	if st != nil {
		return c.end(w, st)
	}
	if rq.Suspended {
		return c.refuse(w, userSuspended("Updating profile while account is suspended is not allowed."))
	}

	// The join events go into every room the user is in: all of them must
	// be ours, or none is (main then does all, as before).
	joins := propagate && (field == "displayname" || field == "avatar_url")
	if joins && c.isMine(req.User) {
		rooms, err := c.o.Store.JoinedRooms(ctx, req.User)
		if err != nil {
			return fallback("db_error")
		}
		for _, room := range rooms {
			if !c.o.Rooms.Check(room) {
				return fallback("room_not_native")
			}
		}
	}

	// dispatch_set_profile_field / dispatch_delete_profile_field, on the
	// writer: it applies the handler's own checks and answers with them.
	if r.Method == http.MethodDelete && field == "displayname" || r.Method == http.MethodDelete && field == "avatar_url" {
		value = json.RawMessage(`""`)
	}
	if value != nil {
		err = c.o.ProfileWriter.ProfileSetField(ctx, c.o.ProfileWriterInstance, req.User, rq.UserID, field, value, rq.Admin, false)
	} else {
		err = c.o.ProfileWriter.ProfileDeleteField(ctx, c.o.ProfileWriterInstance, req.User, rq.UserID, field, rq.Admin)
	}
	if err != nil {
		var he *replication.HTTPError
		if errors.As(err, &he) {
			// The handler's refusal (to_synapse_error).
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(he.Status)
			_, _ = io.WriteString(w, he.Body)
			return Outcome{Result: "writer_" + strconv.Itoa(he.Status)}
		}
		c.o.Log.Error().Err(err).Str("writer", c.o.ProfileWriterInstance).Msg("Could not set a profile field")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, `{"errcode":"M_UNKNOWN","error":"Failed to talk to `+c.o.ProfileWriterInstance+` process"}`)
		return Outcome{Result: "writer_unreachable"}
	}

	if joins {
		// _update_join_states, as the writer would run it: its requester
		// came over replication as a bare user ID, the target's when an
		// admin made the change, and is ratelimited (rc_message) after the
		// field is stored.
		entity := rq.UserID
		if out, limited := c.messageRatelimit(ctx, w, &requester{UserID: req.User, Entity: req.User}); limited {
			return out
		}
		c.propagateProfile(req.User, entity)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "{}")
	return Outcome{Result: "set"}
}

// readPropagate is _read_propagate: with MSC4069, the query parameter when
// given (parse_boolean); otherwise true.
func (c *Creator) readPropagate(r *http.Request) (bool, *stop) {
	if !c.o.MSC4069 {
		return true, nil
	}
	vs, ok := r.URL.Query()[msc4069Propagate]
	if !ok {
		return true, nil
	}
	switch vs[0] {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, badRequest("bad_boolean",
		"Boolean query parameter '"+msc4069Propagate+"' must be one of ['true', 'false']", "M_INVALID_PARAM").stop()
}

// validUserIDStrict is UserID.is_valid: "@", a ":", and a valid server name
// after the first one. The localpart may be empty.
func validUserIDStrict(s string) bool {
	if !strings.HasPrefix(s, "@") {
		return false
	}
	_, domain, ok := strings.Cut(s[1:], ":")
	return ok && pdu.ValidServerName(domain)
}

// profileJobs are the running propagations, one per user: a newer change
// cancels an older one, as _update_join_states cancels the scheduled task.
type profileJobs struct {
	mu   sync.Mutex
	jobs map[string]context.CancelFunc
}

// propagateProfile is _update_join_states_task, in the background: a join,
// ratelimit=False, into each room the user is joined to, in room ID order;
// a failure in one room is logged and the rest go on.
func (c *Creator) propagateProfile(target, entity string) {
	ctx, cancel := context.WithCancel(context.Background())
	c.profiles.mu.Lock()
	if c.profiles.jobs == nil {
		c.profiles.jobs = map[string]context.CancelFunc{}
	}
	if old := c.profiles.jobs[target]; old != nil {
		old()
		propagated.WithLabelValues("cancelled").Inc()
	}
	c.profiles.jobs[target] = cancel
	c.profiles.mu.Unlock()

	done := func() {
		c.profiles.mu.Lock()
		// Only our own entry: a newer job may have replaced it.
		if c.profiles.jobs[target] != nil && ctx.Err() == nil {
			delete(c.profiles.jobs, target)
		}
		c.profiles.mu.Unlock()
		cancel()
	}
	run := func() {
		defer done()
		rooms, err := c.o.Store.JoinedRooms(ctx, target)
		if err != nil {
			c.o.Log.Error().Err(err).Str("user_id", target).Msg("Profile propagation: no rooms")
			return
		}
		counts := map[string]int{}
		for _, room := range rooms {
			if ctx.Err() != nil {
				return
			}
			res := c.propagateJoin(ctx, room, target, entity)
			counts[res]++
			propagated.WithLabelValues(res).Inc()
			if res != "created" && res != "state_dedup" && res != "membership_noop" {
				c.o.Log.Warn().Str("room_id", room).Str("user_id", target).Str("result", res).
					Msg("Failed to update join event for room")
			}
		}
		c.o.Log.Info().Str("user_id", target).Int("rooms", len(rooms)).Interface("results", counts).
			Msg("Profile propagated")
	}
	if c.o.SyncPropagation {
		run()
		return
	}
	go run()
}

// propagateJoin is one update_membership(requester, target, room, "join",
// ratelimit=False) with create_requester's requester: no token, no device.
func (c *Creator) propagateJoin(ctx context.Context, room, target, entity string) string {
	ctx, cancel := context.WithTimeout(ctx, c.o.Timeout)
	defer cancel()
	rq := &requester{UserID: target, Entity: entity, NoToken: true}
	if _, _, suspended, err := c.o.Store.UserFlags(ctx, target); err != nil {
		return "fallback:db_error"
	} else {
		rq.Suspended = suspended
	}
	req := route.Request{Endpoint: route.Membership, Write: true, Room: room, Action: "join"}
	in, st := c.parseMembership(req, rq, []byte("{}"))
	if st == nil {
		in.noRatelimit = true
		st = c.checkMembershipRequest(ctx, req, rq, in)
	}
	if st != nil {
		return stopResult(st)
	}
	unlock := c.locks.lock(room)
	defer unlock()
	p, dup, st := c.prepare(ctx, req, rq, in, gjson.ParseBytes(in.content))
	if st != nil {
		return stopResult(st)
	}
	if dup != "" {
		return in.dedupKind
	}
	out := c.persist(ctx, httptest.NewRecorder(), p)
	if out.Fallback != "" {
		return "fallback:" + out.Fallback
	}
	return out.Result
}

func stopResult(st *stop) string {
	if st.ref != nil {
		return "refused:" + st.ref.reason
	}
	return "fallback:" + st.fallback
}
