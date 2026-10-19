package creator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdu"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdustore"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

// Membership: RoomMembershipRestServlet and JoinRoomAliasServlet
// (rest/client/room.py), then RoomMemberHandler.update_membership and
// update_membership_locked (handlers/room_member.py, v1.163.0), for a local
// membership change in a room this server is in. Remote joins, knocks,
// invite rejections over federation, 3pid invites and anything a module
// would judge are Synapse's.

// Account data types that filter invites (api/constants.py).
const (
	invitePermissionConfig        = "m.invite_permission_config"
	msc4155InvitePermissionConfig = "org.matrix.msc4155.invite_permission_config"
	msc4293RedactEvents           = "org.matrix.msc4293.redact_events"
	authorisingUser               = "join_authorised_via_users_server"
)

// MembershipNotifier is main, told that a user left a room
// (ReplicationUserJoinedLeftRoomRestServlet).
type MembershipNotifier interface {
	UserLeftRoom(ctx context.Context, roomID, userID string) error
}

// memberInput is what a membership request asks for.
type memberInput struct {
	// action is the servlet's: join, invite, leave, ban, unban or kick.
	action string
	// membership is the effective membership: kick and unban are leave.
	membership string
	target     string
	// join answers {"room_id"}; the other actions {}.
	join bool
	// leftFrom is the target's membership before a leave, once known.
	leftFrom string
}

// membershipLimiters are update_membership's ratelimiters.
type membershipLimiters struct {
	joinsLocal, invitesPerRoom, invitesPerUser, invitesPerIssuer *ratelimit.Limiter
}

func (c *Creator) limiters() *membershipLimiters {
	c.limitersOnce.Do(func() {
		m := c.o.Membership
		c.lim = &membershipLimiters{
			joinsLocal:       ratelimit.New(m.JoinsLocal, c.o.Now),
			invitesPerRoom:   ratelimit.New(m.InvitesPerRoom, c.o.Now),
			invitesPerUser:   ratelimit.New(m.InvitesPerUser, c.o.Now),
			invitesPerIssuer: ratelimit.New(m.InvitesPerIssuer, c.o.Now),
		}
	})
	return c.lim
}

// serveMembership serves the membership servlets.
func (c *Creator) serveMembership(w http.ResponseWriter, r *http.Request, req route.Request) Outcome {
	q := r.URL.Query()
	if q.Has("user_id") || q.Has("device_id") {
		return fallback("appservice")
	}
	switch m := c.o.Membership; {
	case m.Modules:
		// user_may_invite, user_may_join_room, check_event_allowed.
		return fallback("modules")
	case m.AuthBlocking:
		return fallback("auth_blocking")
	case m.ConsentRequired:
		return fallback("consent")
	}

	body, tooLarge, err := readBody(r)
	switch {
	case err != nil:
		return fallback("read_body")
	case tooLarge:
		return fallback("too_large")
	}
	// parse_json_object_from_request(allow_empty_body=True).
	if len(body) == 0 {
		body = []byte("{}")
	}
	if !gjson.ValidBytes(body) || !gjson.ParseBytes(body).IsObject() {
		return fallback("not_object")
	}
	if hasDuplicateKeys(gjson.ParseBytes(body)) {
		return fallback("duplicate_keys")
	}

	ctx, cancel := context.WithTimeout(r.Context(), c.o.Timeout)
	defer cancel()
	rq, reason := c.requester(ctx, r)
	if reason != "" {
		return fallback(reason)
	}

	in, reason := c.parseMembership(ctx, req, rq, body)
	if reason != "" {
		return fallback(reason)
	}
	if reason := c.validate(in, gjson.ParseBytes(in.content), req.Room); reason != "" {
		return fallback(reason)
	}

	// fetch_or_execute_request: a transaction that succeeded here answers as
	// it did, before anything else is looked at.
	var txnKey string
	if req.TxnID != "" {
		txnKey = r.URL.EscapedPath() + "\x00" + rq.UserID + "\x00" + rq.DeviceID
		if c.txns.seenAt(txnKey, c.o.Now()) {
			c.respond(w, in, "")
			return Outcome{Result: "txn_cached"}
		}
	}

	if out, limited := c.ratelimitMembership(ctx, w, req, rq, in.member); limited {
		return out
	}
	if reason := c.checkMembershipRequest(ctx, req, rq, in); reason != "" {
		return fallback(reason)
	}

	unlock := c.locks.lock(req.Room)
	defer unlock()

	p, dup, reason := c.prepare(ctx, req, rq, in, gjson.ParseBytes(in.content))
	if reason != "" {
		return fallback(reason)
	}
	if dup != "" {
		c.respond(w, in, dup)
		c.txns.add(txnKey, c.o.Now())
		return Outcome{Result: in.dedupKind}
	}
	out := c.persist(ctx, w, p)
	if out.Result == "created" || out.Result == "writer_dedup" {
		c.txns.add(txnKey, c.o.Now())
	}
	if out.Result == "created" && in.member.membership == "leave" && in.member.leftFrom == "join" && c.o.Main != nil {
		// _user_left_room, on main.
		if err := c.o.Main.UserLeftRoom(context.WithoutCancel(ctx), req.Room, in.member.target); err != nil {
			c.o.Log.Warn().Err(err).Str("room_id", req.Room).Str("user_id", in.member.target).
				Msg("Could not tell main a user left a room")
		}
	}
	return out
}

// parseMembership is the servlet and the content update_membership_locked
// builds, or why the request is Synapse's.
func (c *Creator) parseMembership(ctx context.Context, req route.Request, rq *requester, body []byte) (*input, string) {
	b := gjson.ParseBytes(body)
	m := &memberInput{action: req.Action, target: rq.UserID}
	content := map[string]json.RawMessage{}

	if req.Endpoint == route.Join {
		// JoinRoomAliasServlet: the body is the content.
		m.action, m.join = "join", true
		b.ForEach(func(k, v gjson.Result) bool {
			content[k.Str] = json.RawMessage(v.Raw)
			return true
		})
		if _, ok := content["membership"]; ok {
			// Compared with == against the old content before membership is
			// set: a dict comparison, 1 == True and all.
			return nil, "membership_in_body"
		}
	} else {
		if m.action == "invite" {
			_, medium := pdu.Field(b, "medium")
			_, address := pdu.Field(b, "address")
			if medium && address {
				return nil, "3pid_invite"
			}
		}
		switch m.action {
		case "invite", "ban", "unban", "kick":
			v, ok := pdu.Field(b, "user_id")
			if !ok {
				// assert_params_in_dict: Synapse's 400.
				return nil, "missing_user_id"
			}
			if v.Type != gjson.String || !validUserID(v.Str) {
				return nil, "bad_user_id"
			}
			m.target = v.Str
		case "join", "leave":
		default:
			return nil, "action"
		}
		m.join = m.action == "join"
		if v, ok := pdu.Field(b, "reason"); ok {
			content["reason"] = json.RawMessage(v.Raw)
		}
		if c.o.MSC4293 {
			if v, ok := pdu.Field(b, msc4293RedactEvents); ok {
				content[msc4293RedactEvents] = json.RawMessage(v.Raw)
			}
		}
	}
	if v := pdu.Get(b, "third_party_signed"); v.Exists() && v.Type != gjson.Null {
		return nil, "third_party_signed"
	}

	m.membership = m.action
	if m.action == "kick" || m.action == "unban" {
		m.membership = "leave"
	}

	// update_membership_locked: per-room profiles are kept unless the
	// server says otherwise, then checked for length and, for an avatar,
	// against the media repository. Only /join's body can carry them.
	_, dn := content["displayname"]
	_, av := content["avatar_url"]
	if dn || av {
		return nil, "per_room_profile"
	}
	delete(content, authorisingUser)

	// Profile data for joins and knocks, and invites when configured.
	if m.membership == "join" || (m.membership == "invite" && c.o.Membership.IncludeProfileDataOnInvite) {
		if !c.isMine(m.target) {
			// A federation profile query, whose failure is swallowed.
			return nil, "remote_profile"
		}
		dn, av, ok, err := c.o.Store.Profile(ctx, m.target)
		if err != nil {
			return nil, "db_error"
		}
		// get_displayname raises for a missing profile, and the try around
		// both lookups leaves both out.
		if ok {
			if dn != nil {
				content["displayname"], _ = json.Marshal(*dn)
			}
			if av != nil {
				content["avatar_url"], _ = json.Marshal(*av)
			}
		}
	}
	content["membership"], _ = json.Marshal(m.membership)
	raw, err := json.Marshal(content)
	if err != nil {
		return nil, "not_object"
	}

	sk := m.target
	return &input{typ: "m.room.member", stateKey: &sk, content: raw, txn: req.TxnID, room: req.Room, member: m}, ""
}

// ratelimitMembership is update_membership's ratelimiting: joins by a user
// not yet joined, and invites. A refusal is answered here, as Synapse's 429.
func (c *Creator) ratelimitMembership(ctx context.Context, w http.ResponseWriter, req route.Request, rq *requester, m *memberInput) (Outcome, bool) {
	if m.action != "join" && m.action != "invite" {
		return Outcome{}, false
	}
	exempt, err := c.o.Store.RatelimitExempt(ctx, rq.UserID)
	if err != nil {
		return fallback("db_error"), true
	}
	if exempt {
		return Outcome{}, false
	}
	lim := c.limiters()
	type check struct {
		l      *ratelimit.Limiter
		key    string
		update bool
	}
	var checks []check
	switch m.action {
	case "join":
		cur, _, err := c.o.Store.LocalMembership(ctx, rq.UserID, req.Room)
		if err != nil {
			return fallback("db_error"), true
		}
		if cur == "join" {
			// Not ratelimited: it would block profile updates.
			return Outcome{}, false
		}
		checks = append(checks, check{lim.joinsLocal, rq.UserID, true})
	case "invite":
		checks = append(checks, check{lim.invitesPerRoom, req.Room, true},
			check{lim.invitesPerUser, m.target, true}, check{lim.invitesPerIssuer, rq.UserID, true})
	}
	for _, ch := range checks {
		if ok, retry := ch.l.CanDo(ch.key, ch.update); !ok {
			return c.limitExceeded(ctx, w, ch.l.Name(), retry), true
		}
	}
	if m.action == "join" {
		// The per-room limiter, update=False: it counts every join the
		// events stream brings, which the database has too.
		ok, retry, err := c.perRoomJoins(ctx, req.Room)
		if err != nil {
			return fallback("db_error"), true
		}
		if !ok {
			return c.limitExceeded(ctx, w, c.o.Membership.JoinsPerRoom.Key, retry), true
		}
	}
	return Outcome{}, false
}

// perRoomJoins replays the room's recent joins into a fresh bucket.
func (c *Creator) perRoomJoins(ctx context.Context, room string) (bool, int64, error) {
	cfg := c.o.Membership.JoinsPerRoom
	window := 60 * time.Second
	if cfg.PerSecond > 0 {
		window += time.Duration(float64(cfg.Burst)/cfg.PerSecond*float64(time.Second)) * 2
	}
	now := c.o.Now()
	ts, err := c.o.Store.RecentJoins(ctx, room, now.Add(-window).UnixMilli())
	if err != nil {
		return false, 0, err
	}
	l := ratelimit.New(cfg, func() time.Time { return now })
	for _, t := range ts {
		l.Record(room, time.UnixMilli(t))
	}
	ok, retry := l.CanDo(room, false)
	return ok, retry, nil
}

// limitExceeded is LimitExceededError as the client sees it, after the half
// second Synapse pauses before answering.
func (c *Creator) limitExceeded(ctx context.Context, w http.ResponseWriter, limiter string, retryMS int64) Outcome {
	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
	}
	b, _ := json.Marshal(map[string]any{"errcode": "M_LIMIT_EXCEEDED", "error": "Too Many Requests", "retry_after_ms": retryMS})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", strconv.FormatInt(ratelimit.RetryAfterHeader(retryMS), 10))
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write(b)
	c.o.Log.Info().Str("limiter", limiter).Msg("Membership change ratelimited")
	return Outcome{Result: "ratelimited"}
}

// checkMembershipRequest is update_membership_locked's checks that need no
// room state.
func (c *Creator) checkMembershipRequest(ctx context.Context, req route.Request, rq *requester, in *input) string {
	m := in.member
	if m.membership != "leave" && m.membership != "ban" {
		blocked, err := c.o.Store.RoomBlocked(ctx, req.Room)
		if err != nil {
			return "db_error"
		}
		if blocked {
			return "room_blocked"
		}
	}
	if sn := c.o.Membership.ServerNoticesMXID; sn != "" {
		if rq.UserID == sn || m.target == sn {
			return "server_notices"
		}
		// is_server_notice_room: invites there cannot be rejected, and
		// joining it needs no consent.
		cur, _, err := c.o.Store.LocalMembership(ctx, sn, req.Room)
		if err != nil {
			return "db_error"
		}
		if cur == "join" {
			return "server_notice_room"
		}
	}
	if m.membership == "invite" && !rq.Admin {
		if c.o.Membership.BlockNonAdminInvites {
			return "invites_blocked"
		}
		// get_invite_config_for_user: any rules at all are Synapse's to apply.
		types := []string{invitePermissionConfig}
		if c.o.MSC4155 {
			types = append(types, msc4155InvitePermissionConfig)
		}
		for _, t := range types {
			_, ok, err := c.o.Store.AccountData(ctx, m.target, t)
			if err != nil {
				return "db_error"
			}
			if ok {
				return "invite_config"
			}
		}
	}
	return ""
}

// checkMembership is update_membership_locked's checks against the state
// before the event, then _local_membership_update's transaction lookup. A
// reason means Synapse's; dup is the event a transaction already made.
func (c *Creator) checkMembership(ctx context.Context, req route.Request, rq *requester, in *input, rv *pdu.RoomVersion, before pdu.StateMap) (dup, reason string) {
	m := in.member
	ids := []string{}
	for k, id := range before {
		if (k.Type == "m.room.member" && (c.isMine(k.StateKey) || k.StateKey == m.target)) || k.Type == "m.room.join_rules" && k.StateKey == "" {
			ids = append(ids, id)
		}
	}
	evs, bad, err := c.o.Store.GetStoredEvents(ctx, ids)
	if err != nil {
		return "", "db_error"
	}
	membershipOf := func(user string) (string, bool) {
		id, ok := before[pdu.StateKey{Type: "m.room.member", StateKey: user}]
		if !ok {
			return "", false
		}
		se := evs[id]
		if se == nil || bad[id] != nil {
			// get_event(allow_none=True) found nothing: Synapse goes on as if
			// there were no old membership, we do not.
			return "", true
		}
		v := pdu.Get(se.Content, "membership")
		if v.Type != gjson.String {
			return "", true
		}
		return v.Str, true
	}

	// _is_host_in_room.
	inRoom := false
	for k := range before {
		if k.Type == "m.room.member" && c.isMine(k.StateKey) {
			if ms, _ := membershipOf(k.StateKey); ms == "join" {
				inRoom = true
				break
			}
		}
	}
	if !inRoom {
		return "", "host_not_in_room"
	}

	old, hasOld := membershipOf(m.target)
	switch {
	case hasOld && old == "":
		return "", "old_membership_unknown"
	case !hasOld && m.action == "kick":
		return "", "kick_absent"
	case hasOld && m.action == "unban" && old != "ban":
		return "", "unban_not_banned"
	case old == "ban" && m.action != "ban" && m.action != "unban" && m.action != "leave":
		return "", "target_banned"
	case (old == "ban" || old == "leave") && m.action == "kick":
		return "", "kick_absent"
	}
	m.leftFrom = old

	if m.membership == "join" {
		// _should_perform_remote_join: restricted rooms need an authorising
		// user unless the user is already joined or invited.
		if restrictedJoinRules(rv, before, evs) && old != "join" && old != "invite" {
			return "", "restricted_join"
		}
	}

	// _local_membership_update: get_event_id_from_transaction.
	if in.txn != "" {
		id, err := c.o.Store.TxnEvent(ctx, req.Room, rq.UserID, rq.DeviceID, in.txn)
		switch {
		case err == nil:
			in.dedupKind = "txn_dedup"
			return id, ""
		case !errors.Is(err, pdustore.ErrNotFound):
			return "", "db_error"
		}
	}
	return "", ""
}

// restrictedJoinRules is has_restricted_join_rules, on the events loaded.
func restrictedJoinRules(rv *pdu.RoomVersion, before pdu.StateMap, evs map[string]*pdustore.StoredEvent) bool {
	if !rv.RestrictedJoinRule {
		return false
	}
	id, ok := before[pdu.StateKey{Type: "m.room.join_rules"}]
	if !ok {
		return false
	}
	se := evs[id]
	if se == nil {
		// Not loaded: treated as restricted, which falls back.
		return true
	}
	rule := pdu.Get(se.Content, "join_rule")
	if rule.Type == gjson.String && rule.Str == "restricted" {
		return true
	}
	return rv.KnockRestrictedJoinRule && rule.Type == gjson.String && rule.Str == "knock_restricted"
}

func (c *Creator) isMine(userID string) bool {
	i := strings.IndexByte(userID, ':')
	return i >= 0 && userID[i+1:] == c.o.ServerName
}

// validUserID is UserID.from_string's acceptance, made strict: a sigil, a
// non-empty localpart and a domain.
func validUserID(s string) bool {
	if len(s) < 2 || s[0] != '@' {
		return false
	}
	i := strings.IndexByte(s, ':')
	return i > 1 && i < len(s)-1
}

// respond answers a request with its event.
func (c *Creator) respond(w http.ResponseWriter, in *input, eventID string) {
	if in.member == nil {
		writeEventID(w, eventID)
		return
	}
	out := map[string]string{}
	if in.member.join {
		out["room_id"] = in.room
	}
	b, _ := json.Marshal(out)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

// txnCache is HttpTransactionCache for the membership servlets: a
// transaction that succeeded on this process answers the same for 30
// minutes. Failures are not kept.
type txnCache struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

const txnCacheTTL = 30 * time.Minute

func (t *txnCache) seenAt(key string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	at, ok := t.seen[key]
	return ok && now.Sub(at) < txnCacheTTL
}

func (t *txnCache) add(key string, now time.Time) {
	if key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.seen == nil {
		t.seen = map[string]time.Time{}
	}
	for k, at := range t.seen {
		if now.Sub(at) >= txnCacheTTL {
			delete(t.seen, k)
		}
	}
	t.seen[key] = now
}

// hasDuplicateKeys reports a repeated key in any object: Python's json keeps
// the last, gjson finds the first.
func hasDuplicateKeys(v gjson.Result) bool {
	dup := false
	switch {
	case v.IsObject():
		seen := map[string]bool{}
		v.ForEach(func(k, x gjson.Result) bool {
			if seen[k.Str] || hasDuplicateKeys(x) {
				dup = true
				return false
			}
			seen[k.Str] = true
			return true
		})
	case v.IsArray():
		v.ForEach(func(_, x gjson.Result) bool {
			dup = hasDuplicateKeys(x)
			return !dup
		})
	}
	return dup
}
