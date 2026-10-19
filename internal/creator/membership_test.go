package creator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

const other = "@v:example.org"

func memberReq(action, txn string) route.Request {
	return route.Request{Endpoint: route.Membership, Write: true, Room: room, Action: action, TxnID: txn}
}

func (h *harness) member(t *testing.T, req route.Request, body string) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	path := "/_matrix/client/v3/rooms/" + room + "/" + req.Action
	method := http.MethodPost
	if req.Endpoint == route.Join {
		path = "/_matrix/client/v3/join/" + room
	}
	if req.TxnID != "" {
		path += "/" + req.TxnID
		method = http.MethodPut
	}
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	return rec, h.c.Serve(rec, r, req)
}

func str(s string) *string { return &s }

func TestNativeInvite(t *testing.T) {
	h := newHarness(t)
	h.f.profiles = map[string][2]*string{other: {str("Vee"), nil}}
	rec, out := h.member(t, memberReq("invite", "i1"), `{"user_id":"`+other+`","reason":"come"}`)
	if out.Result != "created" || rec.Code != 200 || rec.Body.String() != "{}" {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	if got := strings.Join(h.w.calls, ","); got != "bump,state_group,stage" {
		t.Errorf("writes %s", got)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	for path, want := range map[string]string{
		"event.type":               `"m.room.member"`,
		"event.state_key":          `"` + other + `"`,
		"event.content":            `{"displayname":"Vee","membership":"invite","reason":"come"}`,
		"extra_users":              `["` + other + `"]`,
		"internal_metadata.txn_id": `"i1"`,
		"context.state_group":      "8",
	} {
		if got := e.Get(path).Raw; got != want {
			t.Errorf("%s = %s, want %s", path, got, want)
		}
	}
	if e.Get("event.unsigned.replaces_state").Exists() {
		t.Error("replaces_state for a first member event")
	}
	// The members auth events: create, power levels, the sender's and the
	// join rules (none here).
	if n := e.Get("event.auth_events.#").Int(); n != 3 {
		t.Errorf("%d auth events", n)
	}

	// The same transaction again answers from the cache, writing nothing.
	h.w.calls = nil
	rec, out = h.member(t, memberReq("invite", "i1"), `{"user_id":"`+other+`","reason":"come"}`)
	if out.Result != "txn_cached" || rec.Body.String() != "{}" || len(h.w.calls) != 0 {
		t.Fatalf("retry: %+v %s %v", out, rec.Body, h.w.calls)
	}
}

func TestJoinWhileJoinedIsANoop(t *testing.T) {
	h := newHarness(t)
	for _, req := range []route.Request{memberReq("join", ""), {Endpoint: route.Join, Write: true, Room: room}} {
		rec, out := h.member(t, req, ``)
		if out.Result != "state_dedup" || rec.Body.String() != `{"room_id":"`+room+`"}` {
			t.Fatalf("%s: %+v %s", req.Endpoint, out, rec.Body)
		}
	}
	if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
		t.Errorf("a no-op join wrote: %v", h.w.calls)
	}
}

func TestJoinWithReasonAddsProfile(t *testing.T) {
	h := newHarness(t)
	h.f.profiles = map[string][2]*string{user: {str("You"), str("mxc://example.org/a")}}
	rec, out := h.member(t, route.Request{Endpoint: route.Join, Write: true, Room: room}, `{"reason":"back",`+
		`"join_authorised_via_users_server":"@x:example.org"}`)
	if out.Result != "created" || rec.Body.String() != `{"room_id":"`+room+`"}` {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	if got := e.Get("event.content").Raw; got != `{"avatar_url":"mxc://example.org/a","displayname":"You","membership":"join","reason":"back"}` {
		t.Errorf("content %s", got)
	}
	if got := e.Get("event.unsigned.replaces_state").Str; got == "" {
		t.Error("no replaces_state for a join over a join")
	}
}

func TestKickTellsMain(t *testing.T) {
	h := newHarness(t)
	h.f.addState(t, "m.room.member", other, `{"membership":"join"}`)
	rec, out := h.member(t, memberReq("kick", ""), `{"user_id":"`+other+`"}`)
	if out.Result != "created" || rec.Body.String() != "{}" {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	if got := e.Get("event.content").Raw; got != `{"membership":"leave"}` {
		t.Errorf("content %s", got)
	}
	if len(h.main.left) != 1 || h.main.left[0] != room+" "+other {
		t.Errorf("main told %v", h.main.left)
	}

	// A ban of someone who already left: main is not told again.
	h2 := newHarness(t)
	h2.f.addState(t, "m.room.member", other, `{"membership":"leave"}`)
	if _, out := h2.member(t, memberReq("ban", ""), `{"user_id":"`+other+`"}`); out.Result != "created" {
		t.Fatalf("ban: %+v", out)
	}
	if len(h2.main.left) != 0 {
		t.Errorf("main told of a ban after a leave: %v", h2.main.left)
	}
}

func TestMembershipFallbacks(t *testing.T) {
	for name, tc := range map[string]struct {
		setup func(h *harness)
		req   route.Request
		body  string
		want  string
	}{
		"kick of absent user": {nil, memberReq("kick", ""), `{"user_id":"` + other + `"}`, "kick_absent"},
		"kick of a leave": {func(h *harness) { h.f.addState(t, "m.room.member", other, `{"membership":"leave"}`) },
			memberReq("kick", ""), `{"user_id":"` + other + `"}`, "kick_absent"},
		"unban not banned": {func(h *harness) { h.f.addState(t, "m.room.member", other, `{"membership":"leave"}`) },
			memberReq("unban", ""), `{"user_id":"` + other + `"}`, "unban_not_banned"},
		"invite of banned": {func(h *harness) { h.f.addState(t, "m.room.member", other, `{"membership":"ban"}`) },
			memberReq("invite", ""), `{"user_id":"` + other + `"}`, "target_banned"},
		"remote invite":   {nil, memberReq("invite", ""), `{"user_id":"@x:remote.example"}`, "remote_profile"},
		"missing user_id": {nil, memberReq("ban", ""), `{}`, "missing_user_id"},
		"bad user_id":     {nil, memberReq("ban", ""), `{"user_id":"nope"}`, "bad_user_id"},
		"user_id number":  {nil, memberReq("ban", ""), `{"user_id":1}`, "bad_user_id"},
		"3pid invite":     {nil, memberReq("invite", ""), `{"medium":"email","address":"a@b"}`, "3pid_invite"},
		"per-room name":   {nil, route.Request{Endpoint: route.Join, Room: room}, `{"displayname":"x"}`, "per_room_profile"},
		"membership in body": {nil, route.Request{Endpoint: route.Join, Room: room}, `{"membership":"join"}`,
			"membership_in_body"},
		"third party signed": {nil, memberReq("join", ""), `{"third_party_signed":{}}`, "third_party_signed"},
		"invite config": {func(h *harness) { h.f.accountData = map[string]bool{other + " " + invitePermissionConfig: true} },
			memberReq("invite", ""), `{"user_id":"` + other + `"}`, "invite_config"},
		"blocked room":  {func(h *harness) { h.f.blocked = true }, memberReq("join", ""), ``, "room_blocked"},
		"not an object": {nil, memberReq("leave", ""), `[]`, "not_object"},
		"duplicate key": {nil, memberReq("leave", ""), `{"reason":"a","reason":"b"}`, "duplicate_keys"},
		"restricted join": {func(h *harness) {
			h.f.addState(t, "m.room.join_rules", "", `{"join_rule":"restricted","allow":[]}`)
			h.f.token.UserID = user
		}, memberReq("join", ""), ``, ""},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			_, out := h.member(t, tc.req, tc.body)
			if tc.want == "" {
				// The joined user rejoining a restricted room needs no
				// authorising server: a no-op here.
				if out.Fallback != "" {
					t.Fatalf("fallback %q", out.Fallback)
				}
				return
			}
			if out.Fallback != tc.want {
				t.Fatalf("fallback %q, want %q", out.Fallback, tc.want)
			}
			if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
				t.Errorf("a fallback wrote: %v", h.w.calls)
			}
		})
	}
}

func TestInvitesAreRatelimited(t *testing.T) {
	h := newHarness(t)
	h.c.o.Membership.InvitesPerIssuer.Burst = 1
	if _, out := h.member(t, memberReq("invite", ""), `{"user_id":"`+other+`"}`); out.Result != "created" {
		t.Fatalf("first invite: %+v", out)
	}
	rec, out := h.member(t, memberReq("invite", ""), `{"user_id":"@w:example.org"}`)
	if out.Result != "ratelimited" || rec.Code != 429 || gjson.Get(rec.Body.String(), "errcode").Str != "M_LIMIT_EXCEEDED" ||
		rec.Header().Get("Retry-After") == "" {
		t.Fatalf("second invite: %+v %d %s", out, rec.Code, rec.Body)
	}

	// An exempt user is not limited.
	h.f.exempt = true
	if _, out := h.member(t, memberReq("invite", ""), `{"user_id":"@w:example.org"}`); out.Result != "created" {
		t.Fatalf("exempt invite: %+v", out)
	}
}

func TestCallInviteInPublicRoom(t *testing.T) {
	h := newHarness(t)
	h.f.addState(t, "m.room.join_rules", "", `{"join_rule":"public"}`)
	r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/"+room+"/send/m.call.invite/c", strings.NewReader(`{"call_id":"x"}`))
	r.Header.Set("Authorization", "Bearer tok")
	out := h.c.Serve(httptest.NewRecorder(), r, route.Request{Endpoint: route.Send, Room: room, Action: "m.call.invite", TxnID: "c"})
	if out.Fallback != "call_invite_public" {
		t.Fatalf("%+v", out)
	}
}
