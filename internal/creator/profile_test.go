package creator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/replication"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

func (f *fakeRoom) JoinedRooms(_ context.Context, u string) ([]string, error) {
	if ms, _, _ := f.LocalMembership(context.Background(), u, room); ms == "join" {
		return []string{room}, nil
	}
	return nil, nil
}

// fakeProfiles is the profile updates writer: it stores what it is sent in
// the room's profiles, as the writer's set_field would, or refuses.
type fakeProfiles struct {
	f      *fakeRoom
	calls  []map[string]any
	refuse *replication.HTTPError
}

func (p *fakeProfiles) ProfileSetField(_ context.Context, instance, userID, requester, field string, value json.RawMessage, byAdmin, propagate bool) error {
	p.calls = append(p.calls, map[string]any{"op": "set", "instance": instance, "user": userID, "requester": requester,
		"field": field, "value": string(value), "by_admin": byAdmin, "propagate": propagate})
	if p.refuse != nil {
		return p.refuse
	}
	if p.f.profiles == nil {
		p.f.profiles = map[string][2]*string{}
	}
	cur := p.f.profiles[userID]
	v := gjson.Parse(string(value)).Str
	var set *string
	if v != "" {
		set = &v
	}
	switch field {
	case "displayname":
		cur[0] = set
	case "avatar_url":
		cur[1] = set
	}
	p.f.profiles[userID] = cur
	return nil
}

func (p *fakeProfiles) ProfileDeleteField(_ context.Context, instance, userID, requester, field string, byAdmin bool) error {
	p.calls = append(p.calls, map[string]any{"op": "delete", "user": userID, "field": field, "by_admin": byAdmin})
	return nil
}

type roomSet map[string]bool

func (r roomSet) Check(id string) bool { return r[id] }

func profileHarness(t *testing.T) (*harness, *fakeProfiles) {
	t.Helper()
	h := newHarness(t)
	p := &fakeProfiles{f: h.f}
	h.c.o.ProfileWriter, h.c.o.ProfileWriterInstance = p, "persister"
	h.c.o.Rooms = roomSet{room: true}
	h.c.o.MSC4069, h.c.o.SyncPropagation = true, true
	return h, p
}

func (h *harness) profile(t *testing.T, method, path, body string) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	return rec, h.c.ServeProfile(rec, r, route.Classify(method, r.URL.EscapedPath()))
}

const profilePath = "/_matrix/client/v3/profile/%40u%3Aexample.org/"

// A display name change: the writer stores it, not propagating, and the
// join event with the new profile is made here, as _update_join_states_task
// makes it: no device, no transaction, not ratelimited.
func TestProfilePropagates(t *testing.T) {
	h, p := profileHarness(t)
	rec, out := h.profile(t, http.MethodPut, profilePath+"displayname", `{"displayname":"New Name"}`)
	if out.Result != "set" || rec.Code != 200 || rec.Body.String() != "{}" {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	if len(p.calls) != 1 {
		t.Fatalf("calls %v", p.calls)
	}
	c := p.calls[0]
	if c["user"] != user || c["requester"] != user || c["field"] != "displayname" || c["value"] != `"New Name"` ||
		c["propagate"] != false || c["by_admin"] != false || c["instance"] != "persister" {
		t.Errorf("call %v", c)
	}
	if len(h.p.entries) != 1 {
		t.Fatalf("join events %d", len(h.p.entries))
	}
	e := gjson.ParseBytes(h.p.entries[0])
	for path, want := range map[string]string{
		"event.type":                     `"m.room.member"`,
		"event.state_key":                `"` + user + `"`,
		"event.content.membership":       `"join"`,
		"event.content.displayname":      `"New Name"`,
		"ratelimit":                      "false",
		"requester.access_token_id":      "null",
		"requester.device_id":            "null",
		"requester.authenticated_entity": `"` + user + `"`,
		"internal_metadata.device_id":    "",
		"internal_metadata.txn_id":       "",
	} {
		if got := e.Get(path).Raw; got != want {
			t.Errorf("%s = %s, want %s", path, got, want)
		}
	}

	// MSC4069: propagate=false stores and sends nothing into rooms.
	rec, out = h.profile(t, http.MethodPut, profilePath+"displayname?org.matrix.msc4069.propagate=false", `{"displayname":"Quiet"}`)
	if out.Result != "set" || len(h.p.entries) != 1 || len(p.calls) != 2 {
		t.Errorf("propagate=false: %+v %d, %d events", out, rec.Code, len(h.p.entries))
	}
	// A custom field never propagates; deleting one is its own call.
	h.profile(t, http.MethodPut, profilePath+"m.tz", `{"m.tz":"Europe/Lisbon"}`)
	h.profile(t, http.MethodDelete, profilePath+"m.tz", ``)
	if len(h.p.entries) != 1 || len(p.calls) != 4 || p.calls[3]["op"] != "delete" {
		t.Errorf("custom fields: %v, %d events", p.calls, len(h.p.entries))
	}
	// Deleting the display name sets it to "": the join it propagates has
	// no displayname, the same content as the fake room's member event (it
	// does not apply what was persisted), so dedup makes none.
	_, out = h.profile(t, http.MethodDelete, profilePath+"displayname", ``)
	if p.calls[4]["value"] != `""` || out.Result != "set" || len(h.p.entries) != 1 {
		t.Errorf("delete displayname: %v %+v, %d events", p.calls[4], out, len(h.p.entries))
	}
}

// The servlet's refusals, in its order and words, before the writer is
// asked; the writer's own refusals relayed.
func TestProfileRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		method, path, body string
		status             int
		errcode, msg       string
	}{
		"bad user":    {"PUT", "/_matrix/client/v3/profile/nobody/displayname", `{}`, 400, "M_INVALID_PARAM", "Invalid user id"},
		"bad server":  {"PUT", "/_matrix/client/v3/profile/%40u%3Aexa%20mple/displayname", `{}`, 400, "M_INVALID_PARAM", "Invalid user id"},
		"empty field": {"PUT", profilePath, `{}`, 400, "M_INVALID_PARAM", "Field name too short"},
		"long field":  {"PUT", profilePath + strings.Repeat("a", 256), `{}`, 400, "M_KEY_TOO_LARGE", "Field name too long"},
		"grammar":     {"PUT", profilePath + "Tz", `{}`, 400, "M_INVALID_PARAM", "Field name does not follow Common Namespaced Identifier Grammar"},
		"missing key": {"PUT", profilePath + "displayname", `{"avatar_url":"x"}`, 400, "M_MISSING_PARAM", "Missing key 'displayname'"},
		"not json":    {"PUT", profilePath + "displayname", `nope`, 400, "M_NOT_JSON", "Content not JSON."},
		"bad boolean": {"PUT", profilePath + "displayname?org.matrix.msc4069.propagate=yes", `{"displayname":"x"}`, 400, "M_INVALID_PARAM", "Boolean query parameter 'org.matrix.msc4069.propagate' must be one of ['true', 'false']"},
	} {
		h, p := profileHarness(t)
		rec, out := h.profile(t, tc.method, tc.path, tc.body)
		b := gjson.Parse(rec.Body.String())
		if out.Refused == "" || rec.Code != tc.status || b.Get("errcode").Str != tc.errcode || b.Get("error").Str != tc.msg {
			t.Errorf("%s: %+v %d %s", name, out, rec.Code, rec.Body)
		}
		if len(p.calls) != 0 {
			t.Errorf("%s: the writer was asked", name)
		}
	}

	// Suspended: after everything else.
	h, p := profileHarness(t)
	h.f.token.Suspended = true
	rec, _ := h.profile(t, "PUT", profilePath+"displayname", `{"displayname":"x"}`)
	if rec.Code != 403 || gjson.Get(rec.Body.String(), "errcode").Str != "M_USER_SUSPENDED" || len(p.calls) != 0 {
		t.Errorf("suspended: %d %s", rec.Code, rec.Body)
	}

	// The writer's refusal (another user's display name, say) is relayed,
	// and nothing goes into rooms.
	h, p = profileHarness(t)
	p.refuse = &replication.HTTPError{Status: 400, Body: `{"errcode":"M_FORBIDDEN","error":"Cannot set another user's displayname"}`}
	rec, out := h.profile(t, "PUT", profilePath+"displayname", `{"displayname":"x"}`)
	if rec.Code != 400 || !strings.Contains(rec.Body.String(), "another user") || out.Result != "writer_400" || len(h.p.entries) != 0 {
		t.Errorf("writer refusal: %+v %d %s", out, rec.Code, rec.Body)
	}
}

// A user in a room that is not ours: the whole change is main's, untouched.
func TestProfileRoomNotNative(t *testing.T) {
	h, p := profileHarness(t)
	h.c.o.Rooms = roomSet{}
	_, out := h.profile(t, "PUT", profilePath+"displayname", `{"displayname":"x"}`)
	if out.Fallback != "room_not_native" || len(p.calls) != 0 || len(h.p.entries) != 0 {
		t.Errorf("%+v %v", out, p.calls)
	}
	// Without propagation no room is written: served.
	_, out = h.profile(t, "PUT", profilePath+"displayname?org.matrix.msc4069.propagate=false", `{"displayname":"x"}`)
	if out.Result != "set" {
		t.Errorf("no propagation: %+v", out)
	}
}

// rc_message is applied after the field is stored, as _update_join_states
// applies it: a 429, and no join events.
func TestProfileRatelimited(t *testing.T) {
	h, p := profileHarness(t)
	h.c.o.Delayed.RcMessage.PerSecond, h.c.o.Delayed.RcMessage.Burst = 0.001, 1
	h.profile(t, "PUT", profilePath+"displayname", `{"displayname":"a"}`)
	rec, _ := h.profile(t, "PUT", profilePath+"displayname", `{"displayname":"b"}`)
	if rec.Code != 429 || len(p.calls) != 2 || len(h.p.entries) != 1 {
		t.Errorf("%d %s, %d calls, %d events", rec.Code, rec.Body, len(p.calls), len(h.p.entries))
	}
}
