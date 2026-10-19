package creator

import (
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/federation"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/build"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdu"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdustore"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/replication"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/writer"
)

const (
	room = "!r:example.org"
	user = "@u:example.org"
)

// fakeRoom is a v11 room with a create event, power levels and @u joined,
// whose one extremity is the member event, in state group 7 (a delta of 6).
type fakeRoom struct {
	create, topic string
	events        map[string]*pdustore.StoredEvent
	state         pdu.StateMap
	prevs         []string
	txn           string
	token         *pdustore.TokenInfo
	partial       bool

	key         *build.Key
	profiles    map[string][2]*string
	blocked     bool
	accountData map[string]bool
	exempt      bool
}

func newFakeRoom(t *testing.T, key *build.Key) *fakeRoom {
	t.Helper()
	f := &fakeRoom{events: map[string]*pdustore.StoredEvent{}, state: pdu.StateMap{},
		token: &pdustore.TokenInfo{ID: 42, UserID: user, DeviceID: "DEV", Used: true}}
	rv, _ := pdu.LookupRoomVersion("11")
	var prev []string
	depth := int64(1)
	add := func(typ, sk, content string, auth []string) string {
		s := sk
		b, err := build.Build(build.Proto{RoomVersion: id.RoomV11, RoomID: room, Sender: user, Type: typ, StateKey: &s,
			Content: jsontext.Value(content), PrevEvents: prev, AuthEvents: auth, Depth: depth, OriginServerTS: 1}, key)
		if err != nil {
			t.Fatal(err)
		}
		ev, err := pdu.ParseEvent(b.JSON, b.ID, rv, false)
		if err != nil {
			t.Fatal(err)
		}
		f.events[b.ID] = &pdustore.StoredEvent{Event: ev, StreamOrdering: depth}
		f.state[pdu.StateKey{Type: typ, StateKey: sk}] = b.ID
		prev = []string{b.ID}
		depth++
		return b.ID
	}
	create := add("m.room.create", "", `{"room_version":"11","creator":"`+user+`"}`, nil)
	member := add("m.room.member", user, `{"membership":"join"}`, []string{create})
	pl := add("m.room.power_levels", "", `{"users":{"`+user+`":100},"events_default":0}`, []string{create, member})
	f.create = create
	f.topic = add("m.room.topic", "", `{"topic":"old"}`, []string{create, member, pl})
	f.prevs = prev
	f.key = key
	return f
}

// addState puts one more state event in the room, sent by @u, as the new
// extremity.
func (f *fakeRoom) addState(t *testing.T, typ, sk, content string) string {
	t.Helper()
	rv, _ := pdu.LookupRoomVersion("11")
	s := sk
	depth := f.events[f.prevs[0]].Depth + 1
	auth := []string{f.create, f.state[pdu.StateKey{Type: "m.room.power_levels"}], f.state[pdu.StateKey{Type: "m.room.member", StateKey: user}]}
	b, err := build.Build(build.Proto{RoomVersion: id.RoomV11, RoomID: room, Sender: user, Type: typ, StateKey: &s,
		Content: jsontext.Value(content), PrevEvents: f.prevs, AuthEvents: auth, Depth: depth, OriginServerTS: 1}, f.key)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := pdu.ParseEvent(b.JSON, b.ID, rv, false)
	if err != nil {
		t.Fatal(err)
	}
	f.events[b.ID] = &pdustore.StoredEvent{Event: ev, StreamOrdering: depth}
	f.state[pdu.StateKey{Type: typ, StateKey: sk}] = b.ID
	f.prevs = []string{b.ID}
	return b.ID
}

func (f *fakeRoom) Profile(_ context.Context, u string) (*string, *string, bool, error) {
	p, ok := f.profiles[u]
	return p[0], p[1], ok, nil
}
func (f *fakeRoom) RoomBlocked(context.Context, string) (bool, error) { return f.blocked, nil }
func (f *fakeRoom) LocalMembership(_ context.Context, u, _ string) (string, string, error) {
	id, ok := f.state[pdu.StateKey{Type: "m.room.member", StateKey: u}]
	if !ok {
		return "", "", nil
	}
	return gjson.Get(f.events[id].Content.Raw, "membership").Str, id, nil
}
func (f *fakeRoom) RatelimitExempt(context.Context, string) (bool, error)       { return f.exempt, nil }
func (f *fakeRoom) RecentJoins(context.Context, string, int64) ([]int64, error) { return nil, nil }
func (f *fakeRoom) AccountData(_ context.Context, u, typ string) ([]byte, bool, error) {
	if f.accountData[u+" "+typ] {
		return []byte("{}"), true, nil
	}
	return nil, false, nil
}

func (f *fakeRoom) Token(_ context.Context, token string) (*pdustore.TokenInfo, error) {
	if token != "tok" {
		return nil, pdustore.ErrNotFound
	}
	return f.token, nil
}
func (f *fakeRoom) TxnEvent(_ context.Context, r, u, d, txn string) (string, error) {
	if f.txn != "" && txn == "seen" {
		return f.txn, nil
	}
	return "", pdustore.ErrNotFound
}
func (f *fakeRoom) RoomVersion(context.Context, string) (string, error)         { return "11", nil }
func (f *fakeRoom) PartialState(context.Context, string) (bool, error)          { return f.partial, nil }
func (f *fakeRoom) PrevEventsForRoom(context.Context, string) ([]string, error) { return f.prevs, nil }
func (f *fakeRoom) RoomMaxStream(context.Context, string) (int64, error)        { return 3, nil }
func (f *fakeRoom) Depths(_ context.Context, ids []string) (map[string]int64, error) {
	out := map[string]int64{}
	for _, i := range ids {
		if e := f.events[i]; e != nil {
			out[i] = e.Depth
		}
	}
	return out, nil
}
func (f *fakeRoom) StateGroupForEvent(context.Context, string) (int64, error)  { return 7, nil }
func (f *fakeRoom) StateForGroup(context.Context, int64) (pdu.StateMap, error) { return f.state, nil }
func (f *fakeRoom) StateDelta(context.Context, int64) (int64, bool, pdu.StateMap, error) {
	return 6, true, pdu.StateMap{{Type: "m.room.power_levels"}: f.prevs[0]}, nil
}
func (f *fakeRoom) GetStoredEvents(_ context.Context, ids []string) (map[string]*pdustore.StoredEvent, map[string]error, error) {
	out := map[string]*pdustore.StoredEvent{}
	for _, i := range ids {
		if e := f.events[i]; e != nil {
			out[i] = e
		}
	}
	return out, map[string]error{}, nil
}
func (f *fakeRoom) RejectionColumn(context.Context, string) (*string, error) { return nil, nil }
func (f *fakeRoom) EventRoom(_ context.Context, ev string) (string, error) {
	if f.events[ev] != nil {
		return room, nil
	}
	return "", pdustore.ErrNotFound
}
func (f *fakeRoom) IsRelationTarget(context.Context, string) (bool, error) { return false, nil }
func (f *fakeRoom) HasRelation(context.Context, string) (bool, error)      { return false, nil }
func (f *fakeRoom) HasAnnotated(context.Context, string, string, string, string) (bool, error) {
	return false, nil
}

type fakeWriter struct {
	calls      []string
	groupDelta []writer.StateRow
}

func (w *fakeWriter) MarkTokenUsed(context.Context, int64) error {
	w.calls = append(w.calls, "token_used")
	return nil
}
func (w *fakeWriter) BumpDeletion(_ context.Context, g []int64) ([]int64, error) {
	w.calls = append(w.calls, "bump")
	return nil, nil
}
func (w *fakeWriter) SetRoomParticipation(context.Context, string, string) error {
	w.calls = append(w.calls, "participation")
	return nil
}
func (w *fakeWriter) StagePush(context.Context, string, []writer.PushRow) error {
	w.calls = append(w.calls, "stage")
	return nil
}
func (w *fakeWriter) StoreStateGroup(_ context.Context, ev, room string, prev int64, delta []writer.StateRow, _ func() ([]writer.StateRow, error)) (int64, error) {
	w.calls = append(w.calls, "state_group")
	w.groupDelta = delta
	return 8, nil
}
func (w *fakeWriter) UnstagePush(context.Context, string) error {
	w.calls = append(w.calls, "unstage")
	return nil
}

type fakePersister struct {
	entries []json.RawMessage
	err     error
	answer  string
}

func (p *fakePersister) SendClientEvents(_ context.Context, instance string, entries []json.RawMessage) (int64, string, error) {
	p.entries = append(p.entries, entries...)
	if p.err != nil {
		return 0, "", p.err
	}
	return 99, p.answer, nil
}

type harness struct {
	c    *Creator
	f    *fakeRoom
	w    *fakeWriter
	p    *fakePersister
	main *fakeMain
}

type fakeMain struct{ left []string }

func (m *fakeMain) UserLeftRoom(_ context.Context, roomID, userID string) error {
	m.left = append(m.left, roomID+" "+userID)
	return nil
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	key, err := build.ParseKey("example.org", federation.GenerateSigningKey().SynapseString())
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{f: newFakeRoom(t, key), w: &fakeWriter{}, p: &fakePersister{}, main: &fakeMain{}}
	lim := synapsecfg.Ratelimit{PerSecond: 1, Burst: 10}
	h.c = New(Options{Store: h.f, Writer: h.w, Persister: h.p, Key: key,
		ServerName: "example.org", EventsWriter: func(string) string { return "persister" }, Sticky: true,
		Membership: synapsecfg.Membership{AllowPerRoomProfiles: true, IncludeProfileDataOnInvite: true,
			JoinsLocal: lim, JoinsPerRoom: lim, InvitesPerRoom: lim, InvitesPerUser: lim, InvitesPerIssuer: lim},
		MSC4155: true, MSC4293: true, Main: h.main,
		Log: zerolog.Nop(), Now: func() time.Time { return time.UnixMilli(1700000000000) }})
	h.c.verify = func(pending) {}
	return h
}

func (h *harness) send(t *testing.T, method, txn, body string) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	path := "/_matrix/client/v3/rooms/" + room + "/send/m.room.message"
	req := route.Request{Endpoint: route.Send, Write: true, Room: room, Action: "m.room.message", TxnID: txn}
	if txn != "" {
		path += "/" + txn
	}
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	out := h.c.Serve(rec, r, req)
	return rec, out
}

func TestNativeSend(t *testing.T) {
	h := newHarness(t)
	rec, out := h.send(t, http.MethodPut, "t1", `{"msgtype":"m.text","body":"hi"}`)
	if out.Fallback != "" || out.Result != "created" {
		t.Fatalf("outcome %+v", out)
	}
	if rec.Code != 200 {
		t.Fatalf("status %d %s", rec.Code, rec.Body)
	}
	eventID := gjson.Get(rec.Body.String(), "event_id").Str
	if !strings.HasPrefix(eventID, "$") {
		t.Fatalf("response %s", rec.Body)
	}
	if got := strings.Join(h.w.calls, ","); got != "bump,participation,stage" {
		t.Errorf("writes %s", got)
	}
	if len(h.p.entries) != 1 {
		t.Fatalf("%d entries sent", len(h.p.entries))
	}
	e := gjson.ParseBytes(h.p.entries[0])
	for path, want := range map[string]string{
		"room_version":                     `"11"`,
		"event_format_version":             "3",
		"outlier":                          "false",
		"rejected_reason":                  "null",
		"ratelimit":                        "true",
		"extra_users":                      "[]",
		"internal_metadata.device_id":      `"DEV"`,
		"internal_metadata.txn_id":         `"t1"`,
		"context.state_group":              "7",
		"context.state_group_before_event": "7",
		"context.rejected":                 "null",
		"context.state_delta_due_to_event": "[]",
		"context.app_service_id":           "null",
		"context.partial_state":            "false",
		"context.state_group_deltas.#":     "2",
		"context.state_group_deltas.0":     "[7,7,[]]",
		"context.state_group_deltas.1.0":   "6",
		"context.state_group_deltas.1.1":   "7",
		"requester.user_id":                `"` + user + `"`,
		"requester.access_token_id":        "42",
		"requester.device_id":              `"DEV"`,
		"requester.is_guest":               "false",
		"requester.shadow_banned":          "false",
		"requester.scope":                  "[]",
		"requester.app_server_id":          "null",
		"requester.authenticated_entity":   `"` + user + `"`,
		"event.unsigned.age_ts":            "1700000000000",
		"event.origin_server_ts":           "1700000000000",
		"event.depth":                      "5",
		"event.prev_events.#":              "1",
		"event.auth_events.#":              "3",
		"event.content.body":               `"hi"`,
		"event.type":                       `"m.room.message"`,
		"event.sender":                     `"` + user + `"`,
	} {
		if got := e.Get(path).Raw; got != want {
			t.Errorf("%s = %s, want %s", path, got, want)
		}
	}
	if e.Get("internal_metadata.token_id").Exists() {
		t.Error("token_id in internal_metadata: create_event no longer sets it")
	}
	// The event the writer gets is the one we answered with.
	rv, _ := pdu.LookupRoomVersion("11")
	gotID, err := pdu.CalculateEventID([]byte(e.Get("event").Raw), rv)
	if err != nil || gotID != eventID {
		t.Errorf("event id of the sent event %s (%v), answered %s", gotID, err, eventID)
	}
}

func TestPOSTHasNoTxnID(t *testing.T) {
	h := newHarness(t)
	if _, out := h.send(t, http.MethodPost, "", `{"msgtype":"m.text","body":"hi"}`); out.Result != "created" {
		t.Fatalf("outcome %+v", out)
	}
	if gjson.GetBytes(h.p.entries[0], "internal_metadata.txn_id").Exists() {
		t.Error("txn_id sent without a transaction")
	}
}

func TestStickyIsSent(t *testing.T) {
	h := newHarness(t)
	path := "/_matrix/client/v3/rooms/" + room + "/send/m.room.message/t?" + StickyParam + "=60000"
	r := httptest.NewRequest(http.MethodPut, path, strings.NewReader(`{"msgtype":"m.text","body":"hi"}`))
	r.Header.Set("Authorization", "Bearer tok")
	out := h.c.Serve(httptest.NewRecorder(), r, route.Request{Endpoint: route.Send, Room: room, Action: "m.room.message", TxnID: "t"})
	if out.Result != "created" {
		t.Fatalf("outcome %+v", out)
	}
	if gjson.GetBytes(h.p.entries[0], "event.msc4354_sticky.duration_ms").Int() != 60000 {
		t.Errorf("sticky missing: %s", h.p.entries[0])
	}
}

// stopped is why a request was not served: its fallback, or "refused:" and
// the refusal.
func stopped(out Outcome) string {
	if out.Refused != "" {
		return "refused:" + out.Refused
	}
	return out.Fallback
}

// Every fallback and refusal is decided before the first write.
func TestFallbacksWriteNothing(t *testing.T) {
	cases := map[string]func(h *harness) (string, string){
		"two extremities": func(h *harness) (string, string) {
			h.f.prevs = append(h.f.prevs, h.f.prevs[0]+"x")
			return `{"msgtype":"m.text","body":"hi"}`, "prev_events_2"
		},
		"not joined": func(h *harness) (string, string) {
			h.f.token = &pdustore.TokenInfo{ID: 1, UserID: user, DeviceID: "DEV", Used: true}
			delete(h.f.state, pdu.StateKey{Type: "m.room.member", StateKey: user})
			return `{"msgtype":"m.text","body":"hi"}`, "refused:not_in_room"
		},
		"shadow banned": func(h *harness) (string, string) {
			h.f.token = &pdustore.TokenInfo{ID: 1, UserID: user, DeviceID: "DEV", Used: true, ShadowBan: true}
			return `{"msgtype":"m.text","body":"hi"}`, "shadow_banned"
		},
		"partial state": func(h *harness) (string, string) {
			h.f.partial = true
			return `{"msgtype":"m.text","body":"hi"}`, "partial_state"
		},
		"bad message":   func(*harness) (string, string) { return `{"msgtype":"m.text"}`, "refused:invalid_message" },
		"not an object": func(*harness) (string, string) { return `[1]`, "refused:not_object" },
		"bad mentions": func(*harness) (string, string) {
			return `{"msgtype":"m.text","body":"x","m.mentions":{"room":1}}`, "refused:invalid_mentions"
		},
		"unknown relation": func(*harness) (string, string) {
			return `{"msgtype":"m.text","body":"x","m.relates_to":{"rel_type":"m.replace","event_id":"$nope"}}`, "refused:relation_unknown"
		},
		"float": func(*harness) (string, string) {
			return `{"msgtype":"m.text","body":"x","n":1.5}`, "refused:non_canonical_json"
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			body, want := setup(h)
			rec, out := h.send(t, http.MethodPut, "t1", body)
			if stopped(out) != want {
				t.Fatalf("stopped %q, want %q", stopped(out), want)
			}
			if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
				t.Errorf("a fallback wrote: %v, %d entries", h.w.calls, len(h.p.entries))
			}
			if out.Fallback != "" && (rec.Code != 200 || rec.Body.Len() != 0) {
				t.Errorf("a fallback answered the client: %d %s", rec.Code, rec.Body)
			}
		})
	}
}

func TestTxnDedupAnswersWithoutWriting(t *testing.T) {
	h := newHarness(t)
	h.f.txn = "$earlier"
	rec, out := h.send(t, http.MethodPut, "seen", `{"msgtype":"m.text","body":"hi"}`)
	if out.Result != "txn_dedup" || gjson.Get(rec.Body.String(), "event_id").Str != "$earlier" {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
		t.Error("a deduplicated transaction wrote")
	}
}

// The writer's refusal reaches the client as Synapse relays it, and the
// staged push actions are removed.
func TestWriterRefusalIsRelayed(t *testing.T) {
	h := newHarness(t)
	h.p.err = &replication.HTTPError{Status: 429, Body: `{"errcode":"M_LIMIT_EXCEEDED","retry_after_ms":100}`}
	rec, out := h.send(t, http.MethodPut, "t1", `{"msgtype":"m.text","body":"hi"}`)
	if out.Result != "writer_429" || rec.Code != 429 || !strings.Contains(rec.Body.String(), "M_LIMIT_EXCEEDED") {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	if got := strings.Join(h.w.calls, ","); got != "bump,participation,stage,unstage" {
		t.Errorf("writes %s", got)
	}
}

func TestWriterDedupAnswersWithItsEvent(t *testing.T) {
	h := newHarness(t)
	h.p.answer = "$theirs"
	rec, out := h.send(t, http.MethodPut, "t1", `{"msgtype":"m.text","body":"hi"}`)
	if out.Result != "writer_dedup" || gjson.Get(rec.Body.String(), "event_id").Str != "$theirs" {
		t.Fatalf("%+v %s", out, rec.Body)
	}
}

func TestRequestsLeftToSynapse(t *testing.T) {
	for _, tc := range []struct{ path, auth, want string }{
		{"/send/m.room.message/t?org.matrix.msc4140.delay=1000", "Bearer tok", "delayed"},
		{"/send/m.room.message/t?user_id=@ghost:example.org", "Bearer tok", "appservice"},
		{"/send/m.room.message/t", "Bearer as-secret", "appservice"},
		{"/send/m.room.message/t", "Bearer MDAxY2xvY2F0aW9uIGV4YW1wbGUub3JnCg", "guest_token"},
	} {
		h := newHarness(t)
		h.c.o.AppServiceTokens = map[string]bool{"as-secret": true}
		r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/"+room+tc.path, strings.NewReader(`{"msgtype":"m.text","body":"hi"}`))
		if tc.auth != "" {
			r.Header.Set("Authorization", tc.auth)
		}
		out := h.c.Serve(httptest.NewRecorder(), r, route.Request{Endpoint: route.Send, Room: room, Action: "m.room.message", TxnID: "t"})
		if out.Fallback != tc.want {
			t.Errorf("%s: fallback %q, want %q", tc.path, out.Fallback, tc.want)
		}
	}
}

// get_user_by_req's refusals, in Synapse's words.
func TestAuthRefusals(t *testing.T) {
	past := int64(1)
	for name, tc := range map[string]struct {
		query   string
		headers []string
		token   *pdustore.TokenInfo
		status  int
		body    string
	}{
		"no token": {"", nil, nil, 401, `{"errcode":"M_MISSING_TOKEN","error":"Missing access token"}`},
		"both": {"?access_token=tok", []string{"Bearer tok"}, nil, 401,
			`{"errcode":"M_MISSING_TOKEN","error":"Mixing Authorization headers and access_token query parameters."}`},
		"two headers": {"", []string{"Bearer tok", "Bearer tok"}, nil, 401,
			`{"errcode":"M_MISSING_TOKEN","error":"Too many Authorization headers."}`},
		"not bearer": {"", []string{"Basic tok"}, nil, 401, `{"errcode":"M_MISSING_TOKEN","error":"Invalid Authorization header."}`},
		"unknown": {"", []string{"Bearer syt_dGVzdA_abcdefghijklmnopqrst_0AbCdE"}, nil, 401,
			`{"errcode":"M_UNKNOWN_TOKEN","error":"Invalid access token passed.","soft_logout":false}`},
		"empty query": {"?access_token=", nil, nil, 401,
			`{"errcode":"M_UNKNOWN_TOKEN","error":"Invalid access token passed.","soft_logout":false}`},
		"expired": {"", []string{"Bearer tok"}, &pdustore.TokenInfo{ID: 1, UserID: user, DeviceID: "DEV", Used: true, ValidUntilMS: &past}, 401,
			`{"errcode":"M_UNKNOWN_TOKEN","error":"Access token has expired","soft_logout":true}`},
		"locked": {"", []string{"Bearer tok"}, &pdustore.TokenInfo{ID: 1, UserID: user, DeviceID: "DEV", Used: true, Locked: true}, 401,
			`{"errcode":"M_USER_LOCKED","error":"User account has been locked","soft_logout":true}`},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			if tc.token != nil {
				h.f.token = tc.token
			}
			r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/"+room+"/send/m.room.message/t"+tc.query,
				strings.NewReader(`{"msgtype":"m.text","body":"hi"}`))
			for _, v := range tc.headers {
				r.Header.Add("Authorization", v)
			}
			rec := httptest.NewRecorder()
			out := h.c.Serve(rec, r, route.Request{Endpoint: route.Send, Room: room, Action: "m.room.message", TxnID: "t"})
			if out.Result != "refused" || rec.Code != tc.status || rec.Body.String() != tc.body {
				t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
			}
			if len(h.w.calls) != 0 {
				t.Errorf("wrote %v", h.w.calls)
			}
		})
	}
}

// A token's first request marks it used, as Synapse does.
func TestFirstRequestMarksTokenUsed(t *testing.T) {
	h := newHarness(t)
	h.f.token.Used = false
	if _, out := h.send(t, http.MethodPut, "t1", `{"msgtype":"m.text","body":"hi"}`); out.Result != "created" {
		t.Fatalf("%+v", out)
	}
	if h.w.calls[0] != "token_used" {
		t.Errorf("writes %v", h.w.calls)
	}
}

func TestMightBeMacaroon(t *testing.T) {
	for tok, want := range map[string]bool{
		"syt_dGVzdA_abcdefghijklmnopqrst_0AbCdE": false,
		"MDAxY2xvY2F0aW9uIGV4YW1wbGUub3JnCg":     true, // "001clocation example.org"
		"AgEL":                                   true, // v2: version byte 2
		`{"v":2}`:                                true,
		"":                                       false,
		"abcde":                                  false, // 4n+1 characters
	} {
		if got := mightBeMacaroon(tok); got != want {
			t.Errorf("%q: %v, want %v", tok, got, want)
		}
	}
}

// A fallback leaves the body readable for the proxy.
func TestFallbackLeavesBodyReadable(t *testing.T) {
	h := newHarness(t)
	h.f.partial = true
	const body = `{"msgtype":"m.text","body":"hi"}`
	r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/"+room+"/send/m.room.message/t", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	h.c.Serve(httptest.NewRecorder(), r, route.Request{Endpoint: route.Send, Room: room, Action: "m.room.message", TxnID: "t"})
	b := make([]byte, 100)
	n, _ := r.Body.Read(b)
	if string(b[:n]) != body {
		t.Errorf("body after fallback: %q", b[:n])
	}
}

// TestValidatorAgainstSynapse runs validate_builder and validate_new's port
// over vectors Synapse answered itself (testdata/oracle_validator.py).
func TestValidatorAgainstSynapse(t *testing.T) {
	raw, err := os.ReadFile("testdata/oracle_validator.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases [][]json.RawMessage
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	c := &Creator{o: Options{ServerName: "example.org"}}
	for _, cs := range cases {
		var typ, content string
		var sk *string
		var want []any
		_ = json.Unmarshal(cs[0], &typ)
		_ = json.Unmarshal(cs[1], &sk)
		_ = json.Unmarshal(cs[2], &content)
		_ = json.Unmarshal(cs[3], &want)
		in := &input{typ: typ, stateKey: sk, content: []byte(content)}
		v := gjson.Parse(content)
		st := validateBuilder(in, v, room)
		if st == nil {
			st = c.validateNew(in, in.content, v)
		}
		name := fmt.Sprintf("%s %v %s", typ, sk != nil, content)
		switch {
		case want == nil && st != nil:
			t.Errorf("%s: Synapse accepts it, we %+v %+v", name, st.fallback, st.ref)
		case want != nil && st == nil:
			t.Errorf("%s: Synapse refuses it (%v), we accept it", name, want)
		case want != nil && st.ref == nil:
			t.Errorf("%s: Synapse refuses it (%v), we fall back (%s)", name, want, st.fallback)
		case want != nil:
			got := []any{float64(st.ref.status), st.ref.errcode, st.ref.msg}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("%s:\n got  %v\n want %v", name, got, want)
			}
		}
	}
}

func TestRoomLocksSerialise(t *testing.T) {
	var l roomLocks
	unlock := l.lock("!a")
	done := make(chan struct{})
	go func() {
		u := l.lock("!a")
		u()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("second lock of the same room did not wait")
	case <-time.After(20 * time.Millisecond):
	}
	other := l.lock("!b")
	other()
	unlock()
	<-done
	if len(l.rooms) != 0 {
		t.Errorf("%d rooms left in the lock table", len(l.rooms))
	}
}

func (h *harness) put(t *testing.T, req route.Request, path, body string) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/"+room+path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	return rec, h.c.Serve(rec, r, req)
}

func stateReq(typ, key string) route.Request {
	return route.Request{Endpoint: route.State, Write: true, Room: room, Action: typ, StateKey: key}
}

func TestNativeState(t *testing.T) {
	h := newHarness(t)
	rec, out := h.put(t, stateReq("m.room.topic", ""), "/state/m.room.topic/", `{"topic":"new"}`)
	if out.Result != "created" || rec.Code != 200 {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	id := gjson.Get(rec.Body.String(), "event_id").Str
	if got := strings.Join(h.w.calls, ","); got != "bump,state_group,stage" {
		t.Errorf("writes %s (no room participation for state)", got)
	}
	if len(h.w.groupDelta) != 1 || h.w.groupDelta[0] != (writer.StateRow{Type: "m.room.topic", StateKey: "", EventID: id}) {
		t.Errorf("group delta %v", h.w.groupDelta)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	for path, want := range map[string]string{
		"context.state_group":              "8",
		"context.state_group_before_event": "7",
		"context.state_group_deltas.#":     "2",
		"context.state_group_deltas.0":     `[7,8,[["m.room.topic","","` + id + `"]]]`,
		"context.state_group_deltas.1.0":   "6",
		"context.state_group_deltas.1.1":   "7",
		"context.state_delta_due_to_event": `[["m.room.topic","","` + id + `"]]`,
		"event.state_key":                  `""`,
		"event.unsigned.replaces_state":    `"` + h.f.topic + `"`,
		"event.content":                    `{"topic":"new"}`,
	} {
		if got := e.Get(path).Raw; got != want {
			t.Errorf("%s = %s, want %s", path, got, want)
		}
	}
	if e.Get("internal_metadata.txn_id").Exists() {
		t.Error("/state has no transaction")
	}
}

func TestStateDedupWritesNothing(t *testing.T) {
	h := newHarness(t)
	rec, out := h.put(t, stateReq("m.room.topic", ""), "/state/m.room.topic/", `{ "topic" : "old" }`)
	if out.Result != "state_dedup" || gjson.Get(rec.Body.String(), "event_id").Str != h.f.topic {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
		t.Errorf("a duplicate wrote: %v", h.w.calls)
	}
}

func TestStateFallbacks(t *testing.T) {
	for _, tc := range []struct {
		typ, key, body, want string
	}{
		{"m.room.member", "@x:example.org", `{"membership":"join"}`, "type_member"},
		{"m.room.power_levels", "", `{"users":{"@u:example.org":"100"}}`, "refused:invalid_power_levels"},
		{"m.room.server_acl", "", `{"allow":["*"],"deny":["example.org"]}`, "refused:acl_denies_us"},
		{"m.room.create", "", `{}`, "refused:type_create"},
		{"m.room.topic", "", `{"topic":1}`, "refused:invalid_topic"},
		{"m.room.name", "", `{}`, "refused:invalid_name"},
		{"m.room.tombstone", "", `{"replacement_room":"` + room + `"}`, "refused:invalid_tombstone"},
		{"m.room.tombstone", "", `{}`, "refused:invalid_tombstone"},
		{"m.room.retention", "", `{"min_lifetime":10,"max_lifetime":5}`, "refused:invalid_retention"},
		{"m.room.aliases", "example.org", `{"aliases":["#` + strings.Repeat("a", 300) + `:example.org"]}`, "refused:alias_too_long"},
	} {
		h := newHarness(t)
		_, out := h.put(t, stateReq(tc.typ, tc.key), "/state/"+tc.typ+"/"+tc.key, tc.body)
		if stopped(out) != tc.want {
			t.Errorf("%s: stopped %q, want %q", tc.typ, stopped(out), tc.want)
		}
		if len(h.w.calls) != 0 {
			t.Errorf("%s: wrote %v", tc.typ, h.w.calls)
		}
	}
}

func redactReq(target, txn string) route.Request {
	return route.Request{Endpoint: route.Redact, Write: true, Room: room, Action: target, TxnID: txn}
}

func TestNativeRedaction(t *testing.T) {
	h := newHarness(t)
	rec, out := h.put(t, redactReq(h.f.topic, "r1"), "/redact/x/r1", `{"reason":"test"}`)
	if out.Result != "created" || rec.Code != 200 {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	for path, want := range map[string]string{
		"event.type":                   `"m.room.redaction"`,
		"event.content.redacts":        `"` + h.f.topic + `"`,
		"event.content.reason":         `"test"`,
		"internal_metadata.txn_id":     `"r1"`,
		"context.state_group":          "7",
		"context.state_group_deltas.0": "[7,7,[]]",
	} {
		if got := e.Get(path).Raw; got != want {
			t.Errorf("%s = %s, want %s", path, got, want)
		}
	}
	if e.Get("event.redacts").Exists() {
		t.Error("v11 redaction with a top-level redacts")
	}
	if e.Get("internal_metadata.recheck_redaction").Exists() {
		t.Error("recheck_redaction for a sender with redact power")
	}
	if got := strings.Join(h.w.calls, ","); got != "bump,stage" {
		t.Errorf("writes %s", got)
	}
}

func TestRedactionFallbacks(t *testing.T) {
	h := newHarness(t)
	for name, tc := range map[string]struct{ target, body, want string }{
		"create event":    {h.f.create, `{}`, "refused:redact_create"},
		"redacts differs": {h.f.topic, `{"redacts":"$other"}`, "refused:redacts_mismatch"},
		"with relations":  {h.f.topic, `{"org.matrix.msc3912.with_relations":["m.thread"]}`, "with_relations"},
	} {
		h := newHarness(t)
		target := tc.target
		if name == "create event" {
			target = h.f.create
		}
		if name == "redacts differs" || name == "with relations" {
			target = h.f.topic
		}
		_, out := h.put(t, redactReq(target, "r"), "/redact/x/r", tc.body)
		if stopped(out) != tc.want {
			t.Errorf("%s: stopped %q, want %q", name, stopped(out), tc.want)
		}
		if len(h.w.calls) != 0 {
			t.Errorf("%s: wrote %v", name, h.w.calls)
		}
	}
}

func TestServerACLValidator(t *testing.T) {
	for acl, refused := range map[string]bool{
		`{"allow":["*"],"deny":["evil.example"]}`: false,
		`{"allow":[]}`:                     true,
		`{"allow":["*"],"deny":["*.org"]}`: true,
	} {
		st := validateServerACL([]byte(acl), "example.org")
		if (st != nil && st.ref != nil) != refused {
			t.Errorf("%s: %+v", acl, st)
		}
	}
}

// A redaction of an event we do not hold is sent: the writer checks nothing
// either, and applies it when the event arrives.
func TestRedactionOfUnknownEvent(t *testing.T) {
	h := newHarness(t)
	rec, out := h.put(t, redactReq("$unknown", "r1"), "/redact/x/r1", `{}`)
	if out.Result != "created" || rec.Code != 200 {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	if got := gjson.GetBytes(h.p.entries[0], "event.content.redacts").Str; got != "$unknown" {
		t.Errorf("redacts %q", got)
	}
}
