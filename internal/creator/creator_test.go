package creator

import (
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/federation"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/auth"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/build"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdu"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdustore"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/replication"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
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
}

func newFakeRoom(t *testing.T, key *build.Key) *fakeRoom {
	t.Helper()
	f := &fakeRoom{events: map[string]*pdustore.StoredEvent{}, state: pdu.StateMap{},
		token: &pdustore.TokenInfo{ID: 42, UserID: user, DeviceID: "DEV"}}
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
	return f
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

type fakeAuth struct{}

func (fakeAuth) AuthenticateAs(_ context.Context, c auth.Credentials) (auth.Verdict, error) {
	if c.Token != "tok" {
		return auth.Verdict{Valid: false}, nil
	}
	return auth.Verdict{Valid: true, UserID: user, DeviceID: "DEV"}, nil
}

type harness struct {
	c *Creator
	f *fakeRoom
	w *fakeWriter
	p *fakePersister
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	key, err := build.ParseKey("example.org", federation.GenerateSigningKey().SynapseString())
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{f: newFakeRoom(t, key), w: &fakeWriter{}, p: &fakePersister{}}
	h.c = New(Options{Store: h.f, Writer: h.w, Persister: h.p, Auth: fakeAuth{}, Key: key,
		ServerName: "example.org", EventsWriter: func(string) string { return "persister" }, Sticky: true,
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

// Every fallback is decided before the first write.
func TestFallbacksWriteNothing(t *testing.T) {
	cases := map[string]func(h *harness) (string, string){
		"two extremities": func(h *harness) (string, string) {
			h.f.prevs = append(h.f.prevs, h.f.prevs[0]+"x")
			return `{"msgtype":"m.text","body":"hi"}`, "prev_events_2"
		},
		"not joined": func(h *harness) (string, string) {
			h.f.token = &pdustore.TokenInfo{ID: 1, UserID: user, DeviceID: "DEV"}
			delete(h.f.state, pdu.StateKey{Type: "m.room.member", StateKey: user})
			return `{"msgtype":"m.text","body":"hi"}`, "auth_rules"
		},
		"shadow banned": func(h *harness) (string, string) {
			h.f.token = &pdustore.TokenInfo{ID: 1, UserID: user, DeviceID: "DEV", ShadowBan: true}
			return `{"msgtype":"m.text","body":"hi"}`, "shadow_banned"
		},
		"partial state": func(h *harness) (string, string) {
			h.f.partial = true
			return `{"msgtype":"m.text","body":"hi"}`, "partial_state"
		},
		"bad message":   func(*harness) (string, string) { return `{"msgtype":"m.text"}`, "invalid_message" },
		"not an object": func(*harness) (string, string) { return `[1]`, "not_object" },
		"bad mentions": func(*harness) (string, string) {
			return `{"msgtype":"m.text","body":"x","m.mentions":{"room":1}}`, "invalid_mentions"
		},
		"unknown relation": func(*harness) (string, string) {
			return `{"msgtype":"m.text","body":"x","m.relates_to":{"rel_type":"m.replace","event_id":"$nope"}}`, "relation_unknown"
		},
		"float": func(*harness) (string, string) { return `{"msgtype":"m.text","body":"x","n":1.5}`, "invalid_json" },
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			body, want := setup(h)
			rec, out := h.send(t, http.MethodPut, "t1", body)
			if out.Fallback != want {
				t.Fatalf("fallback %q, want %q", out.Fallback, want)
			}
			if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
				t.Errorf("a fallback wrote: %v, %d entries", h.w.calls, len(h.p.entries))
			}
			if rec.Code != 200 || rec.Body.Len() != 0 {
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
		{"/send/m.room.message/t", "", "unauthenticated"},
		{"/send/m.room.message/t", "Bearer nope", "unauthenticated"},
	} {
		h := newHarness(t)
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

func TestValidateSend(t *testing.T) {
	for _, tc := range []struct{ typ, content, want string }{
		{"m.room.message", `{"msgtype":"m.text","body":"x"}`, ""},
		{"m.room.message", `{"msgtype":"m.text","body":1}`, "invalid_message"},
		{"m.reaction", `{}`, ""},
		{"m.room.member", `{}`, "type_member"},
		{"m.room.topic", `{"topic":"x"}`, "type_state_only"},
		{"m.room.power_levels", `{}`, "type_validated"},
		{"m.room.message", `{"msgtype":"m.text","body":"x","m.mentions":{"user_ids":["@a:b"],"room":true}}`, ""},
		{"m.room.message", `{"msgtype":"m.text","body":"x","m.mentions":{"user_ids":[1]}}`, "invalid_mentions"},
		{"m.room.message", `{"msgtype":"m.text","body":"x","m.mentions":[]}`, "invalid_mentions"},
	} {
		if got := validate(&input{typ: tc.typ}, gjson.Parse(tc.content)); got != tc.want {
			t.Errorf("%s %s: %q, want %q", tc.typ, tc.content, got, tc.want)
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
		{"m.room.power_levels", "", `{}`, "type_validated"},
		{"m.room.server_acl", "", `{}`, "type_validated"},
		{"m.room.create", "", `{}`, "type_create"},
		{"m.room.topic", "", `{"topic":1}`, "invalid_topic"},
		{"m.room.name", "", `{}`, "invalid_name"},
		{"m.room.tombstone", "", `{"replacement_room":"!x:y"}`, "type_tombstone"},
	} {
		h := newHarness(t)
		_, out := h.put(t, stateReq(tc.typ, tc.key), "/state/"+tc.typ+"/"+tc.key, tc.body)
		if out.Fallback != tc.want {
			t.Errorf("%s: fallback %q, want %q", tc.typ, out.Fallback, tc.want)
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
		"create event":    {h.f.create, `{}`, "redact_create"},
		"unknown target":  {"$nope", `{}`, "redaction_target_unknown"},
		"redacts differs": {h.f.topic, `{"redacts":"$other"}`, "redacts_mismatch"},
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
		if out.Fallback != tc.want {
			t.Errorf("%s: fallback %q, want %q", name, out.Fallback, tc.want)
		}
		if len(h.w.calls) != 0 {
			t.Errorf("%s: wrote %v", name, h.w.calls)
		}
	}
}
