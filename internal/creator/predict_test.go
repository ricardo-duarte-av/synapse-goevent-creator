package creator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

// A predictor answers as the native path would and changes nothing: no
// write, no event sent, for an event it would create as for a refusal.
func TestPredictorWritesNothing(t *testing.T) {
	h := newHarness(t)
	p := h.c.Predictor()
	predict := func(method, path, body string, req route.Request) (int, string, string) {
		r := httptest.NewRequest(method, "/_matrix/client/v3/rooms/"+room+path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer tok")
		status, b, kind := p.Predict(r, req)
		return status, string(b), kind
	}
	send := route.Request{Endpoint: route.Send, Write: true, Room: room, Action: "m.room.message", TxnID: "p1"}
	if _, _, kind := predict(http.MethodPut, "/send/m.room.message/p1", `{"msgtype":"m.text","body":"hi"}`, send); kind != PredictAccepted {
		t.Errorf("message: %s", kind)
	}
	status, body, kind := predict(http.MethodPut, "/send/m.room.message/p1", `{"msgtype":"m.text"}`, send)
	if kind != PredictRefused || status != 400 || body != `{"errcode":"M_UNKNOWN","error":"'body' not in content"}` {
		t.Errorf("refusal: %s %d %s", kind, status, body)
	}
	h.f.addState(t, "m.room.member", other, `{"membership":"join"}`)
	if _, _, kind := predict(http.MethodPost, "/kick", `{"user_id":"`+other+`"}`, memberReq("kick", "")); kind != PredictAccepted {
		t.Errorf("kick: %s", kind)
	}
	delayed := route.Request{Endpoint: route.Send, Write: true, Room: room, Action: "m.room.message", TxnID: "p2"}
	if _, _, kind := predict(http.MethodPut, "/send/m.room.message/p2?org.matrix.msc4140.delay=10", `{"msgtype":"m.text","body":"x"}`, delayed); kind != PredictAccepted {
		t.Errorf("delayed: %s", kind)
	}
	if len(h.w.calls) != 0 || len(h.p.entries) != 0 || len(h.main.left) != 0 || len(h.main.delayed) != 0 {
		t.Errorf("a prediction wrote: %v, %d sent, main %v %v", h.w.calls, len(h.p.entries), h.main.left, h.main.delayed)
	}
	// What the native path leaves to Synapse it leaves in a prediction too.
	if _, _, kind := predict(http.MethodPost, "/invite", `{"user_id":"@x:remote.example"}`, memberReq("invite", "")); kind != PredictFallback {
		t.Errorf("remote invite: %s", kind)
	}
}
