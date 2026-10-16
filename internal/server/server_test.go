package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/allowlist"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/config"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/proxy"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

// TestProxyModeForwardsEverythingUnchanged: in proxy mode every request,
// allowlisted room or not, reaches Python with its method, URI and body.
func TestProxyModeForwardsEverythingUnchanged(t *testing.T) {
	type seen struct{ method, uri, body string }
	got := make(chan seen, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- seen{r.Method, r.URL.RequestURI(), string(b)}
		_, _ = w.Write([]byte(`{"event_id":"$x"}`))
	}))
	defer upstream.Close()

	cfg := &config.Config{Modes: map[route.Endpoint]config.Mode{}}
	for _, ep := range route.Endpoints {
		cfg.Modes[ep] = config.ModeProxy
	}
	up, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	rooms, err := allowlist.New(allowlist.Permitted)
	if err != nil {
		t.Fatal(err)
	}
	h := New(cfg, up, rooms, nil, nil, zerolog.Nop())

	for _, uri := range []string{
		"/_matrix/client/v3/rooms/%21decSfHzLLhrNKJaZid%3Aaguiarvieira.pt/send/m.room.message/t1",
		"/_matrix/client/v3/rooms/%21elsewhere%3Aaguiarvieira.pt/send/m.room.message/t1",
		"/_matrix/client/v3/profile/%40u%3Aaguiarvieira.pt/displayname",
	} {
		before := testutil.ToFloat64(requests.WithLabelValues("send", "true", "allowlisted", "proxy", "2xx"))
		const body = `{"msgtype":"m.text","body":"hi"}`
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, uri, strings.NewReader(body)))

		s := <-got
		if s.method != http.MethodPut || s.uri != uri || s.body != body {
			t.Errorf("upstream saw %+v for %s", s, uri)
		}
		if rec.Code != 200 || rec.Body.String() != `{"event_id":"$x"}` {
			t.Errorf("%s: response %d %q", uri, rec.Code, rec.Body.String())
		}
		after := testutil.ToFloat64(requests.WithLabelValues("send", "true", "allowlisted", "proxy", "2xx"))
		wantDelta := 0.0
		if strings.Contains(uri, "decSfHzLLhrNKJaZid") {
			wantDelta = 1
		}
		if after-before != wantDelta {
			t.Errorf("%s: allowlisted counter moved by %v, want %v", uri, after-before, wantDelta)
		}
	}
}
