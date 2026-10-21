package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/allowlist"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/auth"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/config"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdu"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdustore"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/proxy"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/shadow"
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

// With shadow.all_rooms a write in a room outside the allowlist is read and
// checked; without, only proxied. Either way it reaches main unchanged.
func TestShadowAllRooms(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"event_id":"$x"}`))
	}))
	defer upstream.Close()
	for _, all := range []bool{false, true} {
		cfg := &config.Config{Modes: map[route.Endpoint]config.Mode{}, Shadow: config.Shadow{AllRooms: all}}
		for _, ep := range route.Endpoints {
			cfg.Modes[ep] = config.ModeShadow
		}
		up, _ := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
		rooms, _ := allowlist.New(allowlist.Permitted)
		h := New(cfg, up, rooms, shadow.New(shadow.Options{Store: quickStore{}, Auth: quickAuth{}, Log: zerolog.Nop()}), nil, zerolog.Nop())
		want := "proxy"
		if all {
			want = "shadow"
		}
		before := testutil.ToFloat64(requests.WithLabelValues("send", "true", "other", want, "2xx"))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/%21elsewhere%3Aexample.org/send/m.room.message/t1",
			strings.NewReader(`{"msgtype":"m.text","body":"hi"}`)))
		if rec.Code != 200 || testutil.ToFloat64(requests.WithLabelValues("send", "true", "other", want, "2xx")) != before+1 {
			t.Errorf("all_rooms %v: %d, not counted as %s", all, rec.Code, want)
		}
	}
}

type quickStore struct{}

func (quickStore) PrevEventsForRoom(context.Context, string) ([]string, error) {
	return []string{"$p"}, nil
}
func (quickStore) RoomMaxStream(context.Context, string) (int64, error) { return 1, nil }
func (quickStore) RoomVersion(context.Context, string) (string, error)  { return "11", nil }
func (quickStore) TxnEvent(context.Context, string, string, string, string) (string, error) {
	return "", pdustore.ErrNotFound
}
func (quickStore) GetStoredEvents(context.Context, []string) (map[string]*pdustore.StoredEvent, map[string]error, error) {
	return nil, nil, nil
}
func (quickStore) Depths(context.Context, []string) (map[string]int64, error) { return nil, nil }
func (quickStore) StateGroupForEvent(context.Context, string) (int64, error)  { return 1, nil }
func (quickStore) StateForGroup(context.Context, int64) (pdu.StateMap, error) {
	return pdu.StateMap{}, nil
}
func (quickStore) StateDelta(context.Context, int64) (int64, bool, pdu.StateMap, error) {
	return 0, false, nil, nil
}
func (quickStore) NewMembership(context.Context, string, int64, string, string, string) (string, error) {
	return "", nil
}

type quickAuth struct{}

func (quickAuth) AuthenticateAs(context.Context, auth.Credentials) (auth.Verdict, error) {
	return auth.Verdict{Valid: true, UserID: "@u:example.org", DeviceID: "D"}, nil
}

func (quickStore) GetEvents(context.Context, []string) (map[string]*pdu.Event, error) {
	return nil, nil
}
func (quickStore) AuthEdges(context.Context, []string) (map[string][]string, error) { return nil, nil }
