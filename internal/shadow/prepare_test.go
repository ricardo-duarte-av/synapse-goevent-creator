package shadow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/auth"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdu"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdustore"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

// slowStore answers after delay.
type slowStore struct{ delay time.Duration }

func (s slowStore) wait(ctx context.Context) error {
	select {
	case <-time.After(s.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s slowStore) PrevEventsForRoom(ctx context.Context, _ string) ([]string, error) {
	return []string{"$p"}, s.wait(ctx)
}
func (s slowStore) RoomMaxStream(ctx context.Context, _ string) (int64, error) { return 1, s.wait(ctx) }
func (s slowStore) RoomVersion(ctx context.Context, _ string) (string, error) {
	return "11", s.wait(ctx)
}
func (s slowStore) TxnEvent(ctx context.Context, _, _, _, _ string) (string, error) {
	return "", pdustore.ErrNotFound
}
func (s slowStore) GetStoredEvents(context.Context, []string) (map[string]*pdustore.StoredEvent, map[string]error, error) {
	return nil, nil, nil
}
func (s slowStore) Depths(context.Context, []string) (map[string]int64, error) { return nil, nil }
func (s slowStore) StateGroupForEvent(ctx context.Context, _ string) (int64, error) {
	return 1, s.wait(ctx)
}
func (s slowStore) StateForGroup(ctx context.Context, _ int64) (pdu.StateMap, error) {
	return pdu.StateMap{}, s.wait(ctx)
}
func (s slowStore) StateDelta(context.Context, int64) (int64, bool, pdu.StateMap, error) {
	return 0, false, nil, nil
}
func (s slowStore) NewMembership(context.Context, string, int64, string, string, string) (string, error) {
	return "", nil
}

type okAuth struct{}

func (okAuth) AuthenticateAs(context.Context, auth.Credentials) (auth.Verdict, error) {
	return auth.Verdict{Valid: true, UserID: "@u:example.org", DeviceID: "D"}, nil
}

func request() (*http.Request, route.Request) {
	r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/!r:example.org/send/m.room.message/t", strings.NewReader(`{"body":"x"}`))
	return r, route.Request{Endpoint: route.Send, Write: true, Room: "!r:example.org", Action: "m.room.message", TxnID: "t"}
}

// Outside the allowlist the read before forwarding has a budget: past it
// the request is not checked, and it is not held up.
func TestPrepareOtherBudget(t *testing.T) {
	c := New(Options{Store: slowStore{delay: 50 * time.Millisecond}, Auth: okAuth{}, PrepareBudget: 20 * time.Millisecond, Log: zerolog.Nop()})
	slow := testutil.ToFloat64(checks.WithLabelValues("send", "request", "skip_slow"))
	r, req := request()
	start := time.Now()
	if p := c.PrepareOther(context.Background(), r, req); p != nil {
		t.Fatalf("a slow read was kept: %+v", p)
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Errorf("held the request %s", d)
	}
	if testutil.ToFloat64(checks.WithLabelValues("send", "request", "skip_slow")) != slow+1 {
		t.Error("not counted")
	}

	c = New(Options{Store: slowStore{}, Auth: okAuth{}, Log: zerolog.Nop()})
	r, req = request()
	p := c.PrepareOther(context.Background(), r, req)
	if p == nil || !p.private || p.err != nil {
		t.Fatalf("%+v", p)
	}
}

// At most PrepareConcurrency reads run; the rest are not checked.
func TestPrepareOtherBusy(t *testing.T) {
	c := New(Options{Store: slowStore{delay: 100 * time.Millisecond}, Auth: okAuth{}, PrepareConcurrency: 1,
		PrepareBudget: time.Second, Log: zerolog.Nop()})
	done := make(chan struct{})
	go func() {
		r, req := request()
		c.PrepareOther(context.Background(), r, req)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	r, req := request()
	if p := c.PrepareOther(context.Background(), r, req); p != nil {
		t.Error("a second read ran beyond the limit")
	}
	<-done
}
