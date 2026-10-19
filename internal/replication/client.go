// Package replication speaks Synapse's HTTP replication protocol, as a client.
//
// One endpoint is used: send_events, which is what an event creator calls to
// hand the events it built to the room's events writer
// (EventCreationHandler._persist_events, replication/http/send_events.py).
// The writer then persists them and runs persist_and_notify_client_events:
// remote invites, notifications, the stream.
//
// The wire format is replication/http/_base.py's: POST
// /_synapse/replication/<NAME>/<path args...>/<txn_id>, a JSON body, and the
// shared secret as a Bearer token. The txn_id is what makes a retry safe: the
// writer keeps a 30-minute ResponseCache keyed on it, so the same txn_id is
// processed once however many times it arrives.
//
// Copied from goinbound, which uses the same client for fed_send_edu and
// fed_send_events.
package replication

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/synapsecfg"
)

// Client sends replication requests to named Synapse instances.
type Client struct {
	targets map[string]*target
	secret  string
	// sleep is replaced in tests.
	sleep func(context.Context, time.Duration) error
}

type target struct {
	name string
	base string
	http *http.Client
}

// RequestTimeout bounds one replication request. Synapse's replication client
// uses 60s too.
const RequestTimeout = 60 * time.Second

// NewClient builds a client for every instance in the instance map.
func NewClient(cfg *synapsecfg.Config) *Client {
	c := &Client{targets: make(map[string]*target), secret: cfg.ReplicationSecret, sleep: sleepCtx}
	for name, inst := range cfg.InstanceMap {
		c.targets[name] = newTarget(inst)
	}
	return c
}

func newTarget(inst synapsecfg.Instance) *target {
	t := &target{name: inst.Name}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	if inst.Socket != "" {
		sock := inst.Socket
		var d net.Dialer
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return d.DialContext(ctx, "unix", sock)
		}
		// Synapse ignores the host of a replication request.
		t.base = "http://" + inst.Name
	} else {
		t.base = strings.TrimSuffix(inst.URL, "/")
	}
	t.http = &http.Client{Transport: transport, Timeout: RequestTimeout}
	return t
}

// Has reports whether an instance can be reached.
func (c *Client) Has(instance string) bool {
	_, ok := c.targets[canonical(instance)]
	return ok
}

func canonical(name string) string {
	if name == "main" {
		return synapsecfg.MainProcessInstanceName
	}
	return name
}

// HTTPError is a non-2xx answer from the instance.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("replication: HTTP %d: %s", e.Status, e.Body)
}

// Connection errors are retried this many times, with delays doubling from
// one second: replication/http/_base.py's RETRY_ON_CONNECT_ERROR_ATTEMPTS.
const connectRetries = 5

// call posts body to url with _base.py's retries: a timeout is retried after
// a second, as often as the context allows, and a connection failure up to
// connectRetries times with exponential backoff. Both are safe because every
// attempt carries the same txn_id. Any other failure is returned at once.
func (c *Client) call(ctx context.Context, t *target, name, url string, body []byte) ([]byte, error) {
	connectFailures := 0
	for {
		b, err := c.post(ctx, t, url, body)
		switch {
		case err == nil:
			return b, nil
		case ctx.Err() != nil:
			return nil, fmt.Errorf("replication: %s to %s: %w", name, t.name, ctx.Err())
		case isTimeout(err):
			retries.WithLabelValues(t.name, "timeout").Inc()
			_ = c.sleep(ctx, time.Second)
		case isConnectError(err) && connectFailures < connectRetries:
			retries.WithLabelValues(t.name, "connect").Inc()
			delay := time.Second << connectFailures
			connectFailures++
			_ = c.sleep(ctx, delay)
		default:
			return nil, err
		}
	}
}

func (c *Client) post(ctx context.Context, t *target, url string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.secret != "" {
		req.Header.Set("Authorization", "Bearer "+c.secret)
	}
	resp, err := t.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode/100 != 2 {
		return nil, &HTTPError{Status: resp.StatusCode, Body: string(b)}
	}
	return b, nil
}

// ErrPartialStateConflict is the persister's 409: the room was un-partial
// stated while the event's context was computed, and Synapse recomputes it.
var ErrPartialStateConflict = errors.New("replication: partial state conflict")

// SendClientEvents persists events built here through instance, as
// ReplicationSendEventsRestServlet expects them: {"events": [...]}, each
// entry already serialized (_serialize_payload). It returns the stream ID and
// event ID the writer answers with; an event ID other than the last one sent
// means the writer deduplicated it, by transaction.
//
// The stream positions Synapse's own client includes (_INT_STREAM_POS) are
// omitted: they tell the writer to wait for the sender's writes, and the only
// streams this process could have written to are none.
//
// A non-2xx answer is returned as *HTTPError with the writer's status and
// body, which Synapse relays to the client as they are (to_synapse_error):
// a ratelimit, a forbidden remote invite.
func (c *Client) SendClientEvents(ctx context.Context, instance string, entries []json.RawMessage) (streamID int64, eventID string, err error) {
	t, ok := c.targets[canonical(instance)]
	if !ok {
		return 0, "", fmt.Errorf("replication: instance %q is not in instance_map", instance)
	}
	body, err := json.Marshal(struct {
		Events []json.RawMessage `json:"events"`
	}{entries})
	if err != nil {
		return 0, "", fmt.Errorf("replication: encode events: %w", err)
	}
	url := t.base + "/_synapse/replication/send_events/" + txnID()

	start := time.Now()
	b, err := c.call(ctx, t, "send_events", url, body)
	if err != nil {
		var he *HTTPError
		result := "error"
		switch {
		case ctx.Err() != nil:
			result = "timeout"
		case errors.As(err, &he) && he.Status == http.StatusConflict:
			observeEvents(t.name, "conflict", start)
			return 0, "", ErrPartialStateConflict
		case errors.As(err, &he):
			result = "status_" + fmt.Sprint(he.Status)
		}
		observeEvents(t.name, result, start)
		return 0, "", err
	}
	observeEvents(t.name, "ok", start)
	var resp struct {
		StreamID int64  `json:"stream_id"`
		EventID  string `json:"event_id"`
	}
	if err := json.Unmarshal(b, &resp); err != nil {
		return 0, "", fmt.Errorf("replication: send_events answer: %w", err)
	}
	return resp.StreamID, resp.EventID, nil
}

// isTimeout matches Synapse's RequestTimedOutError: no answer in time, or a
// 504 from the instance.
func isTimeout(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		return he.Status == http.StatusGatewayTimeout
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// isConnectError matches Synapse's ConnectError: nothing listening, or a socket
// that is not there -- the shape of an instance mid-restart.
func isConnectError(err error) bool {
	var oe *net.OpError
	return errors.As(err, &oe) && oe.Op == "dial"
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// quote is Python's urllib.parse.quote(s, safe=""), which _base.py applies to
// every path argument: everything but unreserved characters is escaped.
func quote(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ('A' <= ch && ch <= 'Z') || ('a' <= ch && ch <= 'z') || ('0' <= ch && ch <= '9') ||
			ch == '_' || ch == '.' || ch == '-' || ch == '~' {
			b.WriteByte(ch)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", ch)
	}
	return b.String()
}

const txnIDLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// txnID is Synapse's random_string(10).
func txnID() string {
	var raw [10]byte
	_, _ = rand.Read(raw[:])
	out := make([]byte, len(raw))
	for i, r := range raw {
		out[i] = txnIDLetters[int(r)%len(txnIDLetters)]
	}
	return string(out)
}

func observeEvents(instance, result string, start time.Time) {
	sendEventsDuration.WithLabelValues(instance, result).Observe(time.Since(start).Seconds())
}

var sendEventsDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "goevent_replication_send_events_duration_seconds",
	Help:    "Time to persist events through send_events, including retries, by outcome.",
	Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
}, []string{"writer", "result"})

var (
	retries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_replication_retries_total",
		Help: "Replication requests retried, by instance and reason.",
	}, []string{"writer", "reason"})
)

// UserLeftRoom is ReplicationUserJoinedLeftRoomRestServlet, which a worker's
// _user_left_room calls on main once a local user's leave from a room they
// were joined to is persisted: main's distributor fires user_left_room (the
// typing handler drops their typing notification). CACHE is False for it, so
// the path carries no txn_id.
func (c *Client) UserLeftRoom(ctx context.Context, roomID, userID string) error {
	t, ok := c.targets[synapsecfg.MainProcessInstanceName]
	if !ok {
		return fmt.Errorf("replication: main is not in instance_map")
	}
	url := t.base + "/_synapse/replication/membership_change/" + quote(roomID) + "/" + quote(userID) + "/left"
	_, err := c.call(ctx, t, "membership_change", url, []byte("{}"))
	return err
}

// DelayedEventAdded is ReplicationAddedDelayedEventRestServlet: main, which
// sends delayed events, is told the send time of the next one due.
func (c *Client) DelayedEventAdded(ctx context.Context, nextSendTS int64) error {
	t, ok := c.targets[synapsecfg.MainProcessInstanceName]
	if !ok {
		return fmt.Errorf("replication: main is not in instance_map")
	}
	body, _ := json.Marshal(map[string]int64{"next_send_ts": nextSendTS})
	_, err := c.call(ctx, t, "added_delayed_event", t.base+"/_synapse/replication/added_delayed_event/", body)
	return err
}
