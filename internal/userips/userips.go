// Package userips is what get_user_by_req's _record_request does on a
// worker (api/auth/base.py, storage/databases/main/client_ips.py,
// replication/tcp, v1.163.0): a client's request is told to the worker that
// writes client IPs, at most once per user, token and address every
// LAST_SEEN_GRANULARITY, as a USER_IP command on the replication bus.
// Synapse's main process then updates user_ips, devices' last seen and MAU.
package userips

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/synapsecfg"
)

var sent = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goevent_user_ip_total",
	Help: "USER_IP commands, by outcome: sent, error, or skipped as seen within two minutes.",
}, []string{"outcome"})

// granularity is LAST_SEEN_GRANULARITY.
const granularity = 120 * time.Second

// cacheSize is client_ip_last_seen's max_size.
const cacheSize = 50000

// Publisher sends one command to a channel.
type Publisher interface {
	Publish(ctx context.Context, channel, line string) error
}

// Recorder is insert_client_ip on a worker that does not write client IPs.
type Recorder struct {
	pub     Publisher
	channel string
	log     zerolog.Logger
	now     func() time.Time

	mu   sync.Mutex
	seen map[string]time.Time
}

// New records to pub on Synapse's USER_IP channel for serverName.
func New(pub Publisher, serverName string, log zerolog.Logger, now func() time.Time) *Recorder {
	if now == nil {
		now = time.Now
	}
	return &Recorder{pub: pub, channel: serverName + "/USER_IP", log: log, now: now, seen: map[string]time.Time{}}
}

// Record is insert_client_ip. deviceID nil is None. It does not wait for
// the bus.
func (r *Recorder) Record(userID, token, ip, userAgent string, deviceID *string) {
	if userAgent == "sync-v3-proxy-" {
		return
	}
	now := r.now()
	key := userID + "\x00" + token + "\x00" + ip
	r.mu.Lock()
	if last, ok := r.seen[key]; ok && now.Sub(last) < granularity {
		r.mu.Unlock()
		sent.WithLabelValues("skipped").Inc()
		return
	}
	if len(r.seen) >= cacheSize {
		for k, at := range r.seen {
			if now.Sub(at) >= granularity {
				delete(r.seen, k)
			}
		}
		if len(r.seen) >= cacheSize {
			r.seen = map[string]time.Time{}
		}
	}
	r.seen[key] = now
	r.mu.Unlock()

	line := "USER_IP " + Line(userID, token, ip, userAgent, deviceID, now.UnixMilli())
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.pub.Publish(ctx, r.channel, line); err != nil {
			sent.WithLabelValues("error").Inc()
			r.log.Warn().Err(err).Msg("Could not send USER_IP")
			return
		}
		sent.WithLabelValues("sent").Inc()
	}()
}

// Line is UserIpCommand.to_line: the user, then json_encoder's encoding of
// (token, ip, user_agent, device_id, last_seen), ASCII only as Python
// writes it.
func Line(userID, token, ip, userAgent string, deviceID *string, lastSeen int64) string {
	var device any
	if deviceID != nil {
		device = *deviceID
	}
	b, _ := json.Marshal([]any{token, ip, userAgent, device, lastSeen})
	return userID + " " + asciiJSON(string(b))
}

// asciiJSON escapes what json.dumps(ensure_ascii=True) escapes beyond Go:
// every non-ASCII character, as \uXXXX (surrogate pairs past the BMP).
func asciiJSON(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r < 0x80:
			b.WriteRune(r)
		case r < 0x10000:
			b.WriteString(`\u` + hex4(int(r)))
		default:
			r -= 0x10000
			b.WriteString(`\u` + hex4(0xd800+int(r>>10)) + `\u` + hex4(0xdc00+int(r&0x3ff)))
		}
	}
	return b.String()
}

func hex4(n int) string {
	const digits = "0123456789abcdef"
	return string([]byte{digits[n>>12&15], digits[n>>8&15], digits[n>>4&15], digits[n&15]})
}

// Bus is the replication bus (replication/tcp/redis.py).
type Bus struct{ client *redis.Client }

// NewBus connects as homeserver.yaml's redis section says.
func NewBus(cfg synapsecfg.Redis) *Bus {
	opts := &redis.Options{Addr: cfg.Addr, Username: cfg.Username, Password: cfg.Password, DB: cfg.DBID}
	if cfg.Socket != "" {
		opts.Network, opts.Addr = "unix", cfg.Socket
	}
	return &Bus{client: redis.NewClient(opts)}
}

// Publish sends one command line, as _async_send_command does.
func (b *Bus) Publish(ctx context.Context, channel, line string) error {
	return b.client.Publish(ctx, channel, line).Err()
}

// Close closes the connection.
func (b *Bus) Close() error { return b.client.Close() }
