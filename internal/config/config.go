// Package config loads the goevent-creator configuration.
//
// Only what is ours lives here: where to listen, which Synapse process to
// fall back to (main), and how each endpoint is served. Everything Synapse
// already knows -- the events writer, the instance map, the replication
// secret, the database, the signing key -- is read from homeserver.yaml by
// package synapsecfg and is deliberately not repeated.
package config

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

// Mode is how one endpoint is served.
type Mode string

const (
	// ModeProxy forwards every request to the upstream untouched.
	ModeProxy Mode = "proxy"
)

// accepted is every mode an endpoint may be given today. Shadow and native
// are added per endpoint as each one exists: a mode that is accepted before
// its code path exists would silently mean "proxy".
var accepted = map[route.Endpoint][]Mode{
	route.Send:       {ModeProxy},
	route.State:      {ModeProxy},
	route.Redact:     {ModeProxy},
	route.Membership: {ModeProxy},
	route.Join:       {ModeProxy},
	route.Knock:      {ModeProxy},
	route.Profile:    {ModeProxy},
}

// Config is the top-level worker configuration.
type Config struct {
	// HomeserverYAML is Synapse's homeserver.yaml, mounted read-only.
	HomeserverYAML string `yaml:"homeserver_yaml"`

	Listen   Listen   `yaml:"listen"`
	Upstream Upstream `yaml:"upstream"`
	Metrics  Metrics  `yaml:"metrics"`
	Log      Log      `yaml:"log"`

	// Modes is the serving mode of every endpoint. Each must be given:
	// nothing is proxied "because that is the default".
	Modes map[route.Endpoint]Mode `yaml:"modes"`

	// Rooms are the rooms this worker may write to. Each must also be in
	// allowlist.Permitted, which is compiled in.
	Rooms []string `yaml:"rooms"`
}

// Listen is where nginx reaches us.
type Listen struct {
	// Socket is a unix socket path. Exactly one of Socket and Addr is set.
	Socket string `yaml:"socket"`
	// Addr is a TCP host:port.
	Addr string `yaml:"addr"`
	// SocketMode is the socket's permission bits, in octal. Default "0660".
	SocketMode string `yaml:"socket_mode"`
}

// ParsedSocketMode returns SocketMode as file permissions.
func (l Listen) ParsedSocketMode() (os.FileMode, error) {
	if l.SocketMode == "" {
		return 0o660, nil
	}
	n, err := strconv.ParseUint(l.SocketMode, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("listen.socket_mode %q: %w", l.SocketMode, err)
	}
	return os.FileMode(n), nil
}

// Upstream is where everything not served here goes: Synapse's main process,
// since av-client-events-1 is to be retired.
type Upstream struct {
	Sockets []string `yaml:"sockets"`
	Addrs   []string `yaml:"addrs"`
	// TimeoutSeconds bounds waiting for response headers. A remote join can
	// take minutes. Zero uses 300.
	TimeoutSeconds int `yaml:"timeout_seconds"`
	// MaxIdleConns per upstream. Zero uses 32.
	MaxIdleConns int `yaml:"max_idle_conns"`
}

// Timeout is the upstream response header timeout.
func (u Upstream) Timeout() time.Duration {
	if u.TimeoutSeconds == 0 {
		return 300 * time.Second
	}
	return time.Duration(u.TimeoutSeconds) * time.Second
}

// Metrics is the Prometheus listener.
type Metrics struct {
	Addr string `yaml:"addr"`
}

// Log configures logging.
type Log struct {
	Level  string `yaml:"level"`
	Pretty bool   `yaml:"pretty"`
}

// Load reads and validates the configuration file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return parse(b)
}

func parse(data []byte) (*Config, error) {
	cfg := &Config{
		HomeserverYAML: "/etc/synapse/homeserver.yaml",
		Metrics:        Metrics{Addr: ":9102"},
		Log:            Log{Level: "info"},
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if (c.Listen.Socket == "") == (c.Listen.Addr == "") {
		return fmt.Errorf("listen: exactly one of socket and addr must be set")
	}
	if _, err := c.Listen.ParsedSocketMode(); err != nil {
		return err
	}
	if len(c.Upstream.Sockets)+len(c.Upstream.Addrs) == 0 {
		return fmt.Errorf("upstream: at least one of sockets and addrs must be set")
	}
	if c.HomeserverYAML == "" {
		return fmt.Errorf("homeserver_yaml must be set")
	}

	for ep := range c.Modes {
		if _, ok := accepted[ep]; !ok {
			return fmt.Errorf("modes: unknown endpoint %q", ep)
		}
	}
	for _, ep := range route.Endpoints {
		m, ok := c.Modes[ep]
		if !ok {
			return fmt.Errorf("modes.%s must be set (one of %v)", ep, accepted[ep])
		}
		if !slices.Contains(accepted[ep], m) {
			return fmt.Errorf("modes.%s: %q is not available for this endpoint yet (one of %v)", ep, m, accepted[ep])
		}
	}
	return nil
}

// ModeFor returns how an endpoint is served. Requests that match no endpoint
// are always proxied.
func (c *Config) ModeFor(ep route.Endpoint) Mode {
	if m, ok := c.Modes[ep]; ok {
		return m
	}
	return ModeProxy
}
