package config

import (
	"strings"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

const valid = `
listen: {socket: /var/sockets/goevent.sock}
upstream: {sockets: [/var/sockets/nginx/av-client-events-1.sock]}
rooms: ["!decSfHzLLhrNKJaZid:aguiarvieira.pt"]
modes:
  send: proxy
  state: proxy
  redact: proxy
  membership: proxy
  join: proxy
  knock: proxy
  profile: proxy
`

func TestValid(t *testing.T) {
	cfg, err := parse([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HomeserverYAML != "/etc/synapse/homeserver.yaml" {
		t.Errorf("homeserver_yaml default = %q", cfg.HomeserverYAML)
	}
	if cfg.ModeFor(route.Send) != ModeProxy || cfg.ModeFor(route.Other) != ModeProxy {
		t.Error("unexpected modes")
	}
}

func TestEveryModeIsRequired(t *testing.T) {
	_, err := parse([]byte(strings.Replace(valid, "  knock: proxy\n", "", 1)))
	if err == nil || !strings.Contains(err.Error(), "modes.knock") {
		t.Fatalf("missing mode: err = %v", err)
	}
}

func TestModeNotYetAvailable(t *testing.T) {
	_, err := parse([]byte(strings.Replace(valid, "membership: proxy", "membership: native", 1)))
	if err == nil || !strings.Contains(err.Error(), "not available") {
		t.Fatalf("native membership: err = %v", err)
	}
	// Profile writes touch every room the user is in: never shadowed.
	_, err = parse([]byte(strings.Replace(valid, "profile: proxy", "profile: shadow", 1)))
	if err == nil || !strings.Contains(err.Error(), "not available") {
		t.Fatalf("shadow profile: err = %v", err)
	}
}

func TestShadow(t *testing.T) {
	cfg, err := parse([]byte(strings.Replace(valid, "send: proxy", "send: shadow", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AnyShadow() || cfg.ModeFor(route.Send) != ModeShadow {
		t.Error("send is not in shadow mode")
	}
}

func TestUnknownEndpoint(t *testing.T) {
	_, err := parse([]byte(valid + "  typing: proxy\n"))
	if err == nil || !strings.Contains(err.Error(), "unknown endpoint") {
		t.Fatalf("unknown endpoint: err = %v", err)
	}
}

func TestListenExactlyOne(t *testing.T) {
	_, err := parse([]byte(strings.Replace(valid, "{socket: /var/sockets/goevent.sock}", "{}", 1)))
	if err == nil {
		t.Fatal("no listener accepted")
	}
}

// TestEveryEndpointHasAModeList keeps config and route in step.
func TestEveryEndpointHasAModeList(t *testing.T) {
	for _, ep := range route.Endpoints {
		if len(accepted[ep]) == 0 {
			t.Errorf("endpoint %s has no accepted modes", ep)
		}
	}
}

func TestNative(t *testing.T) {
	cfg, err := parse([]byte(strings.Replace(valid, "send: proxy", "send: native", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AnyNative() || !cfg.AnyShadow() || cfg.ModeFor(route.Send) != ModeNative {
		t.Error("send is not native, or its fallbacks are not shadowed")
	}
}
