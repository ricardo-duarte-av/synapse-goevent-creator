package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/allowlist"
)

// SIGHUP rereads the rooms; a config that does not load changes nothing.
func TestReloadRooms(t *testing.T) {
	example, err := os.ReadFile("../../deploy/goevent-creator.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "c.yaml")
	write := func(rooms string) {
		t.Helper()
		// The example's rooms list, replaced.
		var out []string
		skip := false
		for _, line := range strings.Split(string(example), "\n") {
			switch {
			case line == "rooms:":
				out = append(out, "rooms:"+rooms)
				skip = true
				continue
			case skip && strings.HasPrefix(line, "  - "):
				continue
			}
			skip = false
			out = append(out, line)
		}
		if err := os.WriteFile(path, []byte(strings.Join(out, "\n")), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	rooms, _ := allowlist.New([]string{"!a:example.org"})

	write(` ["!b:example.org", "!c:example.org"]`)
	reloadRooms(path, rooms, zerolog.Nop())
	if rooms.Check("!a:example.org") || !rooms.Check("!b:example.org") || !rooms.Check("!c:example.org") {
		t.Errorf("not reloaded: %v", rooms.Rooms())
	}

	write(` ["#alias:example.org"]`)
	reloadRooms(path, rooms, zerolog.Nop())
	if !rooms.Check("!b:example.org") {
		t.Errorf("a bad config changed the rooms: %v", rooms.Rooms())
	}
}
