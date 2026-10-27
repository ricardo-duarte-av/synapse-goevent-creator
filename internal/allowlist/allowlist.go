// Package allowlist is the one gate between a request and a write.
//
// This worker creates events, and an event cannot be taken back: it is
// persisted, streamed and federated within a second. It writes only to the
// rooms the configuration names (operator, 2026-10-27: any room, added and
// removed at will), and those can be changed while it runs (Set, on SIGHUP).
// Every write path must ask Check, and only Check.
package allowlist

import (
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
)

// TestRooms are the rooms the end-to-end suite may run in: it bans, kicks
// and sends refusals on purpose, so never in a real room. They are not
// otherwise special; the worker writes to them only when configured.
var TestRooms = []string{
	"!decSfHzLLhrNKJaZid:aguiarvieira.pt", // TEST6TEST, v11
	"!nmZrvcKkeQiFGRlJtE:aguiarvieira.pt", // TEST7TESTE, v11
	// The policy server room (MSC4284), its policy server the e2e fake
	// (cmd/goevent-policy, policy.aguiarvieira.pt), v11. Operator, 2026-10-20.
	"!FRudVQysKkIHAjrSzd:aguiarvieira.pt",
	// The v12 test room (MSC4291 room ID: the create event's hash).
	// Operator, 2026-10-20.
	"!goBCqT-LJRiRgPetKo34PrUe4GqYZ_R74c6SBGLwTo4",
}

// List is the configured rooms.
type List struct {
	rooms atomic.Pointer[map[string]bool]
}

// New builds a List from configured room IDs.
func New(rooms []string) (*List, error) {
	l := &List{}
	if err := l.Set(rooms); err != nil {
		return nil, err
	}
	return l, nil
}

// Set replaces the rooms. On an error the list is unchanged.
func (l *List) Set(rooms []string) error {
	m := make(map[string]bool, len(rooms))
	for _, r := range rooms {
		if err := valid(r); err != nil {
			return fmt.Errorf("rooms: %q: %w", r, err)
		}
		m[r] = true
	}
	l.rooms.Store(&m)
	return nil
}

// valid accepts a room ID: "!" then an opaque part, with ":server" for
// room versions before 12 (MSC4291 IDs have none). Never an alias.
func valid(r string) error {
	switch {
	case len(r) < 2 || r[0] != '!':
		return fmt.Errorf("not a room ID (an alias is never accepted)")
	case len(r) > 255:
		return fmt.Errorf("longer than 255 bytes")
	case strings.ContainsAny(r, " \t\r\n"):
		return fmt.Errorf("contains whitespace")
	case strings.HasSuffix(r, ":"):
		return fmt.Errorf("empty server name")
	}
	return nil
}

// Check reports whether this worker may write to roomID. roomID must be a
// resolved room ID; an alias is never allowed.
func (l *List) Check(roomID string) bool {
	if l == nil {
		return false
	}
	m := l.rooms.Load()
	return m != nil && (*m)[roomID]
}

// Rooms returns the configured rooms, for logging.
func (l *List) Rooms() []string {
	m := *l.rooms.Load()
	out := make([]string, 0, len(m))
	for r := range m {
		out = append(out, r)
	}
	slices.Sort(out)
	return out
}
