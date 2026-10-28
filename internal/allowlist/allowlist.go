// Package allowlist is the one gate between a request and a write.
//
// This worker creates events, and an event cannot be taken back: it is
// persisted, streamed and federated within a second. It writes only to the
// rooms the configuration names, or to every room but the excluded ones
// (operator, 2026-10-27: any room, added and removed at will; 2026-10-28:
// all rooms), and those can be changed while it runs (Apply, on SIGHUP).
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

// Spec is what the configuration says: every room (All) or the listed
// Rooms, never the Excluded ones.
type Spec struct {
	All      bool
	Rooms    []string
	Excluded []string
}

// List is the configured rooms.
type List struct {
	cur atomic.Pointer[state]
}

type state struct {
	all      bool
	rooms    map[string]bool
	excluded map[string]bool
}

// New builds a List of the given rooms.
func New(rooms []string) (*List, error) {
	return NewSpec(Spec{Rooms: rooms})
}

// NewSpec builds a List from a Spec.
func NewSpec(s Spec) (*List, error) {
	l := &List{}
	if err := l.Apply(s); err != nil {
		return nil, err
	}
	return l, nil
}

// Set replaces the rooms with the given ones. On an error the list is
// unchanged.
func (l *List) Set(rooms []string) error {
	return l.Apply(Spec{Rooms: rooms})
}

// Apply replaces the rooms. On an error the list is unchanged.
func (l *List) Apply(s Spec) error {
	if s.All && len(s.Rooms) > 0 {
		return fmt.Errorf("rooms: all takes no list of rooms")
	}
	st := &state{all: s.All, rooms: map[string]bool{}, excluded: map[string]bool{}}
	for _, r := range s.Rooms {
		if err := valid(r); err != nil {
			return fmt.Errorf("rooms: %q: %w", r, err)
		}
		st.rooms[r] = true
	}
	for _, r := range s.Excluded {
		if err := valid(r); err != nil {
			return fmt.Errorf("exclude_rooms: %q: %w", r, err)
		}
		st.excluded[r] = true
	}
	l.cur.Store(st)
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
// resolved room ID; an alias is never allowed, even with all rooms.
func (l *List) Check(roomID string) bool {
	if l == nil {
		return false
	}
	st := l.cur.Load()
	switch {
	case st == nil || valid(roomID) != nil || st.excluded[roomID]:
		return false
	case st.all:
		return true
	}
	return st.rooms[roomID]
}

// Rooms returns the configured rooms, or just "all", for logging.
func (l *List) Rooms() []string {
	st := l.cur.Load()
	if st.all {
		return []string{"all"}
	}
	return sorted(st.rooms)
}

// Excluded returns the excluded rooms, for logging.
func (l *List) Excluded() []string {
	return sorted(l.cur.Load().excluded)
}

func sorted(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for r := range m {
		out = append(out, r)
	}
	slices.Sort(out)
	return out
}
