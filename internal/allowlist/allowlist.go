// Package allowlist is the one gate between a request and a write.
//
// This worker creates events, and an event cannot be taken back: it is
// persisted, streamed and federated within a second. While the worker is being
// proven, it may write only to two test rooms. That rule is compiled in
// (Permitted), and the configuration can narrow it but never widen it, so a
// typo in a YAML file cannot reach a real room. Every write path must ask
// Check, and only Check.
package allowlist

import (
	"fmt"
	"slices"
)

// Permitted are the only rooms any configuration may name.
var Permitted = []string{
	"!decSfHzLLhrNKJaZid:aguiarvieira.pt", // TEST6TEST, v11
	"!nmZrvcKkeQiFGRlJtE:aguiarvieira.pt", // TEST7TESTE, v11
	// The policy server room (MSC4284), its policy server the e2e fake
	// (cmd/goevent-policy, policy.aguiarvieira.pt), v11. Operator, 2026-10-20.
	"!FRudVQysKkIHAjrSzd:aguiarvieira.pt",
	// The v12 test room (MSC4291 room ID: the create event's hash).
	// Operator, 2026-10-20.
	"!goBCqT-LJRiRgPetKo34PrUe4GqYZ_R74c6SBGLwTo4",
}

// List is the configured subset of Permitted.
type List struct {
	rooms map[string]bool
}

// New builds a List from configured room IDs. A room outside Permitted is an
// error, not a warning.
func New(rooms []string) (*List, error) {
	l := &List{rooms: make(map[string]bool, len(rooms))}
	for _, r := range rooms {
		if !slices.Contains(Permitted, r) {
			return nil, fmt.Errorf("rooms: %q is not one of the rooms this build may write to (%v)", r, Permitted)
		}
		l.rooms[r] = true
	}
	return l, nil
}

// Check reports whether this worker may write to roomID. roomID must be a
// resolved room ID; an alias is never allowed.
func (l *List) Check(roomID string) bool {
	return l != nil && l.rooms[roomID]
}

// Rooms returns the configured rooms, for logging.
func (l *List) Rooms() []string {
	out := make([]string, 0, len(l.rooms))
	for r := range l.rooms {
		out = append(out, r)
	}
	slices.Sort(out)
	return out
}
