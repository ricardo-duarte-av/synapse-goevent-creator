package pdustore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Queries for the event creator: what Synapse reads when it builds a local
// event, and what the shadow check reads to find the event Synapse built.

// qPrevEventsForRoom is _get_prev_events_for_room_txn
// (storage/databases/main/event_federation.py), verbatim: the ten deepest
// forward extremities. Ties in depth come back in no particular order.
const qPrevEventsForRoom = `SELECT e.event_id FROM event_forward_extremities AS f
	INNER JOIN events AS e USING (event_id)
	WHERE f.room_id = $1
	ORDER BY e.depth DESC
	LIMIT 10`

// PrevEventsForRoom is get_prev_events_for_room.
func (s *Store) PrevEventsForRoom(ctx context.Context, roomID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, qPrevEventsForRoom, roomID)
	if err != nil {
		return nil, fmt.Errorf("pdustore: prev events for room: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("pdustore: prev events for room: %w", err)
	}
	return ids, nil
}

const qMaxDepthOf = `SELECT event_id, depth FROM events WHERE event_id = ANY($1)`

// Depths returns each known event's depth.
func (s *Store) Depths(ctx context.Context, ids []string) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, qMaxDepthOf, ids)
	if err != nil {
		return nil, fmt.Errorf("pdustore: depths: %w", err)
	}
	out := make(map[string]int64, len(ids))
	var id string
	var depth int64
	_, err = pgx.ForEachRow(rows, []any{&id, &depth}, func() error {
		out[id] = depth
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("pdustore: depths: %w", err)
	}
	return out, nil
}

// RoomMaxStream is the newest stream ordering in a room, 0 for none.
func (s *Store) RoomMaxStream(ctx context.Context, roomID string) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, qRoomMaxStream, roomID).Scan(&n); err != nil {
		return 0, fmt.Errorf("pdustore: room max stream: %w", err)
	}
	return n, nil
}

const qNewMembership = `SELECT m.event_id FROM room_memberships AS m
	INNER JOIN events AS e USING (event_id)
	WHERE m.room_id = $1 AND e.stream_ordering > $2 AND m.membership = $3
	  AND ($4 = '' OR m.user_id = $4) AND m.sender LIKE '%:' || $5
	ORDER BY e.stream_ordering DESC
	LIMIT 1`

// ErrNotFound is a lookup that found nothing.
var ErrNotFound = errors.New("pdustore: not found")

// NewMembership finds the newest membership event in a room, newer than
// stream ordering after, with the given membership, for target ("" for
// any), sent by a user of server. The membership endpoints answer {} and
// /join answers only the room ID, so this is how their event is found.
func (s *Store) NewMembership(ctx context.Context, roomID string, after int64, membership, target, server string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, qNewMembership, roomID, after, membership, target, server).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("pdustore: new membership: %w", err)
	}
	return id, nil
}

// qTxnEvent is get_event_id_from_transaction_id_and_device_id
// (storage/databases/main/events_worker.py).
const qTxnEvent = `SELECT event_id FROM event_txn_id_device_id
	WHERE room_id = $1 AND user_id = $2 AND device_id = $3 AND txn_id = $4`

// TxnEvent is the event a client transaction already created, ErrNotFound
// for none.
func (s *Store) TxnEvent(ctx context.Context, roomID, userID, deviceID, txnID string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, qTxnEvent, roomID, userID, deviceID, txnID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("pdustore: txn event: %w", err)
	}
	return id, nil
}

const qReadOnly = `SELECT current_setting('default_transaction_read_only')`

// ReadOnly reports whether the session refuses writes. Until write mode the
// worker connects read-only, and refuses to start otherwise.
func (s *Store) ReadOnly(ctx context.Context) (bool, error) {
	var v string
	if err := s.pool.QueryRow(ctx, qReadOnly).Scan(&v); err != nil {
		return false, fmt.Errorf("pdustore: read-only setting: %w", err)
	}
	return v == "on", nil
}
