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

// qTokenInfo is _query_for_auth (storage/databases/main/registration.py),
// plus the user flags the requester and the native path need.
const qTokenInfo = `SELECT t.id, u.name, t.user_id, COALESCE(t.device_id, ''), t.puppets_user_id IS NOT NULL,
		t.valid_until_ms, COALESCE(t.used, false),
		COALESCE(u.shadow_banned, false), COALESCE(u.suspended, false), u.is_guest <> 0,
		u.deactivated <> 0, COALESCE(u.locked, false), u.appservice_id IS NOT NULL, COALESCE(u.admin, 0) <> 0
	FROM access_tokens t INNER JOIN users u ON u.name = COALESCE(t.puppets_user_id, t.user_id)
	WHERE t.token = $1`

// TokenInfo is what get_user_by_access_token knows about a token: the
// token's ID, which the requester carries, its validity, and the user's
// flags.
type TokenInfo struct {
	ID int64
	// UserID is the user the token acts as; Owner is the token's own user,
	// the authenticated entity. They differ for a puppet token.
	UserID   string
	Owner    string
	DeviceID string
	Puppet   bool
	// ValidUntilMS is valid_until_ms: nil for a token that never expires.
	ValidUntilMS *int64
	// Used is access_tokens.used: Synapse marks a token used on its first
	// request (mark_access_token_as_used).
	Used        bool
	ShadowBan   bool
	Suspended   bool
	Guest       bool
	Deactivated bool
	Locked      bool
	AppService  bool
	// Admin is users.admin: is_server_admin.
	Admin bool
}

// Token is get_user_by_access_token's database lookup. ErrNotFound for a
// token not in access_tokens (an appservice's, a guest macaroon, or none).
func (s *Store) Token(ctx context.Context, token string) (*TokenInfo, error) {
	var t TokenInfo
	err := s.pool.QueryRow(ctx, qTokenInfo, token).Scan(&t.ID, &t.UserID, &t.Owner, &t.DeviceID, &t.Puppet,
		&t.ValidUntilMS, &t.Used, &t.ShadowBan, &t.Suspended, &t.Guest, &t.Deactivated, &t.Locked, &t.AppService, &t.Admin)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("pdustore: token: %w", err)
	}
	return &t, nil
}

const qPartialStateRoom = `SELECT EXISTS (SELECT 1 FROM partial_state_rooms WHERE room_id = $1)`

// PartialState reports whether the room has partial state.
func (s *Store) PartialState(ctx context.Context, roomID string) (bool, error) {
	var b bool
	if err := s.pool.QueryRow(ctx, qPartialStateRoom, roomID).Scan(&b); err != nil {
		return false, fmt.Errorf("pdustore: partial state: %w", err)
	}
	return b, nil
}

const qEventRoom = `SELECT e.room_id FROM events AS e
	WHERE e.event_id = $1 AND NOT EXISTS (SELECT 1 FROM rejections AS r WHERE r.event_id = e.event_id)`

// EventRoom is the room of an event get_event would return: held, and not
// rejected. ErrNotFound otherwise.
func (s *Store) EventRoom(ctx context.Context, eventID string) (string, error) {
	var room string
	err := s.pool.QueryRow(ctx, qEventRoom, eventID).Scan(&room)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("pdustore: event room: %w", err)
	}
	return room, nil
}

const (
	// qIsRelationTarget is event_is_target_of_relation.
	qIsRelationTarget = `SELECT EXISTS (SELECT 1 FROM event_relations WHERE relates_to_id = $1)`
	// qHasRelation is event_includes_relation.
	qHasRelation = `SELECT EXISTS (SELECT 1 FROM event_relations WHERE event_id = $1)`
	// qHasAnnotated is has_user_annotated_event, verbatim.
	qHasAnnotated = `SELECT EXISTS (SELECT 1 FROM event_relations
		INNER JOIN events USING (event_id)
		WHERE relates_to_id = $1 AND relation_type = 'm.annotation' AND type = $2
		  AND sender = $3 AND aggregation_key = $4)`
)

// IsRelationTarget is event_is_target_of_relation.
func (s *Store) IsRelationTarget(ctx context.Context, eventID string) (bool, error) {
	return s.exists(ctx, "relation target", qIsRelationTarget, eventID)
}

// HasRelation is event_includes_relation.
func (s *Store) HasRelation(ctx context.Context, eventID string) (bool, error) {
	return s.exists(ctx, "has relation", qHasRelation, eventID)
}

// HasAnnotated is has_user_annotated_event.
func (s *Store) HasAnnotated(ctx context.Context, parent, eventType, sender, key string) (bool, error) {
	return s.exists(ctx, "annotated", qHasAnnotated, parent, eventType, sender, key)
}

func (s *Store) exists(ctx context.Context, what, q string, args ...any) (bool, error) {
	var b bool
	if err := s.pool.QueryRow(ctx, q, args...).Scan(&b); err != nil {
		return false, fmt.Errorf("pdustore: %s: %w", what, err)
	}
	return b, nil
}

const qRejectionColumn = `SELECT rejection_reason FROM events WHERE event_id = $1`

// RejectionColumn is events.rejection_reason, which the events writer fills
// from the context it was sent; nil for NULL.
func (s *Store) RejectionColumn(ctx context.Context, eventID string) (*string, error) {
	var v *string
	if err := s.pool.QueryRow(ctx, qRejectionColumn, eventID).Scan(&v); err != nil {
		return nil, fmt.Errorf("pdustore: rejection column: %w", err)
	}
	return v, nil
}
