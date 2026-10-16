// Package writer is what the event creator writes to Synapse's database
// itself, written as Synapse writes it. Everything else an event needs is
// done by the events writer, over replication (send_events).
//
// The connection is a role granted exactly Grants and Sequences, created or
// repaired at startup (package dbrole). TestQueriesUseOnlyGrants keeps the
// queries and the grants in step, so a query that touches anything else
// fails a test before it can fail in production.
package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/dbrole"
)

// Role is the write role's name.
const Role = "goevent_creator"

// Grants is every table the creator touches through this connection, and how.
var Grants = []dbrole.Grant{
	// add_push_actions_to_staging, and removal when persisting fails.
	{Table: "event_push_actions_staging", Privileges: "SELECT, INSERT, DELETE"},
	// set_room_participation.
	{Table: "room_memberships", Privileges: "SELECT, UPDATE (participant)"},
	{Table: "local_current_membership", Privileges: "SELECT"},
	// Read by room_memberships' check_event_stream_ordering trigger, which
	// runs as the caller.
	{Table: "events", Privileges: "SELECT"},
	// check_state_groups_and_bump_deletion, which the creator calls with
	// lock=False: a plain SELECT, no FOR KEY SHARE.
	{Table: "state_groups", Privileges: "SELECT"},
	{Table: "state_groups_pending_deletion", Privileges: "SELECT, UPDATE"},
}

// Sequences are granted USAGE: the pending deletion bump's
// sequence_number = DEFAULT.
var Sequences = []string{"state_groups_pending_deletion_sequence_number_seq"}

// Spec is the role as dbrole ensures it.
func Spec(password string) dbrole.Spec {
	return dbrole.Spec{Role: Role, Password: password, Writes: Grants, Sequences: Sequences}
}

const (
	// qStagePush is add_push_actions_to_staging's simple_insert_many
	// (storage/databases/main/event_push_actions.py).
	qStagePush = `
		INSERT INTO event_push_actions_staging (event_id, user_id, actions, notif, highlight, unread, thread_id, inserted_ts)
		SELECT $1, k.user_id, k.actions, k.notif, k.highlight, k.unread, k.thread_id, $2
		FROM unnest($3::text[], $4::text[], $5::smallint[], $6::smallint[], $7::smallint[], $8::text[])
			AS k(user_id, actions, notif, highlight, unread, thread_id)`
	// qUnstagePush is remove_push_actions_from_staging.
	qUnstagePush = `DELETE FROM event_push_actions_staging WHERE event_id = $1`

	// qRoomParticipation is _set_room_participation_txn
	// (storage/databases/main/roommember.py), verbatim.
	qRoomParticipation = `
		UPDATE room_memberships
		SET participant = true
		WHERE event_id IN (
			SELECT event_id FROM local_current_membership
			WHERE user_id = $1 AND room_id = $2
		)
		AND NOT participant`

	// qExistingGroups is _get_existing_groups_with_lock with lock=False
	// (storage/databases/state/deletion.py).
	qExistingGroups = `SELECT id FROM state_groups WHERE id = ANY($1)`
	// qBumpDeletion is _bump_deletion_txn.
	qBumpDeletion = `
		UPDATE state_groups_pending_deletion
		SET sequence_number = DEFAULT, insertion_ts = $1
		WHERE state_group = ANY($2)`
)

// DB is the write connection.
type DB struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// Open connects as the write role.
func Open(ctx context.Context, dsn string, maxConns int32) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("writer: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("writer: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("writer: %w", err)
	}
	return &DB{pool: pool, now: time.Now}, nil
}

// Close closes the pool.
func (d *DB) Close() { d.pool.Close() }

// PushRow is a row for event_push_actions_staging; Actions is the column as
// _serialize_action writes it.
type PushRow struct {
	UserID    string
	Actions   string
	Notif     bool
	Highlight bool
	Unread    bool
	ThreadID  string
}

// StagePush is add_push_actions_to_staging. Nothing is written for no rows.
func (d *DB) StagePush(ctx context.Context, eventID string, rows []PushRow) error {
	if len(rows) == 0 {
		return nil
	}
	n := len(rows)
	users, actions, threads := make([]string, n), make([]string, n), make([]string, n)
	notif, highlight, unread := make([]int16, n), make([]int16, n), make([]int16, n)
	for i, r := range rows {
		users[i], actions[i], threads[i] = r.UserID, r.Actions, r.ThreadID
		notif[i], highlight[i], unread[i] = b2i(r.Notif), b2i(r.Highlight), b2i(r.Unread)
	}
	if _, err := d.pool.Exec(ctx, qStagePush, eventID, d.now().UnixMilli(), users, actions, notif, highlight, unread, threads); err != nil {
		return fmt.Errorf("writer: stage push actions: %w", err)
	}
	return nil
}

// UnstagePush is remove_push_actions_from_staging, after persisting failed.
func (d *DB) UnstagePush(ctx context.Context, eventID string) error {
	if _, err := d.pool.Exec(ctx, qUnstagePush, eventID); err != nil {
		return fmt.Errorf("writer: unstage push actions: %w", err)
	}
	return nil
}

// SetRoomParticipation is set_room_participation.
func (d *DB) SetRoomParticipation(ctx context.Context, userID, roomID string) error {
	if _, err := d.pool.Exec(ctx, qRoomParticipation, userID, roomID); err != nil {
		return fmt.Errorf("writer: room participation: %w", err)
	}
	return nil
}

// BumpDeletion is check_state_groups_and_bump_deletion with lock=False:
// delays any pending deletion of the groups that exist, and reports those
// that are gone.
func (d *DB) BumpDeletion(ctx context.Context, groups []int64) (missing []int64, err error) {
	err = pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, qExistingGroups, groups)
		if err != nil {
			return fmt.Errorf("writer: existing groups: %w", err)
		}
		have, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return fmt.Errorf("writer: existing groups: %w", err)
		}
		if len(have) > 0 {
			if _, err := tx.Exec(ctx, qBumpDeletion, d.now().UnixMilli(), have); err != nil {
				return fmt.Errorf("writer: bump deletion: %w", err)
			}
		}
		held := map[int64]bool{}
		for _, g := range have {
			held[g] = true
		}
		for _, g := range groups {
			if !held[g] {
				missing = append(missing, g)
			}
		}
		return nil
	})
	return missing, err
}

func b2i(b bool) int16 {
	if b {
		return 1
	}
	return 0
}
