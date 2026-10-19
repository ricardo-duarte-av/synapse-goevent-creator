package writer

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/dbrole"
)

// TestWritesAsTheRole creates the role from Spec on a throwaway PostgreSQL
// holding the live schema and no rows, and runs every write as it: a grant
// missing from Grants fails here. Never point it at the live database.
//
//	WRITER_TEST_ADMIN_DSN="host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse" \
//	  GOEXPERIMENT=jsonv2 go test -run TestWritesAsTheRole ./internal/writer/
func TestWritesAsTheRole(t *testing.T) {
	admin := os.Getenv("WRITER_TEST_ADMIN_DSN")
	if admin == "" {
		t.Skip("WRITER_TEST_ADMIN_DSN not set")
	}
	if strings.Contains(admin, "/var/sockets") {
		t.Fatal("WRITER_TEST_ADMIN_DSN points at the live database's socket")
	}
	ctx := context.Background()
	const password = "role-test-password"
	if err := dbrole.Ensure(ctx, admin, Spec(password)); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(admin)
	if err != nil {
		t.Fatal(err)
	}
	a, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(ctx)
	for _, q := range []string{
		`DELETE FROM event_push_actions_staging`, `DELETE FROM room_memberships`, `DELETE FROM local_current_membership`,
		`DELETE FROM state_groups_pending_deletion`, `DELETE FROM state_group_edges WHERE prev_state_group IN (101, 102)`,
		`DELETE FROM state_groups_state WHERE room_id = '!r:x'`, `DELETE FROM state_groups WHERE room_id = '!r:x'`,
		`INSERT INTO room_memberships (event_id, user_id, sender, room_id, membership, participant) VALUES ('$m', '@u:x', '@u:x', '!r:x', 'join', false)`,
		`INSERT INTO local_current_membership (room_id, user_id, event_id, membership) VALUES ('!r:x', '@u:x', '$m', 'join')`,
		`INSERT INTO state_groups (id, room_id, event_id) VALUES (101, '!r:x', '$a'), (102, '!r:x', '$b')`,
		`INSERT INTO state_groups_pending_deletion (state_group, insertion_ts) VALUES (101, 1)`,
		`DELETE FROM access_tokens WHERE id = 7001`,
		`INSERT INTO access_tokens (id, user_id, token) VALUES (7001, '@u:x', 'tok')`,
	} {
		if _, err := a.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	// ConnString would return the admin's own string: build the role's.
	d, err := Open(ctx, admin+" user="+Role+" password="+password, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var who string
	if err := d.pool.QueryRow(ctx, `SELECT current_user`).Scan(&who); err != nil || who != Role {
		t.Fatalf("connected as %q (%v), not the role", who, err)
	}

	if err := d.StagePush(ctx, "$e", []PushRow{
		{UserID: "@a:x", Actions: "", Notif: true, Unread: true, ThreadID: "main"},
		{UserID: "@b:x", Actions: `["notify",{"set_tweak":"sound","value":"ping"}]`, Notif: true, Highlight: true, ThreadID: "$t"},
	}); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := a.QueryRow(ctx, `SELECT count(*) FROM event_push_actions_staging WHERE event_id = '$e' AND inserted_ts > 0`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("staged %d rows (%v)", n, err)
	}
	if err := d.UnstagePush(ctx, "$e"); err != nil {
		t.Fatal(err)
	}

	if err := d.SetRoomParticipation(ctx, "@u:x", "!r:x"); err != nil {
		t.Fatal(err)
	}
	var participant bool
	if err := a.QueryRow(ctx, `SELECT participant FROM room_memberships WHERE event_id = '$m'`).Scan(&participant); err != nil || !participant {
		t.Fatalf("participant %v (%v)", participant, err)
	}

	missing, err := d.BumpDeletion(ctx, []int64{101, 102, 999})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != 999 {
		t.Errorf("missing %v, want [999]", missing)
	}
	var ts int64
	if err := a.QueryRow(ctx, `SELECT insertion_ts FROM state_groups_pending_deletion WHERE state_group = 101`).Scan(&ts); err != nil || ts <= 1 {
		t.Errorf("pending deletion not bumped: %d (%v)", ts, err)
	}

	// A state event's group: a delta on 101.
	g, err := d.StoreStateGroup(ctx, "$s", "!r:x", 101, []StateRow{{"m.room.topic", "", "$s"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var edge int64
	if err := a.QueryRow(ctx, `SELECT prev_state_group FROM state_group_edges WHERE state_group = $1`, g).Scan(&edge); err != nil || edge != 101 {
		t.Errorf("edge %d (%v)", edge, err)
	}
	var row string
	if err := a.QueryRow(ctx, `SELECT type || '|' || state_key || '|' || event_id FROM state_groups_state WHERE state_group = $1`, g).Scan(&row); err != nil || row != "m.room.topic||$s" {
		t.Errorf("row %q (%v)", row, err)
	}
	if _, err := d.StoreStateGroup(ctx, "$s2", "!r:x", 999, []StateRow{{"m.room.topic", "", "$s2"}}, nil); err == nil {
		t.Error("stored a group on a prev group that does not exist")
	}

	if err := d.MarkTokenUsed(ctx, 7001); err != nil {
		t.Fatal(err)
	}
	var used bool
	if err := a.QueryRow(ctx, `SELECT used FROM access_tokens WHERE id = 7001`).Scan(&used); err != nil || !used {
		t.Errorf("token used %v (%v)", used, err)
	}
	if err := d.MarkTokenUsed(ctx, 7002); err == nil {
		t.Error("marked a token that does not exist")
	}

	// Nothing else: Synapse's other tables are refused.
	for _, q := range []string{
		`INSERT INTO events (event_id, topological_ordering, stream_ordering, room_id, type, processed, outlier) VALUES ('$x', 1, 1, '!r:x', 't', true, false)`,
		`UPDATE room_memberships SET membership = 'leave'`,
		`DELETE FROM room_memberships`,
		`UPDATE state_groups SET room_id = '!other:x'`,
		`DELETE FROM state_groups_state`,
		`SELECT token FROM access_tokens`,
		`UPDATE access_tokens SET token = 'x' WHERE id = 7001`,
		`DELETE FROM access_tokens`,
	} {
		if _, err := d.pool.Exec(ctx, q); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: allowed (%v)", q, err)
		}
	}
}
