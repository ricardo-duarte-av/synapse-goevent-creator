# Working on synapse-goevent-creator

A Go replacement for Synapse's `event_creator` worker (`av-client-events-1`):
the client endpoints that create room events. Read `README.md` for what it
does. This file is for things that are not derivable from the code.

The plan, with what Synapse does on the creator vs. the persister, is in
`docs/plan.md`. Deployment specifics and the test account are in
`.claude/deployment-notes.md` (gitignored).

## The deployment

- `aguiarvieira.pt`. nginx (`/opt/npm/data/nginx/proxy_host/2.conf`) sends
  seven patterns to upstream `av-client-events-lc`
  (`/var/sockets/nginx/av-client-events-1.sock`): `rooms/.*/redact`,
  `rooms/.*/send`, `rooms/.*/state/`, `rooms/.*/(join|invite|leave|ban|unban|kick)$`,
  `join/`, `knock/`, `profile/`. `/state/` is one of them, and it also catches
  GETs, which read.
- Events writer: `av-event-persister-1` only (`stream_writers.events`).
- Synapse source: `/home/daedric/synapse`. Port against the deployed tag,
  `git show v1.163.0:<path>`, not the checkout.
- Siblings whose code is copied, not imported: goinbound (`synapsecfg`,
  `proxy`, `replication`, `pdu`, `push`, state groups, `dbrole`), gosync
  (whoami auth), gofederation (`dbtrace`).

## Decisions that are easy to undo by accident

- **Writes only to TEST6TEST (`!decSfHzLLhrNKJaZid`) and TEST7TESTE
  (`!nmZrvcKkeQiFGRlJtE`)** (operator, 2026-10-16). Compiled into
  `internal/allowlist.Permitted`; the config can narrow it, never widen it.
  Every write path asks `allowlist.Check`, and only it. Both rooms have
  remote members, so what we write federates.
- **The goal is retiring `av-client-events-1` entirely** (operator,
  2026-10-16). So the upstream is **main**
  (`/var/sockets/nginx/av-synapse-main.sock`), not the Python event_creator:
  main is the fallback for whatever we don't serve, and nothing may come to
  depend on av-client-events-1. Main is busy and single-threaded, so the
  fallback share should shrink, not grow.
- **Test hostname only: `goevent.aguiarvieira.pt`** (deployed 2026-10-16).
  `aguiarvieira.pt`'s nginx is not to be pointed at us. Changing nginx is
  the operator's call.
- **All of `/profile/` is proxied** (operator, 2026-10-16). Profile writes go
  to main and then send a membership event into every room the user is in
  (`handlers/profile.py`, `_update_join_states`), which breaks the two-room rule.
- **Every endpoint's mode is required** and only modes whose code exists are
  accepted (`config.accepted`). A mode accepted early would silently mean proxy.
- **homeserver.yaml is mounted read-only** at `/etc/synapse`. Never write to it.

## Building

**`GOEXPERIMENT=jsonv2` is required** (Dockerfile and CI set it): mautrix's
`federation/pdu`, which hashes, signs and computes event IDs, only exists
with it. It also switches `encoding/json` to its v2 backend process-wide, as
in gopro.

## Facts checked against the live system

- `av-client-events-1` answers **500 M_UNKNOWN to an unauthenticated request
  without `X-Forwarded-For`** (2026-10-16); authenticated ones (whoami with
  a token, valid or not) are fine without it. ReverseProxy's Rewrite mode
  strips the header, so `proxy.rewrite` puts nginx's headers back. Test by
  hand with `-H 'X-Forwarded-For: 127.0.0.1'`.
- **The build reproduces Synapse byte for byte.** `TestLiveRebuildsSynapseEvents`
  rebuilds the newest 200 local events in the two rooms from their stored
  fields: every event ID and every signature equal (2026-10-16). Ed25519 is
  deterministic, which is what makes the shadow check's "rebuild" exact.
- **mautrix keeps `msc4354_sticky` when redacting; Synapse drops it** (its
  redaction keeps only an allowlist of top-level keys,
  `rust/src/events/utils.rs`, `redact`). The sticky field is in the content
  hash but not in the signature or the event ID, so `build.Build` takes it off
  for those two. Without this every sticky event (most MatrixRTC traffic here)
  gets a wrong ID. Worth reporting upstream.
- **Synapse's `auth_events` order is random**: `auth_types_for_event` returns
  a Python set of tuples, and string hashing is randomised per process. Prev
  events are ordered by depth, ties arbitrary. Compare both as sets.
- The signing key file holds more than one key; Synapse signs with the first
  (`config/key.py`), which is what `synapsecfg.SigningKeyLine` returns.
- Synapse **lets you invite a deactivated user** (`@test01`, 2026-10-16);
  the invite sat there until kicked.
- A join by an already-joined user with an unchanged profile creates **no
  event** (`room_member.py`, duplicate rule: same sender, membership and
  content). The shadow check predicts it (`membership_noop`).

## Native mode (M3 /send, M4 /state and /redact)

`modes.{send,state,redact}: native` serve those endpoints in the allowlisted
rooms (`internal/creator`):
requester from whoami plus `access_tokens`/`users`, txn dedup, one prev event
only, relation and content validation, build, auth, push rows, then the
creator's own writes (deletion bump, room participation, push staging) as the
role `goevent_creator` (`internal/writer`, exactly `writer.Grants`), and
`send_events` to the events writer. Anything else falls back to Synapse before
the first write (`goevent_native_fallback_total{reason}`), and is shadowed.
Each native event is verified against what was persisted
(`goevent_native_verify_total{check,result}`).

- **`context.rejected` is `str | None`: send `null`, never `false`.** On
  2026-10-16 twelve native events in TEST7TESTE were sent with `false`. The
  persister treated it as not rejected (state, stream and push were right) but
  wrote it into `events.rejection_reason` as the text `'false'`. Synapse loads
  rejection from `rejections`, so Synapse itself served them, but **gosync
  filters on `e.rejection_reason IS NULL` (hidden from /sync) and gofederation
  skips any non-empty reason (never sent to remote servers)**. The verify check
  `rejection_column` now catches it. Other Go workers reading Synapse's tables
  have stricter expectations than Synapse: test against them, not only Synapse.
  The operator repaired the 12 rows (NULL) the same day; after the fix the e2e
  suite against a native worker passed 31/31 with every native event verified.
- Room participation's `UPDATE room_memberships` fires the
  `check_event_stream_ordering` trigger, which runs as the caller and reads
  `events`: the role needs SELECT on it (found by `TestWritesAsTheRole`).
- The write role is created or repaired at startup over PostgreSQL's socket with
  homeserver.yaml's credentials (`dbrole.Ensure`); its password is derived.
- **Policy servers (MSC4284)**: Synapse asks a room's policy server to sign
  every local event and refuses what it calls spam
  (`_create_and_send_nonmember_event_locked`). Neither test room has one; a
  room with `m.room.policy` (or the unstable type) in its state falls back.
- **State events**: dedup (`deduplicate_state_event`) is checked before auth,
  as Synapse does; Synapse stores the new state group *before* dedup and leaves
  it orphaned, we do not store it. The new group is a delta of exactly the
  event on the group before (`writer.StoreStateGroup`, goinbound's port, full
  group past 100 hops); the context's `state_group_deltas` is `(before,
  after, [event])` then the group before's own edge. `unsigned.replaces_state`
  is set. Power levels (`POWER_LEVELS_SCHEMA`), server ACL (must not deny us),
  aliases (255 characters), retention and tombstone are validated as
  `validate_new`/`validate_builder` do (`creator/validate.go`): served only when
  the content certainly passes, otherwise Synapse's. Membership, create and
  policy fall back.
- **Presence, not `.get`**: jsonschema and `key in content` treat a present
  `null` as present and invalid; Python's `.get` (`pdu.Get`) reads it as
  absent. Use `pdu.Field` for the former (`m.mentions: null` is Synapse's
  400).
- **Read-your-writes holds only on the creating process.** Synapse's creator
  waits for the writer's streams before answering (`replication/http/_base.py`);
  other processes, main included, catch up over replication. A `GET /event`
  on main right after a native redaction can still show the old content for
  a few milliseconds. The e2e suite polls; that is Synapse's model whenever
  creator and reader differ (av-client-events-1 vs the request workers here).
- **Redactions**: in v11 `redacts` goes into the content (the servlet does
  it); a body naming another `redacts` is Synapse's 400. `recheck_redaction`
  goes in internal metadata when the auth rules set it. Targets that are the
  create event, a server ACL, unknown, or in another room fall back: the
  writer would refuse the first three, and a redaction of a room's create
  event is never sent from here. `msc3912 with_relations` and a set
  `redaction_allowed_period` fall back.

## End-to-end suite

`cmd/goevent-e2e` drives a hostname as a client, as the test account, in one
allowlisted room, and checks statuses, errcodes, events read back
(`unsigned.transaction_id` proves our device and txn reached the writer), and
the worker's counters:

```sh
GOEVENT_E2E_PASSWORD=... GOEXPERIMENT=jsonv2 go run ./cmd/goevent-e2e \
  -base https://goevent.aguiarvieira.pt -metrics http://<container ip>:9102/metrics
```

Run it against shadow mode first: that is Synapse's own behaviour, and the
expectations are calibrated on it (57/57 on 2026-10-19, then 57/57 with send,
state and redact native, 25 native events all verified, power levels, ACL,
aliases and retention among them). It never sends a valid tombstone: that
would mark the room as replaced in every client.

## Shadow mode (M2)

Proxies everything, and for each write in an allowlisted room re-derives
Synapse's decisions and compares (`internal/shadow`): `txn_dedup`,
`state_dedup`, `membership_noop`, `sender`, `prev_events`, `depth`,
`state_before`, `state_after`, `state_group_shape`, `auth_events`,
`auth_rules`, `content`, `rebuild` (exact event ID). Results:
`goevent_shadow_checks_total{endpoint,check,result}`; every mismatch is logged
with both sides, every check summarised at info.

- It reads the room **before** proxying: afterwards, Synapse has already moved
  the extremities on. A concurrent event in the room makes `prev_events`
  mismatch; that is a race, not a bug, if the other event is the difference.
- The database session is **read-only by the server**
  (`default_transaction_read_only=on`); the worker refuses to start
  otherwise, and `TestEveryQueryIsASelect` (pdustore) keeps the code honest.
  It connects as homeserver.yaml's user straight to `/var/sockets`, not pgcat.
- Not compared yet: membership content (profile, M5), refusals (we would need
  the native path's validation), push actions (`event_push_actions`; goinbound's
  push check is the model), events with several prev events (`state_before`
  skipped; Synapse resolves state).

Live run, 2026-10-16, as `@test` in TEST7TESTE: message (PUT and POST), txn
retry, sticky message, topic and its repeat, redaction, ban, unban, invite,
kick, join no-op. Every check matched.
- The live homeserver.yaml parses (`TestLiveHomeserverYAML`).

## Running the tests

```sh
GOEXPERIMENT=jsonv2 go test -race ./...
```

Live tests (read-only) skip without their environment:

```sh
HOMESERVER_YAML=/opt/matrix/synapse/synapse/homeserver.yaml GOEXPERIMENT=jsonv2 go test -run TestLive ./internal/synapsecfg/
SYNAPSE_DSN="host=/var/sockets user=synapse dbname=synapse-db" \
SIGNING_KEY=/opt/matrix/synapse/synapse/aguiarvieira.pt.signing.key \
  GOEXPERIMENT=jsonv2 go test -run TestLive ./internal/build/
```

Write-path tests (from M3) use a throwaway PostgreSQL with the live schema,
never the live database: see goinbound's CLAUDE.md, "Write-mode tests".
