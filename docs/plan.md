# synapse-goevent-creator — plan

## Context

The goal is a Go replacement for Synapse's `event_creator` worker (`av-client-events-1` today). It would serve the client endpoints that create room events: `/send`, `/redact`, `/state`* (matched by `rooms/.*/send`), membership (`join|invite|leave|ban|unban|kick`), `/join/`, `/knock/` and `/profile/`. It is the fifth worker in the family, after gopro, gosync, gofederation and goinbound. It reuses their conventions and code, and mautrix-go wherever it fits.

Writes are limited to **TEST6TEST** (`!decSfHzLLhrNKJaZid:aguiarvieira.pt`, v11) and **TEST7TESTE** (`!nmZrvcKkeQiFGRlJtE:aguiarvieira.pt`, v11). Both have remote members (daedric.net, unredacted.org, matrix.org), so gofederation will send our events out from the stream.

\* `/state/` is routed to the Python event_creator today (`proxy_host/2.conf`, `rooms/.*/state/`), so it is in scope. GETs of it are reads, and are proxied.

## What Synapse actually does (v1.163.0, `git show v1.163.0:…`)

The creating worker does far less than "write the event". Checked in `synapse/handlers/message.py`:

**On the creator** (this is our job):
1. Auth the requester, ratelimit, check the txn ID (`get_event_id_from_transaction`, which reads `event_txn_id_device_id`).
2. Build the event: prev events from the forward extremities (≤10), depth, auth events chosen from current state, content hash, signature, event ID. This is `create_event` → `create_new_client_event`.
3. Compute the context: state before and after, and a **state group** for state events. The creator writes `state_groups`, `state_group_edges` and `state_groups_state` itself (`UnpersistedEventContext.persist`).
4. `handle_new_client_event`: state dedup (`deduplicate_state_event`), `set_room_participation` for m.room.message/encrypted (a **direct write** to `room_participation`, `roommember.py:1916`), `check_auth_rules_from_context`, and the content round-trip check.
5. `_persist_events`: push actions via the bulk push rule evaluator (a **direct write** to `event_push_actions_staging`, deleted again on failure), ratelimit, then `send_events` (`ReplicationSendEventsRestServlet`, `replication/http/send_events.py`) to `av-event-persister-1`, with the serialized event, internal_metadata (txn_id, device_id, token_id), the context (`events/snapshot.py:173` serialize), the requester (now a Rust type, see `rust/src/types`), `ratelimit` and `extra_users`.

**On the persister** (it is not ours, and it comes for free): `persist_and_notify_client_events`. That covers the remote **invite** over federation (`send_invite`) with its signatures, `invite_room_state` and `knock_room_state`, canonical alias validation, guest kicks, ACL cache invalidation, `event_txn_id_device_id`, notifying, and the stream.

**On main, through replication**: remote join, knock, reject and rescind (`replication/http/membership.py`) and profile writes (`profile_set_field`). Profile writes also fan out membership events to every room the user is in (`_update_join_states`), so **all of `/profile/` is proxied**.

## Architecture

New repo, module `github.com/ricardo-duarte-av/synapse-goevent-creator`, built with `GOEXPERIMENT=jsonv2` (mautrix `federation/pdu`). Code is copied from the siblings, as goinbound did, not imported across repos.

| Package | Source | Purpose |
|---|---|---|
| `internal/synapsecfg` | goinbound (`internal/synapsecfg`), keep the `lastWins` duplicate-key handling | Parse `/etc/synapse/homeserver.yaml` (RO mount of `/opt/matrix/synapse/synapse`): server_name, DB DSN, `instance_map`, `stream_writers.events`, `worker_replication_secret`, signing key path, rc_message, experimental flags |
| `internal/config` | goinbound pattern | Own YAML: listen socket, upstream (main, `av-synapse-main.sock`: the fallback, since av-client-events-1 is to be retired), `rooms_allowlist`, per-endpoint `modes` (`proxy` / `shadow` / `native`, all required, no defaults) |
| `internal/proxy` | gopro / goinbound | Reverse proxy to main for everything not served |
| `internal/auth` | gosync (`docs/auth.md`) | `whoami` over the socket, LRU + singleflight, 502 on unknown, never 401. Add a read-only `access_tokens`/`users` lookup for `token_id` and `shadow_banned`, which the requester serialization needs |
| `internal/pdu` | goinbound (Synapse's `event_auth.py` port) | Auth checks. **Not** mautrix eventauth (see goinbound CLAUDE.md "Port Synapse, not the spec") |
| `internal/build` | new, on mautrix `federation/pdu` + `federation/signutil` | Event building: prevs, depth, `auth_events` selection (port `event_auth.auth_types_for_event`), hashes, signing with `aguiarvieira.pt.signing.key`, v11 redaction, event ID |
| `internal/state` / stategroups | goinbound `internal/inbound/stategroups.go`, state cache LRU | State before the event from the prevs' groups, and a new state group for state events (Synapse's delta rule) |
| `internal/push` | goinbound `internal/push` + `base_rules.json` | Bulk push rule evaluation into `event_push_actions_staging` |
| `internal/replication` | goinbound `internal/replication/client.go` | Add `SendClientEvents` (`send_events`, not `fed_send_events`), same txn-ID retry semantics |
| `internal/dbrole` | goinbound | A dedicated role `goevent_creator_rw`, granted SELECT plus write on exactly `state_groups`, `state_group_edges`, `state_groups_state`, `state_group_id_seq`, `event_push_actions_staging` and `room_participation`. Direct socket, not pgcat |
| `internal/handlers` | new | `/send/{type}/{txn}`, `/state/{type}/{key}`, `/redact/{id}/{txn}`, membership, `/join/{id}`, `/knock/` (initially proxied) |
| `internal/metrics`, `dbtrace` | gofederation | Named queries, Prometheus, `writer` label (never `instance`) |

### Request flow
1. Match the route. Profile and anything unknown are proxied.
2. Auth via whoami.
3. Resolve the room (alias → ID for `/join/`). If it is **not in the allowlist, proxy**.
4. Per-room in-process linearizer, matching Synapse's per-room member/event locks.
5. Txn dedup: an in-memory cache, then `event_txn_id_device_id`.
6. Build → auth → dedup state → state group → push staging → `send_events` → reply `{"event_id"}`. A persist failure deletes the push staging rows, as Synapse does.
7. Membership: `leave`/`ban`/`unban`/`kick`/`invite` and a local `join` in a room we're joined to all go native (`invite` to a remote user also goes native, because the persister does the federation part). A join to a room we're not in, any knock, and an invite rejection of a remote invite are proxied. Port `update_membership`'s checks: `is_direct`, the join-rules and restricted-join `join_authorised_via_users_server`, power levels, "already in room", and the ratelimits.

### Safety rails (enforced by tests, not just config)
- `TestWritesOnlyAllowlistedRooms`: every code path that reaches `send_events` or a DB write goes through one `allowlist.Check(roomID)`, and a test asserts no other caller.
- `TestEveryWriteIsGranted`: the write SQL only touches the granted tables (like goinbound's `TestEveryQueryIsASelect`).
- `TestEveryQuerySiteIsNamed` (gofederation).
- The allowlist is compiled in as a default **and** configured. The config cannot add rooms outside a compile-time set until we decide to lift it.

## Milestones

1. **Skeleton + proxy** (done, 2026-10-16): config, synapsecfg on the RO mount, unix socket listener, proxy everything (deployed on `goevent.aguiarvieira.pt`, upstream main), metrics, Dockerfile, docker-compose (`/opt/matrix/synapse/synapse:/etc/synapse:ro`, `/var/sockets`).
2. **Read path + "decide" shadow** (done, 2026-10-16, except push actions; see CLAUDE.md "Shadow mode"): for proxied requests in the two rooms, compute what Go *would* have built from the same prev events, then compare it with the persisted event: `auth_events`, depth, state group contents, push actions in `event_push_actions` and `room_participation`. Same idea as goinbound's `decide` check. No writes.
3. **Native `/send` (message events)** in the two rooms, through `send_events`. (Done 2026-10-16: e2e 31/31 on a native worker, every native event verified; see CLAUDE.md "Native mode".)
4. **State events** (`/state/`, with dedup and new state groups) and **`/redact`** (redaction rules, `is_admin_redaction`, the MSC2815 / redaction period check). (Done 2026-10-16: e2e 46/46 with all three native.)
5. **Membership**: local leave/kick/ban/unban, invite (local and remote), and local join, with `/join/{alias}` resolution.
6. **Hardening**: ratelimit parity (`rc_message`, `rc_invites`), shadow-ban, guest rules, the `m.room.encrypted` participation write, and the `extra_users` notify.

`/knock/` and remote joins stay proxied in v1. Profile stays proxied.

## Routing (test hostname only)

`goevent.aguiarvieira.pt` carries the event_creator `location` blocks (`deploy/nginx.conf.example`). `aguiarvieira.pt`'s `proxy_host/2.conf` is not touched. Go proxies everything it does not serve to **main** (`/var/sockets/nginx/av-synapse-main.sock`), so the test host stays fully functional. Main is the fallback, not av-client-events-1, because the end goal is to retire av-client-events-1 entirely (operator, 2026-10-16).

## Verification

- `go test -race ./...` with no DB.
- Write-path tests on the **throwaway PostgreSQL** with the live schema (the goinbound CLAUDE.md recipe), running as the granted role, so a missing grant fails there.
- Replication wire-format tests against a fake persister, compared byte for byte with what Python's `_serialize_payload` produces (capture one real `send_events` body from av-client-events-1 if possible).
- Live, on the test host with the test account `@test:aguiarvieira.pt` (joined to both rooms; credentials in `.claude/deployment-notes.md`): send a message, a state change, a redaction, kick/invite/leave/rejoin, and a remote invite to `@test:daedric.net`. Each must show up in `/sync` (gosync and Python), federate (gofederation metrics, and the remote side sees it), carry correct push actions, and survive the decide check re-run on Go-built events.
- Prometheus: `synapse_outgoing_replication_requests_total{name="send_events"}` from our worker, and persister errors. Python's event_creator traffic on the main host must be unchanged.

## Open items to resolve during M1–M2
- The exact `Requester.serialize()` fields (Rust, `rust/src/types`) and whether `token_id` is required for txn dedup.
- How Synapse orders prev events and picks extremities (`get_prev_events_for_room`), and the "dummy event" extremity pruning, which is main's background job and not ours.
