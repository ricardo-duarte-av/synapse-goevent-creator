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

- **Writes only to TEST6TEST (`!decSfHzLLhrNKJaZid`), TEST7TESTE
  (`!nmZrvcKkeQiFGRlJtE`)** (operator, 2026-10-16) **the policy room
  (`!FRudVQysKkIHAjrSzd`) and the v12 room (`!goBCqT-...`)** (operator,
  2026-10-20). Compiled into
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
- **The same for a top-level `redacts`** (rooms before v11): Synapse's
  redaction drops it, mautrix's keeps it, so every v3–v10 redaction got a
  wrong signature and ID until 2026-10-22 (shadow caught one in a v9 room;
  the test rooms are all v11+). `TestLiveRebuildsSynapseEvents` now also
  rebuilds local redactions from any room.
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

## Native mode (M3 /send, M4 /state and /redact, M5 membership, M6 refusals)

`modes.{send,state,redact,membership,join}: native` serve those endpoints in
the allowlisted rooms (`internal/creator`):
requester from `access_tokens`/`users` (no whoami), txn dedup, the state
before (resolved across up to ten prev events), relation and content validation, build, auth, push rows, then the
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
- **Policy servers (MSC4284) are served** (`creator/policy.go`), as
  `_create_and_send_nonmember_event_locked` uses `RoomPolicyHandler`: after
  the event is built and before dedup and the auth rules, a non-member event
  (not the policy event itself) is POSTed to the room's policy server
  (`/_matrix/policy/v1/sign`, the unstable path when that is unknown, 30 s),
  its signatures added; then the `ed25519:policy_server` signature must
  verify against `m.room.policy`'s key, or it is asked once more and the event
  refused ("This message has been rejected as probable spam"). A 4xx is
  relayed as `to_synapse_error` writes it, a 5xx, 429 or no answer is a 500,
  an empty answer the policy server's spam refusal. The policy server counts
  only when it is a valid server name, not ours, and has a member joined.
  Membership is never asked. A signature does not change the event ID.
- **The policy room** (`!FRudVQysKkIHAjrSzd`, v11, operator, 2026-10-20) has
  the fake policy server `policy.aguiarvieira.pt` (`cmd/goevent-policy`, the
  `goevent-policy` compose service: its own keys, one bot that joined over
  federation, `/send`, `/sign`, and a localhost control listener on 28449 to
  switch it between sign, refuse, empty and error). It signs everything but
  bodies with `goevent-spam`. Run the suite there with `-room` and
  `-policy-control http://127.0.0.1:28449` and
  `-golden cmd/goevent-e2e/refusals.policy.golden.json` (room IDs in some
  refusals make the room's own recording). Calibrated on Synapse (99/101,
  ACL and fork skipped), then 100/101 fully native, every non-member event
  stored with the policy signature. Every server in that room asks the fake:
  keep it running. Since 2026-10-20 the room's `m.room.policy` is `{}` (no
  policy server) and the bot was kicked, to retire the fake: putting a
  policy server back means inviting and joining the bot again
  (control `/join`) before restoring `m.room.policy`.
- **The v12 room** (`!goBCqT-LJRiRgPetKo34PrUe4GqYZ_R74c6SBGLwTo4`,
  operator, 2026-10-20): MSC4291 room ID, the creator's unlimited power,
  state resolution v2.1. `TestLiveRebuildsSynapseEvents` covers its events;
  the suite there (`-golden cmd/goevent-e2e/refusals.v12.golden.json`)
  passed 94 on Synapse and 95/97 fully native (ACL and policy skipped), the
  fork included.
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
- **Membership** (`creator/membership.go`): the servlets, then
  `update_membership`/`update_membership_locked` for a local change in a room
  we are in. Content is `{membership}` plus `reason` (and MSC4293's
  `redact_events`) from the membership servlet, or the whole body from
  `/join/`, plus the target's global profile (`profiles`) for joins and,
  with `include_profile_data_on_invite`, invites; a missing profile row leaves
  both fields out (Synapse's 404 is swallowed). `extra_users` is the target.
  Order matters and is Synapse's: the PUT success cache
  (`HttpTransactionCache`, 30 min, per process), ratelimits, the
  state-independent checks, then under the room lock the old-membership
  checks (kick of a leave, unban of a non-ban, anything but leave/ban/unban
  on a ban), the restricted-join check, and only then the event transaction
  lookup, as `_local_membership_update` has it. A join by a joined user with
  an unchanged profile is the generic state dedup (`deduplicate_state_event`),
  not `update_membership_locked`'s own comparison, which compares content
  without `membership` and never matches. A leave from a join tells main
  (`membership_change/.../left`, `_user_left_room`). Fallbacks: remote
  joins and knocks (main's in Synapse too), 3pid invites, `third_party_signed`,
  invite filtering account data (MSC4380, MSC4155), server notice rooms,
  restricted joins needing an authorising server, the host not in the room,
  an avatar with media limits configured, and any module,
  `hs_disabled`/MAU limit or consent requirement in homeserver.yaml.
  Blocked rooms are refused here.
- **Membership ratelimits are this process's**, as each Synapse worker's are:
  `rc_joins.local` (joins by a user not yet joined), `rc_invites.*`, and
  `rc_joins_per_room`, which Synapse feeds from every join on the events
  stream and we replay from `room_memberships` (`RecentJoins`). A
  `ratelimit_override` row with no rate exempts. A refusal is answered here
  (429 after Synapse's half-second pause). A request that passes our limits
  and then falls back is counted again by main's.
- **`m.call.invite` is refused in public rooms** (`create_new_client_event`);
  native `/send` refused it (`call_invite_public`). Found while porting
  M5: it had been missed in M3.
- **Repeated JSON keys are Python's**: json.loads keeps the key's first
  position and its last value; gjson finds the first. `pyjson.Dedupe`
  rewrites the body that way before anything reads it, in native and in
  shadow mode.
- **Forks are resolved here** (M6): `resolve_state_groups_for_events` over
  the prev events' groups (`internal/stateres`, goinbound's v2 port), then
  `_make_state_cache_entry`: an input group holding exactly the result is
  reused (and the context has no edge for it), otherwise a new group is
  stored on the input it differs least from, or whole when every input has a
  key the result lacks. Synapse tries the inputs in Python set order, random
  per process, so any of equal ones is Synapse's choice; ours is the lowest.
  Synapse stores that group in `calculate_context_info`, before the checks
  that may refuse the event (an orphan then); we store it only once nothing
  can be refused. All the inputs are bumped (`resolve_state_groups`). On
  2026-10-19 3.7% of the week's local events had several prev events (76 of
  them ten). The e2e step "send on a fork" races a native state event with
  a delayed one (added here, sent by main when due) and checks the resolved group
  (`state_before`). It usually forks within a few attempts (up to 10, the
  delay cycling 0-100 ms); a skip is timing, not a failure.
- **The suite's appservice** (`goevent-e2e`, operator, 2026-10-19):
  `goevent-e2e-appservice.yaml` beside homeserver.yaml, `url: null`,
  exclusive ghosts `@goevent-e2e-ghost*`, and `@test` non-exclusively to
  double-puppet. Its `as_token` is in `.claude/deployment-notes.md` only;
  the suite reads `GOEVENT_E2E_AS_TOKEN` and skips the steps without it.
  Steps: double puppeting with `ts` and its txn retry (the transaction
  cache), device masquerading, the one fixed ghost's join, message and
  leave, and five refusals. Calibrated on Synapse with a local shadow
  worker (`-record`), 89/90 there, the fork skipped as shadow cannot
  verify it; then 91/91 fully native locally and 90/91 deployed
  (`f1944de`, the fork skipped on timing). Synapse answers an appservice's
  txn retry from `HttpTransactionCache`, which shadow cannot see: no
  longer a `new_event` mismatch.
- **Synapse is not asked who the requester is** (M6, operator: "we must not
  depend on synapse"). `requester` is `get_user_by_req` on the database:
  Synapse's token rules (`Mixing Authorization headers...`, `Too many
  Authorization headers.`, `Invalid Authorization header.`, `Missing access
  token`), `_query_for_auth`'s join on `COALESCE(puppets_user_id, user_id)`,
  expiry (`soft_logout: true`), locked accounts, and
  `mark_access_token_as_used` on a token's first request. That is the one
  write outside the creator's tables: `UPDATE (used)` on `access_tokens`, and
  `SELECT (id, used)` for its WHERE; the role cannot read a token. On
  2026-10-19 5245 of 5306 tokens were not marked used (old ones, whose
  traffic goes to other Go workers), so falling back for them was no
  option. Appservice tokens (the registrations' `as_token`), anything
  pymacaroons could deserialize (guest macaroons need
  `macaroon_secret_key`), delegated auth (MAS, MSC3861) and
  `account_validity` are Synapse's.
- **Appservices are served** (`get_appservice_user`): the registrations'
  `as_token` (re-read at start, `appservice.Load`), `?user_id=` masquerading
  (`validate_appservice_can_control_user_id`: the namespace regexes as
  `re.match`, then the user must exist), MSC3202 `?device_id=` (a device that
  is not hidden), and `?ts=` as `origin_server_ts`. The requester and the
  context carry `app_service_id`, so the writer applies the appservice's
  `rate_limited`; membership ratelimits skip unlimited ones too. Without a
  device there is no transaction lookup: a retry is answered by the
  transaction cache (`answerCache`, Synapse's `HttpTransactionCache`, keyed
  as it keys: path plus user and device, guest, appservice, or token). The
  doublepuppet registration claims every local user, so bridged messages
  from real users come this way. `ip_range_whitelist` is Synapse's. Not run
  live: it needs a bridge's token, which is the operator's (since covered by the
  suite's own appservice).
- **`_record_request` is reproduced** (`internal/userips`): a `USER_IP`
  command on the Redis bus, channel `<server_name>/USER_IP`, at most once per
  user, token and address every two minutes per process, as
  `insert_client_ip` sends it from a worker; main writes `user_ips`, the
  device's last seen and MAU. The address is the first of X-Forwarded-For's
  (`x_forwarded: true`), the user the token's owner; appservices and puppets
  only when `track_appservice_user_ips` / `track_puppeted_user_ips` say so.
- **Refusals are given here, in Synapse's order and words** (M6). A check
  answers nil, a refusal (status, errcode, message, extra fields) or a
  fallback (`creator.stop`). A fallback may sit anywhere before the first
  write (main then decides); a refusal must come after every check Synapse
  makes before it, so `serve` and `prepare` follow Synapse's order: auth,
  `parse_json_object_from_request`, the servlet (sticky `parse_integer`,
  the redact servlet's `redacts`, a member event on `/send` is a 500), the
  txn lookup, `check_local_user_in_room`, `validate_builder`, the create
  resend check, `validate_new` (canonical JSON, aliases, retention, ACL, the
  power levels and mentions schemas), `_validate_event_relation`, the call
  invite check, state dedup, the writer's redaction checks, size limits,
  then the auth rules (`pdu.AuthError` now carries Synapse's exact text,
  status and MSC3848 errcode; `Inexact` ones fall back). Membership:
  `assert_params_in_dict`, `UserID.from_string` (its TypeErrors are 500s),
  the 3pid params check, blocked rooms, `Cannot invite this user`,
  `Invites have been disabled on this server`, then the old-membership
  refusals.
- **Python's words need Python's values**: jsonschema messages and some
  others hold `repr()` of the decoded JSON (`pyRepr`: quotes, `\x`/`\u`
  escapes, `True`/`None`), and `best_match` (jsonschema 4.26) picks the
  shallowest error, then the *greatest* path among siblings. Python's
  `Sequence` makes a string an array (`m.mentions.user_ids: "x"` passes); a
  non-string in `user_ids` is a TypeError, Synapse's 500. `1E400` is a float
  infinity that cannot be built into an event: a 500 before any "Bad JSON
  value: float". Content 127 levels deep (the content object counted)
  cannot be built into an event, Rust's serde_json stopping at 128 with the
  event around it: `400 M_BAD_JSON "Invalid event JSON"`, before any
  canonical JSON refusal. Python decodes and encodes up to ~9,996 levels;
  past 9,000 the request is main's. Lone surrogate escapes and integers past
  4300 digits fall back too. `internal/creator/testdata/oracle_validator.py`
  runs Synapse's own validator in its container (read-only) and its output is
  the test vectors; regenerate it rather than guessing.
- **Every kind of requester whose token is in the database is served**:
  shadow-banned (a non-member event or an invite is pretended, after
  Synapse's 1-10 s pause, with `generate_fake_event_id`'s `$` and 43
  letters, remembered for a retried transaction as `HttpTransactionCache`
  would; other membership goes through, per-room profiles dropped),
  suspended (MSC3823's `M_USER_SUSPENDED` refusals: only leaving, and
  redacting one's own events), guests (no redactions, only join and leave,
  joins where `guest_access` is `can_join`), puppet tokens (the puppet's
  owner is `authenticated_entity`) and tokens without a device (no txn
  lookup, no `device_id` in the metadata).
- **`PUT /state/m.room.member/{user}` is membership** (the state servlet
  calls `update_membership` with the body as content and its membership as
  the action): Element's per-room nicknames. Per-room `displayname` and
  `avatar_url` are served (`len(v or "")` checks, a TypeError being a 500;
  with `max_avatar_size` or `allowed_avatar_mimetypes` set the avatar is
  the media repository's to judge, and main's), and the global profile fills
  in only what the content leaves out. `update_membership_locked`'s own
  no-op (same sender, membership, and content compared as Python dicts, so
  `true == 1`) answers with the old event (`membership_noop`); for the
  membership servlet it never matches, the content having no membership.
- **Remote invitees' profiles are asked of their server** (the one outbound
  federation request: `internal/fedclient`, goinbound's client). Synapse's
  `make_query`, one field at a time (10 s), and a failure ends the lookups,
  swallowed by `update_membership_locked`; whatever the server answers is
  the content (a float there is then the canonical JSON refusal). Outbound
  connections obey `federation_ip_range_blocklist` (Synapse's default
  private ranges, `ip_range_whitelist`) and `federation_domain_whitelist`.
  Synapse asks once too: its retry loop retries only `if retries_left and
  not timeout`, and `make_query` passes `timeout=10000`. The
  container needs outbound HTTPS for it. The invite itself is the events
  writer's to send: an invitee on a server that does not exist is its 502,
  relayed (`Can't connect to server nonexistent.invalid`, an e2e step).
- **Knocks on rooms this server is in are served** (`modes.knock:
  native`): `KnockRoomAliasServlet` is POST only, refuses guests
  (`M_GUEST_ACCESS_FORBIDDEN`), needs a body, and keeps only `reason`; the
  knocker's profile is added as for a join, and the auth rules judge it. A
  knock on a room we are not in is a remote knock, main's, and so is an
  alias.
- **Delayed events are added here** (MSC4140, `DelayedEventsHandler.add`):
  `?org.matrix.msc4140.delay=` on `/send` and `/state` (member events too,
  as sent, before the member path), after the servlet's JSON, `ts` and
  sticky parsing. `rc_message` is counted in this process (`update=True`,
  unlike the event path's pre-check), with the user's
  `ratelimit_override` rates; then `max_event_delay_duration`,
  `validate_builder`, and `add_delayed_event_txn` (per-user limit, its
  429 with `retry_after_ms` possibly null) as the role, granted SELECT and
  INSERT on `delayed_events`. Main, which schedules and sends delayed
  events, is told the next send time
  (`/_synapse/replication/added_delayed_event/`) and creates the event
  when due. Cancel, restart and send (`/delayed_events/...`) are not
  routed here.
- **Writer-side refusals** (`persist_and_notify_client_events`): redacting
  the create event (403), an event of another room (400) or a server ACL
  (403) is refused here, never sent. The writer ratelimits (`rc_message`)
  before those checks, so a refusal here is not counted there. A redaction
  of an event we do not hold is sent: the writer checks nothing either.
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

Every refusal's body is compared with Synapse's recording,
`cmd/goevent-e2e/refusals.golden.json` (run IDs, the account and event IDs
normalised). Record it against a worker that proxies the refusals (shadow, or
the M5 image) with `-record`; on 2026-10-19 it holds 24 bodies.

Run it against shadow mode first: that is Synapse's own behaviour, and the
expectations are calibrated on it (57/57 on 2026-10-19, then 57/57 with send,
state and redact native, 25 native events all verified, power levels, ACL,
aliases and retention among them; with M5's membership steps 69/69 on
Synapse, then 69/69 twice with membership and join native too, 31 native
events, 205 verify checks matched; with M6 70/70 fully native, the 24
refusal bodies equal to Synapse's, and the only fallback left the create
event's redaction, since answered natively too; then 75/75 on Synapse and
76/76 fully native with the fork and per-room name steps, 26 refusal bodies). It never sends a valid tombstone: that
would mark the room as replaced in every client. Membership steps use
`@goevent-e2e-probe` (no account, no profile) as the target, and the test
account only joins: **it must never leave TEST7TESTE, which is
invite-only**.

## Shadow mode (M2)

**`shadow.all_rooms`** (2026-10-20, for real traffic): writes in every room
are checked, not only the allowlisted ones; it only reads. Outside the
allowlist the read before forwarding has a budget (`prepare_ms`, 250) and a
cap (`prepare_concurrency`, 8), past which the request is simply forwarded
(`skip_slow`, `skip_busy`); mismatches there are logged without content
(no event JSON, no bodies), and the per-event summary is at debug. Shadow
identifies requesters from the database too: whoami is gone entirely.
The read-only pool defaults to 16 connections with it.

**Refusals are compared too** (check `refusal`): when Synapse answers a
shadowed request with anything but 200, the native path runs dry on the
same request after it (`creator.NewPredictor`: no writer, no events writer,
no main, no policy server, no remote server, no USER_IP; it stops before
persisting and before a delayed event's insert). Nothing was written for a
refusal, so the room is as it was. Results: `match` (same status and
canonical body), `mismatch`, `accepted` (we would have served what Synapse
refused: the one to look at first), `skip_fallback` (the native path would
leave it to Synapse) and `skip_ratelimit` (Synapse's 429, or ours: each
process has its own counters). Outside the allowlist the log has status and
errcode only, as messages can quote content.

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
- Not compared yet: membership content (profile, M5). Events with several
  prev events have their state before resolved in the background check
  (`resolveBefore`, the native path's state resolution; the groups do not
  change), so `state_before` is compared on forks too. A refusal not
  compared because the native path would leave it to Synapse is counted by
  reason (`goevent_shadow_refusal_fallbacks_total`) and logged without
  content. A `prev_events` difference where a stored
  prev event was persisted after the room was read is a race: recorded as
  skipped, and nothing past it is compared.

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
