# Working on synapse-goevent-creator

A Go replacement for Synapse's `event_creator` worker (`av-client-events-1`):
the client endpoints that create room events. Read `README.md` for what it
does. This file is for things that are not derivable from the code.

The plan, with what Synapse does on the creator vs. the persister, is in
`docs/plan.md`.

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
- **Test hostname only.** `aguiarvieira.pt`'s nginx is not to be pointed at
  us. The old gosync test host (`proxy_host/324.conf`) is gone (gosync now
  serves the main host), so a new test host is needed, with gosync's `/login`
  well_known rewrite. Changing nginx is the operator's call.
- **All of `/profile/` is proxied** (operator, 2026-10-16). Profile writes go
  to main and then send a membership event into every room the user is in
  (`handlers/profile.py`, `_update_join_states`), which breaks the two-room rule.
- **Every endpoint's mode is required** and only modes whose code exists are
  accepted (`config.accepted`). A mode accepted early would silently mean proxy.
- **homeserver.yaml is mounted read-only** at `/etc/synapse`. Never write to it.

## Facts checked against the live system

- `av-client-events-1` answers **500 M_UNKNOWN to any request without
  `X-Forwarded-For`** (2026-10-16). ReverseProxy's Rewrite mode strips it, so
  `proxy.rewrite` puts nginx's headers back. Test by hand with
  `-H 'X-Forwarded-For: 127.0.0.1'`.
- The live homeserver.yaml parses: `HOMESERVER_YAML=/opt/matrix/synapse/synapse/homeserver.yaml go test -run TestLive ./internal/synapsecfg/`.

## Running the tests

```sh
go test -race ./...
```

Write-path tests (from M3) use a throwaway PostgreSQL with the live schema,
never the live database: see goinbound's CLAUDE.md, "Write-mode tests".
