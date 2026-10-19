# synapse-goevent-creator

A Go worker for Synapse's client event-creation endpoints: `rooms/{id}/send`,
`rooms/{id}/state`, `rooms/{id}/redact`, membership
(`join|invite|leave|ban|unban|kick`), `join/`, `knock/` and `profile/`. It sits
in place of the Python `event_creator` worker (`av-client-events-1`, which it
is meant to retire) and proxies whatever it does not serve itself to Synapse's
main process.

**Status: M6.** In the rooms compiled into `internal/allowlist`, `send`,
`state`, `redact`, membership and `join/` can be served natively
(`modes.*: native`). The worker builds and signs the event, auths it, computes
its state group and push actions, writes what Synapse's event creator writes
itself (push actions staging, room participation, state groups) as the role
`goevent_creator`, and hands the event to the events writer over the
`send_events` replication endpoint. The persister does the rest: storage, the
stream, notifications and remote invites. gofederation sends it out from the
stream. Each native event is checked afterwards against what was persisted
(`goevent_native_verify_total{check,result}`).

Requests are authenticated from Synapse's own tables, as `get_user_by_req`
does, without asking Synapse. A request Synapse would refuse is refused here,
in Synapse's words and at the point Synapse refuses it
(`goevent_native_refusals_total{endpoint,reason}`); the e2e suite compares
every refusal body with a recording of Synapse's
(`cmd/goevent-e2e/refusals.golden.json`).

Anything not reproduced exactly (a remote join, a 3pid invite, a room with
several forward extremities, an appservice or guest, ...) is proxied to main
before the first write, counted
(`goevent_native_fallback_total{endpoint,reason}`) and shadow-checked: Synapse's event is rebuilt and must come out with the same
event ID (`goevent_shadow_checks_total{endpoint,check,result}`). Every request
is counted by endpoint and room
(`goevent_requests_total{endpoint,write,room,served,status}`). `knock/` and
`profile/` are always proxied.

## Running

```sh
mkdir -p ./goevent
cp deploy/goevent-creator.example.yaml ./goevent/goevent-creator.yaml
docker compose up -d
```

Then point a **test** hostname's event_creator locations at
`/var/sockets/nginx/av-goevent-creator-1.sock` (`deploy/nginx.conf.example`).

Build: `GOEXPERIMENT=jsonv2 go build ./cmd/goevent-creator`. Test:
`GOEXPERIMENT=jsonv2 go test -race ./...`.
