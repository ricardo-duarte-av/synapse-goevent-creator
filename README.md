# synapse-goevent-creator

A Go worker for Synapse's client event-creation endpoints: `rooms/{id}/send`,
`rooms/{id}/state`, `rooms/{id}/redact`, membership
(`join|invite|leave|ban|unban|kick`), `join/`, `knock/` and `profile/`. It sits
in place of the Python `event_creator` worker (`av-client-events-1`, which it
is meant to retire) and proxies whatever it does not serve itself to Synapse's
main process.

**Status: M2, shadow.** Every request is forwarded unchanged and counted by
endpoint and room (`goevent_requests_total{endpoint,write,room,served,status}`).
For writes in the allowlisted rooms, the worker also re-derives what Synapse
decided and rebuilds the event, which must come out with the same event ID
(`goevent_shadow_checks_total{endpoint,check,result}`). It writes nothing.
Native event creation comes in stages (`docs/plan.md`), and only ever for the
rooms compiled into `internal/allowlist`.

## How native creation will work

As Synapse does it (`handlers/message.py`, v1.163.0), the creator builds and
signs the event, auths it, computes its state group and push actions, then
hands it to the events writer over the `send_events` replication endpoint. The
persister does the rest: storage, the stream, notifications and remote invites.
gofederation sends it out from the stream.

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
