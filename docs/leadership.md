# Coordinator leadership (LM-038)

Compose runs a three-member etcd cluster. The Coordinator competes for the
linearizable key `/ledgermesh/coordinator/leader`, attached to a renewable
lease. Only an instance that both holds the local lease state and confirms
ownership with a linearizable etcd read may accept `POST /v1/redemptions`.
When renewal or the ownership check fails, that process immediately steps down
and returns `503 Service Unavailable` for new redemption requests. Existing
redemptions and recovery remain durable in PostgreSQL and continue to be
handled by the existing recovery workflow.

## Local validation

```bash
make infra-up
make etcd-smoke
curl -s http://localhost:8000/metrics | grep -E 'ledgermesh_coordinator_leader|ledgermesh_etcd_quorum_available'
```

To demonstrate a second Coordinator process, start it with
`docker compose -f infra/compose.yaml --profile leadership-demo up -d coordinator-follower`.
Only one of `localhost:8000` and `localhost:8004` should accept `POST
/v1/redemptions`; the other should return 503. Remove the demo process with
`docker compose -f infra/compose.yaml --profile leadership-demo stop coordinator-follower`.

The metrics are `ledgermesh_coordinator_leader` and
`ledgermesh_etcd_quorum_available`. Leadership and quorum changes are also
logged. Stop one etcd member and confirm the cluster retains quorum and the
Coordinator remains leader; stop two and confirm the Coordinator reports
quorum unavailable, steps down, and rejects new redemptions. Restore members
and confirm a Coordinator can acquire the lease again.

## Configuration

| Variable | Default | Meaning |
|---|---:|---|
| `COORDINATOR_LEADERSHIP_ENABLED` | `false` | Enables lease-backed admission. Compose sets this to `true`. |
| `ETCD_ENDPOINTS` | local host endpoints | Comma-separated etcd v3 HTTP gateway URLs; use all three in Compose. |
| `COORDINATOR_LEADER_TTL_SECONDS` | `9` | Lease lifetime. Must be at least 3 seconds. |
| `COORDINATOR_LEADER_RENEW_INTERVAL_SECONDS` | `2` | Keepalive interval; must be positive and below TTL. |
| `ETCD_REQUEST_TIMEOUT_SECONDS` | `1` | Per-endpoint timeout so network partitions cannot hang renewal indefinitely. |

The local setting remains opt-in outside Compose so unit tests and standalone
development do not require etcd. Do not enable leadership in a deployment
without configuring the full etcd endpoint set and persistent member storage.
This local cluster uses plaintext HTTP and is for development only; production
must use authenticated TLS and protected etcd endpoints.

If LM-034's shared metrics endpoint is already present when applying this
change, keep its `/metrics` route and use its shared registry; remove the
duplicate route added here. The leadership gauges register with that shared
registry when it is available.
