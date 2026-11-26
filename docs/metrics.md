# Metrics and the 2PC-vs-Saga dashboard (LM-034)

`make infra-up` now also starts **Prometheus** (http://localhost:9090) and **Grafana** (http://localhost:3000). Grafana opens on the provisioned **"LedgerMesh: 2PC vs Saga"** dashboard, with no login needed locally. `make metrics-open` prints both links.

If another project already uses those ports, move them, along with any other clashing service port:

```bash
PROMETHEUS_PORT=19090 GRAFANA_PORT=13000 make infra-up
```

Only the host ports change. Prometheus still reaches the services inside the Compose network.

## What is scraped

| Job | Targets | How |
|---|---|---|
| `services` | `coordinator:8000`, `wallet:8001`, `catalog:8002`, `audit-ledger:8003` | each API serves `GET /metrics` (kept out of the OpenAPI contract) |
| `workers` | host ports `9100` coordinator, `9101` wallet, `9102` catalog, `9103` audit ledger | each Kafka worker (`make run-*-saga`, `make run-audit-consumer`) serves metrics on its port. `LEDGERMESH_METRICS_PORT` overrides it, and `0` disables it |
| `invariants` | host port `9104` | `make metrics-invariants` |

A worker or exporter that isn't running just shows as **down** on http://localhost:9090/targets. That's expected when you only run the APIs.

## Metrics

All names and labels are declared once, in `ledgermesh_messaging/metrics.py`.

| Metric | Labels | Meaning |
|---|---|---|
| `ledgermesh_redemptions_total` | `mode`, `outcome` | redemptions that reached a terminal state (`COMMITTED`, `ABORTED`, `COMPLETED`, `FAILED`, `COMPENSATED`) |
| `ledgermesh_redemption_duration_seconds` | `mode`, `outcome` | request → terminal state; **p50/p95/p99** come from this histogram |
| `ledgermesh_transitions_total` | `mode`, `state` | every Coordinator state change (a repeated, idempotent transition isn't counted twice) |
| `ledgermesh_compensations_total` | `mode`, `attempt` | Saga compensations started (`first`) or retried (`retry`) |
| `ledgermesh_2pc_prepared_hold_seconds` | `service`, `decision` | **blocking**: how long a prepared transaction held its locks before the commit/abort arrived |
| `ledgermesh_retries_total` | `consumer`, `tier` | `in_process` retries and `retry_topic` trips (LM-024) |
| `ledgermesh_dead_letters_total` | `consumer`, `reason` | `invalid_envelope`, `permanent_error`, `retries_exhausted` |
| `ledgermesh_duplicates_rejected_total` | `consumer` | redeliveries the inbox recognised and did not apply again |
| `ledgermesh_message_handling_seconds` | `consumer` | time per message-handling attempt |
| `ledgermesh_http_request_duration_seconds` | `service`, `method`, `route`, `status` | HTTP latency; `route` is the matched route template, or `unmatched` for any path with no route (404s, probes), so arbitrary URLs never become label values |
| `ledgermesh_invariant_violations` | `invariant` | `INV-01`…`INV-06`, `prepared_leak`; 0 when clean |
| `ledgermesh_invariant_check_up` | | 1 if the last invariant check ran |

`mode` is the redemption's `coordination_mode`: `two_phase_commit` or `saga`.

**Only committed state is counted.** Transition, outcome, duration and compensation metrics are collected while their database transaction is open, and published only after it commits (`metrics_on_commit` in `ledgermesh_messaging.metrics`). A transaction that rolls back publishes nothing, and its retry is counted once, so the dashboard agrees with the durable `redemption_transitions` history. `InboxConsumer`, the Saga starter and the Coordinator repository's own transactions all use it. Code that passes its own connection to the repository should wrap its transaction the same way.

## The dashboard

| Row | Panels |
|---|---|
| Outcomes: 2PC vs Saga | redemptions per minute by mode and outcome; success ratio by mode; HTTP availability by service |
| Latency | **p50 / p95 / p99** for 2PC and, separately, for Saga; HTTP p95 by route |
| Blocking and state flow | 2PC prepared-lock hold time p50/p95/p99; transitions per minute by mode and state |
| Retries, compensations and duplicates | retries by tier; dead letters by reason; Saga compensations; duplicates rejected |
| Correctness | invariant violations per invariant (red above 0); whether the check is running |

The dashboard JSON is in `infra/grafana/dashboards/` and is provisioned read-only. To change it, edit the file and restart Grafana.

## Invariant violations

`make metrics-invariants` runs the cross-service invariant checker from the 2PC integration tests (LM-017) every 15 seconds against the four Compose databases, and publishes the result. It reuses that checker rather than copying the rules, so when the invariant checker issue (LM-031) extends it, the dashboard picks that up. Database URLs default to the Compose host ports. Override them with `COORDINATOR_DATABASE_URL`, `WALLET_DATABASE_URL`, `CATALOG_DATABASE_URL` and `AUDIT_LEDGER_DATABASE_URL`.

## Tests

| Guarantee | Test |
|---|---|
| Every service serves `/metrics`, with its own HTTP latency | `tests/test_metrics_endpoints.py` |
| Prometheus scrapes every service; the dashboard separates 2PC and Saga, shows p50/p95/p99, and has a panel for blocking, retries, compensations, duplicates and invariants; every query uses a declared metric; panels don't overlap | `tests/infra/test_observability_config.py` |
| Outcomes, durations, transitions, compensations, hold time, retries, dead letters, invariants and HTTP latency are recorded | `libs/messaging/tests/test_metrics.py` |
| The inbox counts duplicates and in-process retries | `libs/messaging/tests/test_consumer_metrics.py` |
| The Coordinator and the Wallet participant record against real databases | `tests/db/test_metrics_recording.py` |
