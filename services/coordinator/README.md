# Redemption Coordinator Service

The Coordinator owns the durable redemption workflow. It creates one transaction
per client idempotency key, selects the requested coordination mode, validates
state transitions, and preserves enough state for later workflow recovery.

## Core API (LM-010)

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/redemptions` | Create or return an idempotent redemption |
| `GET` | `/v1/redemptions/{transaction_id}` | Return the current durable state |
| `GET` | `/internal/v1/redemptions/{transaction_id}/transitions` | Return ordered transition history |

Creation requires an `Idempotency-Key` header. Repeating the same body with the
same key returns the existing transaction. Reusing the key with a different
body returns `409`. The redemption row and initial `REQUESTED` transition are
committed atomically.

State changes are deliberately not exposed through HTTP. Only Coordinator
workflow code can call the repository transition operation. This prevents an
untrusted client from walking a transaction through valid-looking states and
reporting success without the corresponding Wallet and Catalog effects. The
internal history endpoint is read-only and, like every `/internal` route, answers
`404` unless `LEDGERMESH_ENV` is `local` or `test` or the request carries the
operator token (LM-042, [security guide](../../docs/security.md)).

## State validation

LM-010 enforces the Coordinator state edges from ADR-001. A 2PC transaction
cannot enter a Saga state, a Saga cannot enter a 2PC decision state, and terminal
states cannot transition again. Every accepted transition:

- locks the redemption row;
- updates status, workflow step, version, and timestamp;
- appends a durable `redemption_transitions` row;
- commits both changes in one PostgreSQL transaction.

Reapplying the current state is an idempotent no-op: the stored record is
returned without advancing the version or workflow step and without appending
another history row. `COMPENSATING -> COMPENSATING` is the one intentional
self-loop because it records a new compensation attempt. Row locking also makes
competing commit and abort decisions mutually exclusive.

Later issues reuse this foundation:

- LM-015 drives the 2PC workflow;
- LM-016 resumes incomplete 2PC transactions;
- LM-022 drives and recovers the Saga state machine;
- LM-023 sequences and durably records Saga compensations.

## Saga worker (LM-022)

`make run-coordinator-saga` runs three durable loops: it starts `REQUESTED`
Saga redemptions after reading the authoritative Catalog quote, consumes Wallet
and Catalog result events, and publishes the Coordinator outbox. A state only
advances when the result acknowledges `current_command_id` and matches the
stored correlation ID, workflow step and aggregate.

Late deliveries are normal under at-least-once messaging. Results for an
earlier workflow step, duplicates with a new event ID after the state has
advanced, and results received after a terminal state are logged, recorded in
the inbox, and acknowledged without changing the redemption. Messages that can
never belong to the workflow—unknown or non-Saga transactions, wrong
correlation IDs, unsupported result types, and wrong aggregates—remain
permanent errors.

Each transition, the next participant command and the inbox claim commit in
one Coordinator database transaction. Restarting the worker therefore resumes
from the stored state and unpublished outbox rows. Transition events are sent
to `redemption.events`; run `make run-audit-consumer` to persist them in the
Audit Ledger with its transactional inbox.

The Catalog quote, `point_cost`, `reward_version`, and `total_points` are filled
when the workflow begins. LM-010 intentionally persists the client request first
without reading another service's database.

A missing reward or structurally invalid Catalog quote moves that redemption
durably from `REQUESTED` to `FAILED` and emits its audit events. Transient
Catalog, network, and database failures leave the redemption in `REQUESTED` and
use bounded exponential backoff. Failures are isolated per redemption so one
bad request cannot stop the starter, result consumer, or outbox publisher.

The Audit Ledger consumer publishes unsupported or malformed transition events
to `redemption.events.dlq`; such an event no longer stops the consumer.

## Saga compensation (LM-023)

A definite Catalog rejection proves that no reservation was applied, so the
Coordinator requests exactly one Wallet refund. For an unknown reservation
outcome, such as the timeout detected by LM-024, Catalog release is requested
first. Only after `catalog.inventory_released` is confirmed does the
Coordinator request the Wallet refund. This avoids releasing inventory for a
known rejection while still covering a reservation whose result was lost.

Every move into or within `COMPENSATING`, its audit event, and the next
compensation command commit in the same Coordinator transaction. A restart
therefore resumes through the inbox/outbox without inventing another business
effect. Participant compensation commands are idempotent and create tombstones
when their forward operation did not happen.

`COMPENSATED` is terminal and distinct from successful `COMPLETED`. A rejected
compensation stays non-terminal in `COMPENSATING`; retry/backoff tooling calls
`retry_compensation` and can alert operators without creating a terminal
`COMPENSATION_FAILED` outcome. The retry API derives the required action from
the durable current command, so a caller cannot refund before a required
inventory release has been confirmed.

## Local use

```bash
make infra-up

curl -X POST http://localhost:8000/v1/redemptions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-redemption-1' \
  -d '{
    "user_id": "user-alice",
    "reward_id": "reward-coffee",
    "quantity": 1,
    "coordination_mode": "saga"
  }'
```

Run tests:

```bash
uv run pytest services/coordinator/tests -q
make infra-up
uv run pytest tests/db/test_coordinator_core.py -q
```

## Two-Phase Commit workflow (LM-015)

Run or resume a 2PC redemption with:

```bash
export LEDGERMESH_ENV=local
curl -X POST \
  http://localhost:8000/internal/v1/redemptions/<transaction-id>/execute-2pc
```

The manual execution endpoint is available only when `LEDGERMESH_ENV` is
explicitly `local` or `test`; hosted/public deployments return `404` unless the
request carries the operator token (`X-LedgerMesh-Operator-Token`, see
[the security guide](../../docs/security.md)). The
recovery worker introduced by LM-016 starts and resumes durable transactions
without exposing this control endpoint publicly.

The workflow reads the authoritative reward quote from Catalog and sends
prepare requests to Wallet and Catalog. Two `PREPARED` votes cause one durable
`COMMIT_DECIDED` transition. Any NO vote, transport failure, or prepare timeout
causes one durable `ABORT_DECIDED` transition. The decision is committed before
COMMIT or ABORT is sent.

If either participant does not acknowledge the final decision, the Coordinator
remains in the corresponding decided state. Calling the endpoint again resends
the same irreversible decision. Once both acknowledge, the Coordinator
atomically stores the terminal state and an audit event in its transactional
outbox.

The Coordinator does not also post the event directly to the Audit Ledger.
`OutboxPublisher` sends the single durable outbox record to Kafka, and the Audit
Ledger consumer stores it idempotently. An Audit Ledger outage therefore never
reverses or blocks the business outcome and there is only one delivery path.

Session advisory locks use a dedicated bounded connection pool. Participant
HTTP waits cannot consume the repository pool and then deadlock while waiting
for a second repository connection. The same lock pool is shared by normal
execution and LM-016 recovery inside the Coordinator process.

| Variable | Default |
|---|---|
| `WALLET_URL` | `http://wallet:8001` |
| `CATALOG_URL` | `http://catalog:8002` |
| `COORDINATOR_HTTP_TIMEOUT_SECONDS` | `2` |
| `COORDINATOR_LOCK_POOL_SIZE` | `5` |
| `COORDINATOR_LOCK_MAX_OVERFLOW` | `10` |
| `COORDINATOR_LOCK_POOL_TIMEOUT_SECONDS` | `30` |

Focused validation:

```bash
uv run pytest services/coordinator/tests/test_two_phase_commit.py -q
make infra-up
uv run pytest tests/db/test_two_phase_commit_workflow.py -q
```

## 2PC crash recovery (LM-016)

The Coordinator runs a bounded recovery scan immediately after startup and at
the configured interval. Recovery uses only committed Coordinator state:

| Durable state | Recovery action |
|---|---|
| `REQUESTED` | Start the workflow normally; no participant was contacted yet |
| `PREPARING` | Persist `ABORT_DECIDED` (presumed abort), then retransmit ABORT |
| `COMMIT_DECIDED` | Retransmit COMMIT until both participants acknowledge it |
| `ABORT_DECIDED` | Retransmit ABORT until both participants acknowledge it |
| `COMMITTED` / `ABORTED` | Never change the outcome; audit delivery remains retryable from the outbox |

The crucial rule is that recovery never repeats the vote phase for a
`PREPARING` transaction. A crash may have occurred after one or both participants
prepared but before the Coordinator durably recorded a decision. Presumed abort
prevents recovery from inventing a conflicting COMMIT decision.

Participant COMMIT and ABORT endpoints are idempotent, so retransmitting a known
decision is safe when one participant acknowledged before the other or when an
acknowledgement was lost.

Operator endpoints:

```bash
# Available only when LEDGERMESH_ENV is explicitly local or test.
# Show transactions that need a presumed abort or decision retransmission.
curl http://localhost:8000/internal/v1/2pc/in-doubt

# Trigger one bounded recovery pass without waiting for the worker interval.
curl -X POST http://localhost:8000/internal/v1/2pc/recovery/run
```

Hosted, staging, and production configurations return `404` for both operator
routes unless the request carries the operator token from
`LEDGERMESH_OPERATOR_TOKEN` ([security guide](../../docs/security.md)). The
periodic recovery worker calls the recovery service directly and does not
depend on either HTTP endpoint.

| Variable | Default in Compose | Purpose |
|---|---:|---|
| `COORDINATOR_RECOVERY_ENABLED` | `true` | Enable the startup/periodic worker |
| `COORDINATOR_RECOVERY_INTERVAL_SECONDS` | `5` | Delay between bounded scans |

Focused validation:

```bash
uv run pytest services/coordinator/tests/test_recovery.py \
  services/coordinator/tests/test_recovery_api.py \
  services/coordinator/tests/test_two_phase_commit.py -q
make infra-up
uv run pytest tests/db/test_two_phase_commit_workflow.py -q
```
