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
internal history endpoint is read-only.

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
- LM-022 drives and recovers the Saga state machine.

## Saga worker (LM-022)

`make run-coordinator-saga` runs three durable loops: it starts `REQUESTED`
Saga redemptions after reading the authoritative Catalog quote, consumes Wallet
and Catalog result events, and publishes the Coordinator outbox. A state only
advances when the result acknowledges `current_command_id` and matches the
stored correlation ID, workflow step and aggregate.

Each transition, the next participant command and the inbox claim commit in
one Coordinator database transaction. Restarting the worker therefore resumes
from the stored state and unpublished outbox rows. Transition events are sent
to `redemption.events`; run `make run-audit-consumer` to persist them in the
Audit Ledger with its transactional inbox.

The Catalog quote, `point_cost`, `reward_version`, and `total_points` are filled
when the workflow begins. LM-010 intentionally persists the client request first
without reading another service's database.

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
explicitly `local` or `test`; hosted/public deployments return `404`. The
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
routes. The periodic recovery worker calls the recovery service directly and
does not depend on either HTTP endpoint.

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
