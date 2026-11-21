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
