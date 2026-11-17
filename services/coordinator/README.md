# Redemption Coordinator Service

The Coordinator owns the durable redemption workflow. It creates one transaction
per client idempotency key, selects the requested coordination mode, validates
state transitions, and preserves enough state for later workflow recovery.

## Core API (LM-010)

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/redemptions` | Create or return an idempotent redemption |
| `GET` | `/v1/redemptions/{transaction_id}` | Return the current durable state |
| `POST` | `/internal/v1/redemptions/{transaction_id}/transitions` | Apply one valid internal transition |
| `GET` | `/internal/v1/redemptions/{transaction_id}/transitions` | Return ordered transition history |

Creation requires an `Idempotency-Key` header. Repeating the same body with the
same key returns the existing transaction. Reusing the key with a different
body returns `409`. The redemption row and initial `REQUESTED` transition are
committed atomically.

## State validation

LM-010 enforces the Coordinator state edges from ADR-001. A 2PC transaction
cannot enter a Saga state, a Saga cannot enter a 2PC decision state, and terminal
states cannot transition again. Every accepted transition:

- locks the redemption row;
- updates status, workflow step, version, and timestamp;
- appends a durable `redemption_transitions` row;
- commits both changes in one PostgreSQL transaction.

Later issues reuse this foundation:

- LM-015 drives the 2PC workflow;
- LM-016 resumes incomplete 2PC transactions;
- LM-022 drives and recovers the Saga state machine.

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
