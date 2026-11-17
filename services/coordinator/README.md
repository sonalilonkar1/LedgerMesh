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
curl -X POST \
  http://localhost:8000/internal/v1/redemptions/<transaction-id>/execute-2pc
```

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

The Coordinator then attempts to append the event to the Audit Ledger. An Audit
Ledger outage never reverses or blocks the durable business outcome: the outbox
row remains unpublished and a later execution attempt retries the same
`event_id`.

| Variable | Default |
|---|---|
| `WALLET_URL` | `http://wallet:8001` |
| `CATALOG_URL` | `http://catalog:8002` |
| `AUDIT_LEDGER_URL` | `http://audit-ledger:8003` |
| `COORDINATOR_HTTP_TIMEOUT_SECONDS` | `2` |

Focused validation:

```bash
uv run pytest services/coordinator/tests/test_two_phase_commit.py -q
make infra-up
uv run pytest tests/db/test_two_phase_commit_workflow.py -q
```
