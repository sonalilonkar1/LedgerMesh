# Audit Ledger Service

The Audit Ledger stores an immutable, append-only history of LedgerMesh events.
It supports auditability and transaction reconstruction but is not the source of
truth for Wallet balances, Catalog inventory, or Coordinator decisions.

## Core API (LM-009)

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/audit-events` | Append a complete event envelope idempotently |
| `GET` | `/v1/transactions/{transaction_id}/events` | Read the transaction timeline |

The append request requires every field in the shared event envelope. An
identical retry returns `outcome: duplicate` with the original ledger sequence
and recording timestamp. Reusing an `event_id` with different content returns
`409` and never changes the stored row.

Transaction queries are ordered by ascending `ledger_sequence`, which is the
order in which the Audit Ledger durably recorded the events. `occurred_at` is
preserved in each envelope but is not used to reorder distributed events whose
producer clocks may differ.

There are no update or delete API routes. PostgreSQL triggers independently
reject `UPDATE`, `DELETE`, and `TRUNCATE`, so application bugs cannot rewrite
history. The unique `event_id` constraint and idempotent append implementation
provide exactly-once audit storage over at-least-once delivery.

The repository also accepts a caller-owned SQLAlchemy `Connection`, allowing a
Kafka consumer to commit its inbox record and audit append atomically.

## Local use

```bash
make infra-up
```

Append an event:

```bash
curl -X POST http://localhost:8003/v1/audit-events \
  -H 'Content-Type: application/json' \
  -d '{
    "schema_version": 1,
    "event_id": "33333333-3333-4333-8333-333333333333",
    "event_type": "redemption.completed",
    "transaction_id": "44444444-4444-4444-8444-444444444444",
    "correlation_id": "44444444-4444-4444-8444-444444444444",
    "causation_id": null,
    "aggregate_type": "redemption",
    "aggregate_id": "44444444-4444-4444-8444-444444444444",
    "aggregate_version": 1,
    "workflow_step": 1,
    "fencing_token": null,
    "occurred_at": "2026-11-16T22:00:00Z",
    "payload": {"state": "COMPLETED"}
  }'
```

Read the transaction timeline:

```bash
curl http://localhost:8003/v1/transactions/44444444-4444-4444-8444-444444444444/events
```

Run tests:

```bash
uv run pytest services/audit_ledger/tests -q
make infra-up
uv run pytest tests/db/test_audit_ledger_core.py -q
```
