# Rewards Catalog Service

The Catalog Service owns reward definitions, point costs, inventory, and the
reservation lifecycle. No other LedgerMesh service reads or writes its database.

## Core API (LM-008)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/rewards` | List rewards with available inventory by default |
| `GET` | `/v1/rewards/{reward_id}` | Return authoritative cost, inventory, and version |
| `POST` | `/v1/rewards/{reward_id}/reservations` | Reserve inventory for one transaction |
| `POST` | `/v1/rewards/{reward_id}/releases` | Release or tombstone the same reservation |
| `POST` | `/v1/rewards/{reward_id}/fulfillments` | Mark an active reservation fulfilled |

Reserve and release requests share the `transaction_id`, reward, and quantity.
An identical retry returns the stored reservation together with current reward
inventory and version without changing inventory again. Reusing a transaction
ID for another reward or quantity returns `409`.

The reward row is locked before checking the reservation record. Inventory and
the reservation row change in one PostgreSQL transaction, and database checks
prevent inventory from becoming negative or exceeding its total.

If release arrives before reserve, Catalog stores a `tombstone` without changing
inventory. A delayed reserve with the same transaction ID is then rejected, as
required by ADR-001 section 7.1.

Repository mutations accept an optional caller-owned SQLAlchemy `Connection`.
LM-021 uses it to commit the inbox record, inventory mutation, and outbox reply
in one local transaction. HTTP routes use the transaction-owning wrapper.

## Local use

```bash
make infra-up
LEDGERMESH_ENV=local make db-baseline

curl http://localhost:8002/v1/rewards
curl 'http://localhost:8002/v1/rewards?available_only=false'
curl http://localhost:8002/v1/rewards/reward-coffee
```

Reserve, release, and fulfill:

```bash
curl -X POST http://localhost:8002/v1/rewards/reward-coffee/reservations \
  -H 'Content-Type: application/json' \
  -d '{"transaction_id":"22222222-2222-4222-8222-222222222222","quantity":1}'

curl -X POST http://localhost:8002/v1/rewards/reward-coffee/releases \
  -H 'Content-Type: application/json' \
  -d '{"transaction_id":"22222222-2222-4222-8222-222222222222","quantity":1}'
```

Run tests:

```bash
uv run pytest services/catalog/tests -q
make infra-up
uv run pytest tests/db/test_catalog_core.py -q
```

The 2PC participant API is implemented by LM-014. Kafka Saga consumption is
implemented by LM-021. Both reuse these core Catalog operations.
