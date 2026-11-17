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

## Saga consumer (LM-021)

The worker consumes `catalog.commands` through the `catalog-saga` consumer
group and publishes transactional replies to `catalog.results`.

| Command | Result |
|---|---|
| `catalog.reserve_inventory` | `catalog.inventory_reserved` or `catalog.reservation_rejected` |
| `catalog.release_inventory` | `catalog.inventory_released` or `catalog.release_rejected` |

The inbox claim, inventory mutation, durable command outcome, and outbox reply
commit in one Catalog database transaction. The inbox handles redelivery of the
same event ID. `catalog_saga_outcomes` also preserves the result for a logical
retry using a new event ID, including rejected reservations.

Release-before-reserve creates a tombstone. A late matching reserve returns
`cancelled`; transaction-ID reuse for another reward or quantity remains a
`conflict` reply rather than being dead-lettered. Concurrent deliveries use the
durable outcome row as the authority: an identical retry returns `duplicate`,
while a different reward or quantity returns `conflict`. A reservation must be
workflow step 3, and a release must be a later compensation step. LM-026
completes semantic cross-topic ordering. This worker also validates the command
type, Catalog aggregate, payload identity, and absence of an aggregate version
on commands.

```bash
make infra-up
uv run ledgermesh-topics
make run-catalog-saga

uv run pytest tests/db/test_catalog_saga.py -q
```

## 2PC participant API (LM-014)

| Method | Path | Behavior |
|---|---|---|
| `POST` | `/v1/transactions/{id}/prepare` | Validate inventory and the quoted version/cost, then return `PREPARED` or `ABORTED` |
| `POST` | `/v1/transactions/{id}/commit` | Commit the PostgreSQL prepared reservation |
| `POST` | `/v1/transactions/{id}/abort` | Roll back prepared work or create an abort tombstone |
| `GET` | `/v1/transactions/{id}` | Return the participant's durable state |

Prepare verifies the reward exists, inventory is sufficient, and the supplied
`expected_reward_version` and `expected_point_cost` still match the Catalog's
authoritative row. It then decrements inventory and inserts the reservation
inside a PostgreSQL transaction followed by `PREPARE TRANSACTION`. Those changes
survive connection loss but remain invisible until COMMIT.

Repeated prepare and final-decision calls return the stored result. COMMIT for
an unknown transaction returns `404`; ABORT for an unknown transaction creates
an `ABORTED` tombstone so a delayed PREPARE cannot allocate ghost inventory.
Conflicting request reuse or an attempt to reverse a final decision returns
`409`. PostgreSQL advisory locks serialize concurrent decisions across Catalog
instances.

Example:

```bash
transaction_id="33333333-3333-4333-8333-333333333333"
curl -X POST "http://localhost:8002/v1/transactions/$transaction_id/prepare" \
  -H 'Content-Type: application/json' \
  -d '{"correlation_id":"44444444-4444-4444-8444-444444444444","reward_id":"reward-coffee","quantity":1,"expected_reward_version":0,"expected_point_cost":100}'

curl -X POST "http://localhost:8002/v1/transactions/$transaction_id/commit"
curl "http://localhost:8002/v1/transactions/$transaction_id"
```

Run the participant tests:

```bash
make infra-up
uv run pytest tests/db/test_catalog_participant.py -q
make db-prepared-list
```

The 2PC and Saga paths reuse the same core Catalog operations while keeping
their coordination and idempotency records separate.
