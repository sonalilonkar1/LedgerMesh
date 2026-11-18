# Databases and migrations (LM-005)

Each service owns one PostgreSQL database (`infra/compose.yaml`) and the only code that changes its schema is that service's Alembic migrations. No service reads or writes another service's database (ADR-001 section 2).

| Service | Schema module | Migrations |
|---|---|---|
| Coordinator | `services/coordinator/app/db/schema.py` | `services/coordinator/migrations/` |
| Wallet | `services/wallet/app/db/schema.py` | `services/wallet/migrations/` |
| Catalog | `services/catalog/app/db/schema.py` | `services/catalog/migrations/` |
| Audit Ledger | `services/audit_ledger/app/db/schema.py` | `services/audit_ledger/migrations/` |

## Tables

| Service | Table | Purpose |
|---|---|---|
| Wallet | `wallets` | Point balance and optimistic-concurrency `version` per user |
| | `wallet_operations` | One row per deduction or refund of a transaction; an early refund stores a deduction `tombstone` that cancels the late deduction |
| | `participant_transactions` | 2PC participant state per transaction |
| Catalog | `rewards` | Point cost, total and available inventory, `version` |
| | `reservations` | One reservation per transaction: `reserved`, `released`, `fulfilled` or `tombstone` |
| | `participant_transactions` | 2PC participant state per transaction |
| | `catalog_saga_outcomes` | Durable reserve/release result per transaction and command type, including rejections retried with a new event ID |
| Coordinator | `redemptions` | One row per redemption: idempotency key, mode, status, current workflow step and command |
| | `redemption_transitions` | History of status changes |
| Audit Ledger | `audit_events` | Append-only record of every event, one row per `event_id` |
| Coordinator, Wallet, Catalog | `inbox`, `outbox` | Transactional inbox and outbox from `libs/messaging` (LM-019) |
| Audit Ledger | `inbox` | Deduplicates consumed events |

## Safety rules enforced by the database

These hold even if application code has a bug, because PostgreSQL rejects the write:

| Rule | Constraint |
|---|---|
| INV-01: a balance never goes negative | `ck_wallets_available_points_non_negative` |
| INV-02: inventory is never negative or above the total | `ck_rewards_available_inventory_non_negative`, `ck_rewards_available_within_total` |
| INV-03: one deduction and one refund per transaction | `uq_wallet_operations_transaction_operation` |
| INV-03: one reservation per transaction (a redemption has exactly one reward) | `uq_reservations_transaction` |
| A late forward command after its compensation cannot apply (ADR-001 7.1) | the compensation stores a tombstone for the *forward* operation, which occupies its unique key |
| A Wallet tombstone always blocks a deduction (a refund tombstone would block nothing) | `ck_wallet_operations_tombstone_blocks_deduct` |
| One redemption per client idempotency key | unique `redemptions.idempotency_key` |
| A redemption's status belongs to its mode (no `COMMIT_DECIDED` Saga, no `COMPENSATION_FAILED`) | `ck_redemptions_status_matches_mode` |
| A 2PC participant row is either an empty abort tombstone or carries its full request (`correlation_id`, `request_hash` and the user and points, or the reward and quantity), so a reused transaction id with a different request is detectable | `ck_participant_transactions_request_identity` |
| Audit history cannot be rewritten | trigger rejects `UPDATE`, `DELETE` and `TRUNCATE` on `audit_events` |
| An event is audited once | unique `audit_events.event_id` |

## Running migrations

`make infra-up` applies every service's migrations when its container starts. Against the Compose databases from the host:

```bash
make db-upgrade
LEDGERMESH_ENV=local make db-downgrade
LEDGERMESH_ENV=local make db-reset
LEDGERMESH_ENV=local SERVICES="wallet catalog" make db-reset
```

`db-downgrade` and `db-reset` delete data, so there is no default: they refuse to run unless you set `LEDGERMESH_ENV=local` or `LEDGERMESH_ENV=test` yourself. That also covers a `<SERVICE>_DATABASE_URL` override pointing somewhere else.

## Seed data and a clean baseline (LM-006)

Demos, tests and experiments start from the same known data:

```bash
LEDGERMESH_ENV=local make db-baseline
LEDGERMESH_ENV=local make db-seed
```

- `db-baseline` wipes every service database (migrations down and up), then seeds it. Afterwards there are no redemptions, transitions, operations, reservations, participant rows, audit events, inbox or outbox rows: only the seed data below, with every version at 0. **Use this to get exact known values.**
- `db-seed` is insert-only: it adds any missing seed user or reward and never changes an existing row. It does not restore a balance or inventory that has changed, because operations or reservations may account for the difference, and overwriting it would leave unmatched deductions, ghost allocations and a regressed version. It is safe to repeat.

Both change data, so both refuse to run unless `LEDGERMESH_ENV` is `local` or `test`. That applies to the public `seed()` and `baseline()` functions too, not only the Make targets. The data lives in `infra/scripts/seed.py`.

| Wallet user | Points | Used for |
|---|---:|---|
| `user-alice` | 10,000 | happy paths, throughput |
| `user-bob` | 1,000 | general use |
| `user-carol` | 150 | exactly one coffee: competing redemptions for one balance |
| `user-dave` | 0 | insufficient points |

| Reward | Cost | Inventory | Used for |
|---|---:|---:|---|
| `reward-coffee` | 100 | 100 | happy paths |
| `reward-headphones` | 2,500 | 5 | expensive reward |
| `reward-last-unit` | 500 | 1 | competing redemptions for the final unit |
| `reward-sold-out` | 300 | 0 | sold-out rejection |

These identifiers are stable. Tests and experiments may refer to them directly, and `test_seed_identifiers_are_stable` fails if one is renamed by accident. The Coordinator and Audit Ledger have no seed data; they start empty.

## Changing a schema

Never edit a migration that has been merged. Instead:

1. Change the service's `app/db/schema.py`.
2. Generate a new revision and review it by hand:
   ```bash
   DATABASE_URL=postgresql+psycopg://wallet:ledgermesh_dev@localhost:54322/wallet \
     uv run alembic -c services/wallet/alembic.ini revision --autogenerate -m "Add ..."
   ```
3. Add a test in `tests/db/test_migrations.py` for any new safety rule.
4. Run `make db-test`.

Migrations never import the live schema modules or the messaging library, so a later code change cannot silently rewrite an old revision. `test_schema_module_matches_the_migrations` fails if `schema.py` and the migrations disagree.

## Tests

```bash
make infra-up
make db-test
```

Each test creates and drops its own temporary database on the Wallet database server, so it never touches your local data. Set `LM_TEST_POSTGRES_URL` to use another PostgreSQL server; CI does this with a PostgreSQL service container.
