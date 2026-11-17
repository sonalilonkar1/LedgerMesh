# Wallet Service

The Wallet Service owns point balances and the operation records that make
deductions and refunds idempotent. No other LedgerMesh service reads or writes
the Wallet database.

## Core API (LM-007)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/wallets/{user_id}` | Return current points and Wallet version |
| `POST` | `/v1/wallets/{user_id}/deductions` | Atomically deduct points and record the transaction |
| `POST` | `/v1/wallets/{user_id}/refunds` | Refund the applied deduction with the same transaction ID |

A deduction and its refund share one `transaction_id`. An identical retry
returns the stored operation together with the wallet's current balance and
version, without applying the balance change again. Reusing the transaction ID
with a different wallet or point amount returns `409`.

Every successful mutation updates the balance and its `wallet_operations` row
in one PostgreSQL transaction. The Wallet row is locked during the mutation,
the database prevents negative balances, and `version` increases once per
applied operation. Expected-version conditional updates and retry policy remain
in LM-027.

Repository mutations also accept a caller-owned SQLAlchemy `Connection`. LM-020
uses that form so the inbox record, Wallet mutation, and outbox reply commit in
one transaction. HTTP routes use the repository's thin transaction-owning
wrapper.

Over HTTP, a refund that arrives before its deduction returns `404`. The Saga
consumer below applies ADR-001 section 7.1 instead: it records a deduction
tombstone, so a delayed deduction cannot apply after compensation.

Run the service with the local stack:

```bash
make infra-up
LEDGERMESH_ENV=local make db-baseline
curl http://localhost:8001/v1/wallets/user-alice
```

Or provide a database explicitly when running Wallet by itself (`DATABASE_URL`
is the service-container setting; `WALLET_DATABASE_URL` is also accepted for a
clear host-side override):

```bash
WALLET_DATABASE_URL=postgresql+psycopg://wallet:ledgermesh_dev@localhost:54322/wallet \
  make run-wallet
```

Example deduction and refund:

```bash
curl -X POST http://localhost:8001/v1/wallets/user-alice/deductions \
  -H 'Content-Type: application/json' \
  -d '{"transaction_id":"11111111-1111-4111-8111-111111111111","points":100}'

curl -X POST http://localhost:8001/v1/wallets/user-alice/refunds \
  -H 'Content-Type: application/json' \
  -d '{"transaction_id":"11111111-1111-4111-8111-111111111111"}'
```

Run Wallet unit tests:

```bash
uv run pytest services/wallet/tests -q
```

The PostgreSQL repository tests run in the database suite:

```bash
make infra-up
uv run pytest tests/db/test_wallet_core.py -q
```

## Saga command consumer (LM-020)

`services/wallet/app/saga.py` consumes `wallet.commands` (consumer group
`wallet-saga`) and replies on `wallet.results`, keyed by `transaction_id`. Each
command runs through the messaging library's `process_once`, so the inbox row,
the Wallet change and the reply commit in one PostgreSQL transaction. The Wallet
change uses the caller-owned `connection=` form inside a savepoint, so a rejected
command still commits its rejection reply.

| Command | Payload | Replies |
|---|---|---|
| `wallet.deduct_points` | `user_id`, `points` | `wallet.points_deducted` (`applied` or `duplicate`), or `wallet.deduction_rejected` with reason `insufficient_points`, `wallet_not_found`, `cancelled` or `conflict` |
| `wallet.refund_points` | `user_id`, `points` | `wallet.points_refunded` with outcome `applied`, `duplicate` or `cancelled_before_deduction` |

- **Validation:** a command must target the `wallet` aggregate, carry a positive
  `workflow_step` and no `aggregate_version`, name the same user in its payload
  and `aggregate_id`, and carry positive integer `points`. Anything else is a
  `PermanentError` and goes to `wallet.commands.dlq`.
- **Replies** carry the command's `transaction_id`, `correlation_id` and
  `workflow_step`, with `causation_id` set to the command's `event_id`. Only a
  reply describing a new balance change carries `aggregate_version` (the Wallet
  version after the change).
- **Duplicates:** a redelivered command is a duplicate in the inbox. A command
  re-sent with a new `event_id` for the same transaction replies with outcome
  `duplicate` and changes nothing.
- **Outcomes belong to the transaction:** a rejected deduction is stored in
  `deduction_rejections`, so a later command for the same transaction gets the
  same rejection even if the wallet is topped up or created afterwards. A
  genuinely new redemption uses a new `transaction_id`.
- **Identity:** `cancelled` and `duplicate` are returned only when the user and
  points match the stored record. Any other body reusing the transaction id is a
  `conflict`, and a conflicting early refund is dead-lettered.
- **Workflow step:** a deduction must be step 1, the first Saga step in the
  Coordinator's state machine (LM-010), and a refund must be a later
  compensation step. Ordering across messages is checked by LM-026.
- **Early refund:** a refund with no deduction stores a deduction tombstone
  (using the command's `points`) and replies `cancelled_before_deduction`. The
  late deduction is rejected with reason `cancelled`.
- **Restart:** Kafka offsets are committed only after the database transaction,
  so a crash in between redelivers the command and the inbox reports a duplicate.
  Replies already in the outbox are published after the restart.

Run the worker against the local stack (create the topics first):

```bash
uv run ledgermesh-topics
make run-wallet-saga
```

Its tests run in the database suite: `uv run pytest tests/db/test_wallet_saga.py -q`.

## 2PC participant API (LM-013)

| Method | Path | Behavior |
|---|---|---|
| `POST` | `/v1/transactions/{id}/prepare` | Validate the balance and return a durable `PREPARED` or `ABORTED` vote |
| `POST` | `/v1/transactions/{id}/commit` | Commit the PostgreSQL prepared deduction |
| `POST` | `/v1/transactions/{id}/abort` | Roll back prepared work, or create an abort tombstone for an unknown transaction |
| `GET` | `/v1/transactions/{id}` | Return the participant's durable state |

Prepare writes request identity to `participant_transactions`, applies the
deduction inside a PostgreSQL transaction and then executes `PREPARE
TRANSACTION`. The uncommitted balance change survives connection loss but is
not visible until COMMIT. A transaction-scoped PostgreSQL advisory lock
serializes concurrent duplicate prepare, commit and abort calls across Wallet
instances.

Repeated prepare and final-decision requests return the stored outcome. COMMIT
for an unknown transaction returns `404`; ABORT for an unknown transaction
creates an `ABORTED` tombstone, causing a delayed PREPARE to vote NO without
locking the wallet. Reusing a transaction ID with different request data or
trying to reverse a final decision returns `409`.

Example:

```bash
transaction_id="11111111-1111-4111-8111-111111111111"
curl -X POST "http://localhost:8001/v1/transactions/$transaction_id/prepare" \
  -H 'Content-Type: application/json' \
  -d '{"correlation_id":"22222222-2222-4222-8222-222222222222","user_id":"user-alice","points":100}'

curl -X POST "http://localhost:8001/v1/transactions/$transaction_id/commit"
curl "http://localhost:8001/v1/transactions/$transaction_id"
```

Run the participant tests with the local databases running:

```bash
make infra-up
uv run pytest tests/db/test_wallet_participant.py -q
make db-prepared-list
```

The last command must show no unresolved transactions after the test suite.
Kafka Saga consumption is implemented separately by LM-020.
