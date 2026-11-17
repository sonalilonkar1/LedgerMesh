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
returns the stored outcome without applying the balance change again. Reusing
the transaction ID with a different wallet or point amount returns `409`.

Every successful mutation updates the balance and its `wallet_operations` row
in one PostgreSQL transaction. The Wallet row is locked during the mutation,
the database prevents negative balances, and `version` increases once per
applied operation. Expected-version conditional updates and retry policy remain
in LM-027.

Run the service with the local stack:

```bash
make infra-up
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

The 2PC participant API is implemented by LM-013. Kafka Saga consumption is
implemented by LM-020. Both reuse these core Wallet operations.
