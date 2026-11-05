# Local development

## Install the core service dependencies

```bash
uv sync --python 3.12
```

## Run quality checks

```bash
make check
```

Run a single service's health tests while developing:

| Service | Test command |
|---|---|
| Coordinator | `uv run pytest services/coordinator/tests -q` |
| Wallet | `uv run pytest services/wallet/tests -q` |
| Catalog | `uv run pytest services/catalog/tests -q` |
| Audit Ledger | `uv run pytest services/audit_ledger/tests -q` |

## Run one service

| Service | Command | Default port |
|---|---|---:|
| Coordinator | `make run-coordinator` | 8000 |
| Wallet | `make run-wallet` | 8001 |
| Catalog | `make run-catalog` | 8002 |
| Audit Ledger | `make run-ledger` | 8003 |

Verify a running service:

```bash
curl http://localhost:8000/health
```

Expected response:

```json
{"status":"ok","service":"coordinator"}
```

## Messaging-library tests

```bash
cd libs/messaging
docker compose -f docker-compose.test.yml up -d --wait
uv sync --python 3.12 --locked
uv run pytest
docker compose -f docker-compose.test.yml down -v
```
