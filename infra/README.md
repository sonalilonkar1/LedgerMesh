# Local Docker Compose infrastructure

LM-003 provides a reproducible local stack containing:

- One Apache Kafka broker in single-node KRaft mode
- Four isolated PostgreSQL instances
- Coordinator, Wallet, Catalog, and Audit Ledger FastAPI services
- Health checks and dependency-aware startup ordering
- Named volumes for broker and database data

The messaging library's test-only environment remains separate at
`libs/messaging/docker-compose.test.yml`.

## Start the stack

From the repository root:

```bash
make infra-up
```

This builds the service image and runs Compose with `--wait`. Compose does not start an application service until its database and Kafka report healthy.

Verify the complete stack:

```bash
make infra-status
make infra-smoke
```

## Stop or reset the stack

Stop containers while preserving database and Kafka volumes:

```bash
make infra-down
```

Delete containers and all local LedgerMesh infrastructure volumes:

```bash
make infra-reset
```

`infra-reset` permanently deletes local Compose data. It does not delete repository files.

## Ports

| Component | Host port | Container port |
|---|---:|---:|
| Coordinator API | 8000 | 8000 |
| Wallet API | 8001 | 8001 |
| Catalog API | 8002 | 8002 |
| Audit Ledger API | 8003 | 8003 |
| Kafka | 29092 | 29092 |
| Coordinator PostgreSQL | 54321 | 5432 |
| Wallet PostgreSQL | 54322 | 5432 |
| Catalog PostgreSQL | 54323 | 5432 |
| Audit Ledger PostgreSQL | 54324 | 5432 |

Applications inside the Compose network connect to Kafka at `kafka:19092`. Programs running on the host connect at `localhost:29092`.

## Databases

| Service | Host | Database | User |
|---|---|---|---|
| Coordinator | `localhost:54321` | `coordinator` | `coordinator` |
| Wallet | `localhost:54322` | `wallet` | `wallet` |
| Catalog | `localhost:54323` | `catalog` | `catalog` |
| Audit Ledger | `localhost:54324` | `audit_ledger` | `audit_ledger` |

The local-development password defaults to `ledgermesh_dev`. These credentials are intentionally limited to the local Compose environment and must not be reused in a deployed environment.

All PostgreSQL instances start with `max_prepared_transactions=100`, enabling the prepared transactions required by the 2PC implementation.

## Volumes

Compose creates the following named volumes:

- `ledgermesh_kafka_data`
- `ledgermesh_coordinator_db_data`
- `ledgermesh_wallet_db_data`
- `ledgermesh_catalog_db_data`
- `ledgermesh_audit_ledger_db_data`

`make infra-down` preserves them. `make infra-reset` removes them.

## Configuration overrides

Defaults work without an environment file. To change ports or the local password:

```bash
cp infra/.env.example infra/.env
```

Run Compose with that file:

```bash
docker compose --env-file infra/.env -f infra/compose.yaml up --build -d --wait
```

Do not commit `infra/.env`.

## Logs and troubleshooting

Follow all logs:

```bash
make infra-logs
```

Inspect one service:

```bash
docker compose -f infra/compose.yaml logs coordinator
docker compose -f infra/compose.yaml logs kafka
```

If a host port is already in use, stop the conflicting process or override that port in `infra/.env`.

After changing `max_prepared_transactions` or database initialization settings, use `make infra-reset` so PostgreSQL starts with fresh local volumes.

## Two-machine verification

Each tester should record the following in the LM-003 pull request:

| Check | Expected result |
|---|---|
| `make infra-up` | Compose exits successfully and every container becomes healthy |
| `make infra-status` | Kafka, four databases, and four services are running/healthy |
| `make infra-smoke` | Script prints `LedgerMesh infrastructure smoke test passed.` |
| `make infra-down` | All stack containers stop cleanly |

Record the operating system, CPU architecture, Docker version, and Docker Compose version for both machines.

