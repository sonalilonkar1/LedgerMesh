# Local Docker Compose infrastructure

LM-003 provides a reproducible local stack containing:

- One Apache Kafka broker in single-node KRaft mode
- Four isolated PostgreSQL instances
- Coordinator, Wallet, Catalog, and Audit Ledger FastAPI services
- Health checks and dependency-aware startup ordering
- Named volumes for broker and database data

The messaging library's test-only environment remains separate at
`libs/messaging/docker-compose.test.yml`.

The same `infra/Dockerfile.service` also builds the production images that CI
publishes to GHCR. See [Service images (LM-047)](IMAGES.md).

The hosted staging environment (LM-048) runs those images with its own Compose
file, `infra/staging/compose.yaml`: no host ports except the Caddy HTTPS
gateway, one password per database, data on a persistent disk, and the Saga
workers and audit consumer as their own containers. See the
[staging runbook](../docs/deployment/staging.md) and
[ADR-002](../docs/architecture/ADR-002-hosted-staging.md).

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

### Prepared-transaction operations

PostgreSQL keeps a transaction after its client disconnects once it has run
`PREPARE TRANSACTION`. This durability is required by 2PC, but it also means an
unresolved transaction can retain row locks and consume one of the 100 prepared
transaction slots.

List unresolved transactions across the service databases:

```bash
make db-prepared-list
```

The underlying diagnostic query is:

```sql
SELECT gid, prepared, owner, database
FROM pg_prepared_xacts
WHERE database = current_database()
ORDER BY prepared, gid;
```

For local or test recovery only, roll back every unresolved transaction. Stop
the Coordinator and participant services first so that cleanup cannot race an
active 2PC workflow:

```bash
docker compose -f infra/compose.yaml stop coordinator wallet catalog audit-ledger
LEDGERMESH_ENV=local make db-prepared-clean
```

The cleanup command deliberately refuses to run when `LEDGERMESH_ENV` is unset
or is not `local`/`test`. In staging or production, inspect the transaction ID,
Coordinator decision and participant logs before issuing a specific `COMMIT
PREPARED` or `ROLLBACK PREPARED`; bulk cleanup could violate the durable 2PC
decision. Prepared transactions count against `max_prepared_transactions` and
can hold locks indefinitely, so alerting on their age and count is required in
the hosted environment. In staging, `infra/staging/stack.sh verify
prepared-transactions` reports both and fails when the oldest is older than a
threshold; a paging alert is still a follow-up.

`max_prepared_transactions` is a server-wide capacity, is allocated in shared
memory at PostgreSQL startup, and therefore requires a server restart when
changed. The value should be at least the maximum number of participant
transactions that can simultaneously remain prepared, with operational
headroom; reaching the limit makes new `PREPARE TRANSACTION` commands fail.

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

If a health check returns an unexpected status or identifies the wrong service, another application may already be using that host port. Override the port in `infra/.env`, or provide the same override to both commands:

```bash
WALLET_PORT=18001 make infra-up
WALLET_PORT=18001 make infra-smoke
```

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

