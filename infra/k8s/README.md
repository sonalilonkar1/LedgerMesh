# Kubernetes deployment (LM-041)

These manifests run the whole LedgerMesh system on Kubernetes, using the
same images, commands and settings as [Docker Compose](../compose.yaml) and the
Makefile, so the application behaves the same way. They also run the processes
that Compose does not start (the Saga workers, the audit consumer and the
Kafka topic bootstrap), so a Kubernetes deployment completes both transaction
modes, two-phase commit (2PC) and Saga.

```
infra/k8s/
  base/                 kustomize base, namespace "ledgermesh"
    config.yaml         ConfigMaps ledgermesh-config and kafka-config
    kafka.yaml          Kafka (KRaft, 1 node) StatefulSet + Services
    kafka-topics-job.yaml  Job that creates the 15 topics
    postgres.yaml       4 PostgreSQL StatefulSets + Services
    apis.yaml           4 API Deployments + Services
    workers.yaml        3 Saga workers + audit consumer Deployments
  overlays/kind/        local images (:local), generated Secret, LEDGERMESH_ENV=local
  overlays/ghcr/        published GHCR images by commit SHA (amd64 images: render-only on arm64)
  kind-up.sh  smoke.sh  kind-down.sh
```

## Prerequisites

- Docker, [kind](https://kind.sigs.k8s.io/) and `kubectl` (kustomize is built
  into `kubectl`; Helm is not used).
- `uv` and Python 3.12 (`make install`), because the smoke test seeds data with
  the project's own seed script.
- `git` and `openssl` (used by `kind-up.sh` for the image tag and the generated
  password), and `curl`, `python3` and `base64` on the host (used by
  `smoke.sh`).
- The smoke test reads `/tmp/lm041.kubeconfig`, which `make k8s-up` writes and
  a reboot may remove. If it is missing, run `make k8s-up` again or
  `kind export kubeconfig --name lm041 --kubeconfig /tmp/lm041.kubeconfig`.
- About 3 GB of memory free for Docker. Requests add up to about 2.3 GiB and limits
  to about 5.5 GiB.

## Deploy

```bash
make k8s-up
```

`infra/k8s/kind-up.sh`:

1. creates the kind cluster `lm041`, writing its kubeconfig to
   `/tmp/lm041.kubeconfig`. Your default kubectl context and any other kind
   cluster are never changed;
2. builds the four images with `make images-build` and loads them into kind as
   `ledgermesh-<name>:local`. Set `SKIP_IMAGE_BUILD=1` to reuse images already
   built for the current commit;
   - **Node image.** Set `KIND_NODE_IMAGE` to choose kind's node image. On the
     Apple-silicon machine used for verification, the default image of kind
     v0.33.0 (`kindest/node:v1.37.0`) failed with `exec format error`, and
     `KIND_NODE_IMAGE=kindest/node:v1.35.0` worked.
3. creates `overlays/kind/.env.secrets` with a random hex password if it does
   not exist yet;
4. applies `overlays/kind` and waits for Kafka, the databases, the topic Job,
   the APIs and the workers.

To use kubectl yourself:

```bash
export KUBECONFIG=/tmp/lm041.kubeconfig
kubectl --context kind-lm041 -n ledgermesh get pods
```

## Smoke test

```bash
make k8s-smoke
```

`infra/k8s/smoke.sh` checks that everything has rolled out and that every
database has `max_prepared_transactions` set. It then port-forwards the four
APIs to ports 18000-18003 and the wallet and catalog databases to 15432-15433.
These high ports avoid clashes with Compose. Next, it seeds the LM-006 baseline
data with `python -m infra.scripts.seed seed wallet catalog`. Seeding only
inserts missing rows, so it is safe to repeat. Finally, for each mode it:

1. reads the current balance of `user-alice` and the inventory and cost of
   `reward-coffee`;
2. sends `POST /v1/redemptions` with a unique `Idempotency-Key` and expects
   `202`;
3. polls `GET /v1/redemptions/{id}` until the status is terminal. The default
   limit is 90 s; set `SMOKE_TIMEOUT_SECONDS` to change it.
   2PC must reach `COMMITTED`; the Saga must reach `COMPLETED`;
4. checks that the balance dropped by exactly `point_cost` and the inventory by
   exactly 1. The test compares against the values read before the redemption,
   so it can run again on the same cluster;
5. polls the Audit Ledger, with the same time limit, until it holds the
   terminal event. This proves the full outbox → Kafka → audit consumer path.

Seeding retries up to three times and re-opens the database port-forwards
between attempts, because a forward opened while a database pod restarts can
refuse connections.

It ends with `LedgerMesh Kubernetes smoke test passed (2PC=COMMITTED, Saga=COMPLETED).`
On failure it prints the redemption's transitions and the logs of the
Coordinator and the workers.

**How each mode completes.**
- **2PC.** The Coordinator's recovery worker drives it to the end:
  `COORDINATOR_RECOVERY_ENABLED=true` (as in Compose), and `REQUESTED` is a
  recoverable 2PC state. This is the path a hosted deployment uses, where the
  internal `execute-2pc` route returns 404. Set `SMOKE_EXECUTE_2PC=1` to also
  call that route. It works only while `LEDGERMESH_ENV=local`, which the kind
  overlay sets.
- **Saga.** The `coordinator-saga` worker picks up the `REQUESTED` row, gets a
  quote from Catalog and sends commands to `wallet-saga` and `catalog-saga`
  over Kafka.

## What runs

| Workload | Kind | Image | Command | Probes |
|---|---|---|---|---|
| `kafka` | StatefulSet, 2Gi PVC | `apache/kafka:4.4.0` | image default | TCP 19092 (startup, readiness, liveness) |
| `coordinator-db`, `wallet-db`, `catalog-db`, `audit-ledger-db` | StatefulSet, 1Gi PVC each | `postgres:17.11-alpine` | `postgres -c max_prepared_transactions=100` | `pg_isready -U $POSTGRES_USER -d $POSTGRES_DB` |
| `kafka-topics` | Job | coordinator | `ledgermesh-topics --bootstrap-servers kafka:19092 --replication-factor 1` (retried) | - |
| `coordinator` :8000, `wallet` :8001, `catalog` :8002, `audit-ledger` :8003 | Deployment + ClusterIP Service | `ledgermesh-<name>` | image default `/app/start-service.sh`; init container `migrate` runs `alembic -c services/<svc>/alembic.ini upgrade head` | HTTP `GET /health` |
| `coordinator-saga`, `wallet-saga`, `catalog-saga` | Deployment | owning service | `python -m services.<svc>.app.saga_worker` | none (no endpoint) |
| `audit-consumer` | Deployment | audit-ledger | `python -m services.audit_ledger.app.event_worker` | none (no endpoint) |

Where each value comes from:

| Value | Source |
|---|---|
| Ports, database URLs, `KAFKA_*`, Kafka and PostgreSQL settings, `COORDINATOR_RECOVERY_*`, `WALLET_URL`, `CATALOG_URL`, `LEDGERMESH_ENV=local` | `infra/compose.yaml` |
| Worker commands | the `run-*-saga` and `run-audit-consumer` Makefile targets |
| Migration command | `infra/docker/start-service.sh:13` |
| Topic bootstrap | `libs/messaging/README.md` "Creating the topics" |

Each manifest cites the file and line it copies from.

### Startup order

Kubernetes has no `depends_on`, so the order is enforced by init containers:

- Each API's `migrate` init container retries the migration for up to about two
  minutes while its database starts. PostgreSQL DDL is transactional, so a
  retried migration is safe. A migration that keeps failing blocks the API,
  just as it stops the container in Compose.
- Each worker first waits until its own API is healthy, which happens only
  after that API's migration has run. `coordinator-saga` also waits for
  Catalog, which it asks for quotes.
- Each worker then runs the idempotent topic bootstrap, because Kafka has
  topic auto-creation disabled.
- The APIs and workers run one replica each with the `Recreate` strategy, so
  two migrations never run at once.

### Probes

- **APIs.**
  - **Startup:** `/health` every 2 s, for up to 60 s.
  - **Readiness:** every 5 s, 3 s timeout.
  - **Liveness:** every 10 s, 3 s timeout; restarts after 3 failures.
  - `/health` returns `{"status":"ok","service":...}` without touching the
    database or Kafka. **Readiness therefore means "the process is serving",
    not "its dependencies are up".** A database outage shows up as request
    errors, not as an unready pod. Changing that would change application code,
    which LM-041 does not do.
- **Kafka:** TCP checks on 19092. The Compose check (`kafka-topics.sh --list`)
  starts a JVM on every probe, which is too expensive for a laptop cluster; use
  it by hand:
  ```bash
  kubectl --context kind-lm041 -n ledgermesh exec kafka-0 -- \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
  ```
- **PostgreSQL:** `pg_isready`, the same check as Compose.
- **Workers:** no probes, because they expose no endpoint. They exit on an
  unrecoverable error and on SIGTERM, and Kubernetes restarts them.

## Configuration and secrets

- **Non-secret settings** live in the ConfigMaps `ledgermesh-config` (shared)
  and `kafka-config` (broker).
  - The base does **not** set `LEDGERMESH_ENV`, so the Coordinator's internal
    routes return 404, as in a hosted environment.
  - The kind overlay sets `LEDGERMESH_ENV=local`, as Compose does.
  - The kind overlay also sets `KAFKA_HEAP_OPTS=-Xmx512m -Xms512m`. This is
    laptop tuning, not a Compose setting.
- **The database password** comes from the Secret `ledgermesh-db`, which
  kustomize's `secretGenerator` builds from `overlays/<overlay>/.env.secrets`.
  - `.env.secrets` is gitignored by the root `.gitignore` (`.env.*`). Only
    `.env.secrets.example` is committed, and the manifests never contain a
    password.
  - Each pod builds its `DATABASE_URL` from `$(POSTGRES_PASSWORD)`. Use hex
    characters only, because the value is placed in the URL without escaping.
  - All four databases share one password, as in Compose.
- To rotate the password on a fresh cluster, edit `.env.secrets` and re-apply.
  The generated Secret name changes, so the pods restart. An existing
  PostgreSQL data volume keeps the password it was initialised with, so on a
  running cluster also run `ALTER USER`, or recreate the cluster.

## Security

- **Application pods.**
  - Run as UID/GID 10001, the image's user, with `runAsNonRoot`.
  - Use a read-only root filesystem with an `emptyDir` at `/tmp`.
  - Drop all capabilities, disallow privilege escalation, and use the
    `RuntimeDefault` seccomp profile.
- **PostgreSQL** runs as UID 70 (`postgres` in the alpine image).
- **Kafka** runs as UID 1000 (`appuser` in `apache/kafka`).
- **All pods** set `automountServiceAccountToken: false` and
  `enableServiceLinks: false`. The second is required: a Service named `kafka`
  would otherwise inject `KAFKA_PORT=tcp://...`, which the Kafka image reads as
  broker configuration.

## Using the published GHCR images

The `ghcr` overlay uses `ghcr.io/sonalilonkar1/ledgermesh-<name>` and leaves
`LEDGERMESH_ENV` unset.

1. Set `newTag` in `overlays/ghcr/kustomization.yaml` to a published full commit
   SHA (see [IMAGES.md](../IMAGES.md)).
2. Create `overlays/ghcr/.env.secrets` from `overlays/ghcr/.env.secrets.example`.
3. Delete the finished topic Job, then apply the overlay:

   ```bash
   kubectl -n ledgermesh delete job kafka-topics --ignore-not-found
   kubectl apply -k infra/k8s/overlays/ghcr
   ```

   A Job's pod template is immutable, and a new `newTag` changes its image, so
   without the delete every upgrade after the first is rejected for
   `job/kafka-topics`. The bootstrap is idempotent, so re-running it is safe
   (`kind-up.sh` does the same).

The published images are **linux/amd64 only**, so they do not run on an arm64
kind node, such as one on Apple silicon. Use the kind overlay there.

## Teardown

```bash
make k8s-down
```

This deletes only the kind cluster `lm041`, with its volumes, and removes
`/tmp/lm041.kubeconfig`. The locally built `ledgermesh-*` images stay in Docker.
Remove them with `docker image rm` if you need the space.

## Differences from Docker Compose

| Topic | Compose | Kubernetes |
|---|---|---|
| Saga workers, audit consumer | not started | Deployments; Saga and audit events work end to end |
| Kafka topics | not created (auto-create is off) | `kafka-topics` Job and an init container in each worker |
| Migrations | in the container command before uvicorn | `migrate` init container, then the image's default command with `RUN_MIGRATIONS=false` |
| Kafka controller voter | `1@kafka:29093` | `1@localhost:29093`, because `kafka` resolves to the Service |
| Kafka external listener | host port 29092 | `localhost:29092` through `kubectl port-forward pod/kafka-0 29092` |
| Kafka health check | `kafka-topics.sh --list` | TCP 19092 (the Compose check is documented above for manual use) |
| Host ports | 8000-8003, 54321-54324 | none; `kubectl port-forward` (the smoke test uses 18000-18003) |
| Password | `${POSTGRES_PASSWORD:-ledgermesh_dev}` | generated Secret, no default |
| Kafka log dir | `/var/lib/kafka/data` (volume root) | `/var/lib/kafka/data/logs` (subdirectory of the PVC) |
| Kafka heap | image default | 512 MiB in the kind overlay |

## Verified on kind

The following was checked on kind v0.33.0 with node image `kindest/node:v1.35.0`,
on an arm64 Mac with Docker limited to 7.6 GiB. Deploy, smoke, the readiness and
liveness test and a repeat smoke were re-run on a fresh cluster by following only
this README; the Kafka/PostgreSQL pod-deletion and port-conflict checks come from
earlier development runs.

- **Deploy.** `make k8s-up` brings every pod to Ready with 0 restarts in about
  2 minutes, including the image build. The kind node then uses about 2.1 GiB.
- **Smoke test.** On a fresh cluster, 2PC reaches `COMMITTED` and Saga reaches
  `COMPLETED`. Each run takes 100 points and 1 unit, and the Audit Ledger holds
  `redemption.committed` and the Saga transition events.
- **Server-side validation.** `kubectl apply -k overlays/kind --dry-run=server`
  accepts all 32 objects.
- **Readiness and liveness (Coordinator API).** Its uvicorn process was frozen with
  `SIGSTOP`, sent from the node.
  - About 20 s later, readiness failed and the pod left the Service endpoints.
  - About 60 s later, liveness restarted the container. It became Ready again
    and rejoined the endpoints.
- **Kafka pod deleted.** The pod was back and ready in about 10 s on the same
  PVC, with all 15 topics under `/var/lib/kafka/data/logs`. The workers
  reconnected without restarting.
- **Repeat runs.** A second `make k8s-smoke` on the same cluster passes, also
  after `job/kafka-topics` was deleted, and leaves no `kubectl port-forward`
  running. With ports 15432 already taken, the smoke stops with kubectl's
  "address already in use" error instead of using the other listener.
- **PostgreSQL pod deleted.** The data survived: balances carried on from
  their previous values.

**Recovery after a dependency restart is slower than a normal run.** After a
PostgreSQL pod restart, a worker's next database connection waited about
2 minutes until it timed out, because `DATABASE_URL` sets no `connect_timeout`,
as in Compose. Saga steps owned by that worker paused for that time, then
completed. Use `SMOKE_TIMEOUT_SECONDS=240` when running the smoke right after a
fault test.

## Limits

- **Single node, single replica everywhere.** There is no Kafka replication or
  PostgreSQL high availability, and a node loss loses availability until the
  pods are rescheduled. The data survives on the PVCs while the cluster exists.
- **Readiness is process-level only** (see Probes). Only the Coordinator API's
  readiness and liveness were exercised by fault; the Kafka TCP and PostgreSQL
  `pg_isready` probes were not observed failing. The workers have no liveness
  probe, so a worker stuck on a database connection is not restarted.
- **Kafka storage.**
  - The PVC is mounted at `/var/lib/kafka/data`, as in Compose. The
    `apache/kafka` image declares that path as a `VOLUME`, so mounting the PVC
    anywhere else silently loses every topic when the pod restarts.
  - `KAFKA_LOG_DIRS` is the subdirectory `/var/lib/kafka/data/logs`, unlike
    Compose, which uses the volume root. Kafka 4.4 refuses to start if its log
    directory holds anything that is not a partition directory, and storage
    classes backed by ext4 block volumes (the usual cloud defaults) create
    `lost+found` at the volume root. PostgreSQL uses a `PGDATA` subdirectory
    for the same reason.
- **Known issue: one audit event was lost after a broker restart.** This was
  seen once, during fault testing, and was not reproduced.
  - Right after the Kafka pod was deleted, the Coordinator's first send of a
    2PC terminal event failed with `OutOfOrderSequenceNumber`.
  - The next attempt marked the outbox row as published.
  - The event was never in `redemption.events`, so the Audit Ledger never
    recorded it. The business outcome (`COMMITTED`) was correct.
  - The cause is believed to be in the outbox publisher or the Kafka client
    rather than the manifests, but this is not yet proven. It needs a follow-up
    investigation. The normal smoke runs do not hit it.
- **No Ingress or NetworkPolicy.** Services are ClusterIP only and are reached
  with port-forward.
- **The smoke test assumes it is the only writer** to `user-alice` and
  `reward-coffee` while it runs, because it compares balances before and after.

## Troubleshooting

- **A pod is stuck in `Init`.** Run
  `kubectl --context kind-lm041 -n ledgermesh logs <pod> -c migrate`, or use
  `-c wait-api` or `-c ensure-topics`.
- **The topic Job failed.** Look at
  `kubectl --context kind-lm041 -n ledgermesh logs job/kafka-topics`.
  Re-running `make k8s-up` deletes and recreates the Job.
- **A redemption stays non-terminal.** The smoke test prints the transitions and
  the worker logs. Check `coordinator-saga` first: it runs the Saga starter and
  the Coordinator's outbox publisher.
- **`kubectl apply` rejects a StatefulSet with "spec: Forbidden".** A
  StatefulSet's `serviceName` and `volumeClaimTemplates` cannot change in place.
  A cluster created before the databases got their `<name>-headless` governing
  Services needs `make k8s-down && make k8s-up` (or, outside kind,
  `kubectl -n ledgermesh delete statefulset <name> --cascade=orphan` before the
  apply; the PVCs and data are kept).
