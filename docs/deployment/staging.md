# Hosted staging runbook (LM-048)

Staging is a single Google Compute Engine VM that runs the LedgerMesh images with Docker Compose behind Caddy. The decision, alternatives and cost estimate are in [ADR-002](../architecture/ADR-002-hosted-staging.md). ADR-001 wins on any conflict.

| | |
|---|---|
| **URL** | **Pending provisioning.** It will be `https://<static-ip-with-dashes>.sslip.io`; `provision.sh up` and `provision.sh status` print it. Record it here, in the README and in the GitHub `staging` environment's `STAGING_URL` variable. |
| Project, region, zone | `deven-cloud-lab`, `us-west1`, `us-west1-b` |
| VM | `ledgermesh-staging`, `e2-standard-4`, Debian 12, `linux/amd64` |
| Data | Disk `ledgermesh-data`, mounted at `/var/lib/ledgermesh`, snapshot daily, 7-day retention |
| Files | `infra/staging/` (Compose file, Caddyfile and scripts) |

Automatic deployment from `main` is LM-049 and the hosted smoke suite is LM-050. This runbook covers provisioning and operating the environment by hand.

## Contents

1. [Architecture](#1-architecture)
2. [Public API](#2-public-api)
3. [Provision](#3-provision)
4. [Deploy a commit by hand](#4-deploy-a-commit-by-hand)
5. [Verify](#5-verify)
6. [Stop, start and cost](#6-stop-start-and-cost)
7. [Inspect logs and failures](#7-inspect-logs-and-failures)
8. [Backup and reset](#8-backup-and-reset)
9. [Disaster recovery](#9-disaster-recovery)
10. [Secrets and rotation](#10-secrets-and-rotation)
11. [Admin access](#11-admin-access)
12. [GitHub `staging` environment](#12-github-staging-environment)
13. [Teardown](#13-teardown)
14. [Local validation](#14-local-validation)
15. [Optional web UI](#15-optional-web-ui)
16. [Known limitations](#16-known-limitations)

## 1. Architecture

```text
Internet ──TCP 80/443──▶ caddy ──edge──▶ coordinator :8000, wallet :8001, catalog :8002, audit-ledger :8003
                                                     │
                                         backend (internal: no egress, no ports)
                                                     │
              kafka :19092 ◀── coordinator-saga, wallet-saga, catalog-saga, audit-consumer
              coordinator-db, wallet-db, catalog-db, audit-ledger-db (PostgreSQL 17, :5432)
```

- **Firewall:** only TCP 80 and 443 are open to the internet. TCP 22 is open only to Google's IAP range `35.235.240.0/20`. The VM uses its own VPC, `ledgermesh-staging`, not the `default` network.
- **Containers** (`infra/staging/compose.yaml`): Kafka, four PostgreSQL databases, four APIs, four Kafka consumers and Caddy. Only Caddy has host ports. Every container has `restart: unless-stopped`, CPU and memory limits, and `json-file` logs capped at 5 × 10 MB.
- **Images:** `ghcr.io/sonalilonkar1/ledgermesh-<service>:<full commit sha>` ([IMAGES.md](../../infra/IMAGES.md)). The four consumers run the same images with a different command (`python -m services.<service>.app.saga_worker`, `python -m services.audit_ledger.app.event_worker`).
- **Data** on the persistent disk:

  | Path under `/var/lib/ledgermesh` | Contents |
  |---|---|
  | `kafka/` | Kafka log directory (owned by UID 1000, the `apache/kafka` user) |
  | `postgres/<service>/` | One PostgreSQL data directory per service |
  | `caddy/data`, `caddy/config` | Certificates and the ACME account, kept so restarts do not request new certificates |
  | `releases/`, `deploy-logs/` | Reserved for LM-049 release records |

- **Settings:** `LEDGERMESH_ENV=staging`, so the Coordinator's `/internal/*` routes return 404 at the application too. `RUN_MIGRATIONS=false`; migrations run as a separate step. Kafka has `auto.create.topics.enable=false`. Every database runs with `max_prepared_transactions=100`.

## 2. Public API

Caddy (`infra/staging/Caddyfile`) forwards only these routes. Everything else, including `/internal/*`, FastAPI `/docs` and `/openapi.json`, and every participant write route (`deductions`, `refunds`, `prepare`, `commit`, `abort`, `reservations`, `releases`, `fulfillments`, `audit-events`), returns **404**.

| Method and path | Forwarded to |
|---|---|
| `GET /health` | Coordinator `/health` |
| `POST /v1/redemptions` | Coordinator |
| `GET /v1/redemptions/{transaction_id}` | Coordinator |
| `GET /health/coordinator`, `/health/wallet`, `/health/catalog`, `/health/audit-ledger` | That service's `/health` |
| `GET /wallet/v1/wallets/{user_id}` | Wallet `/v1/wallets/{user_id}` |
| `GET /catalog/v1/rewards`, `GET /catalog/v1/rewards/{reward_id}` | Catalog `/v1/rewards…` |
| `GET /audit/v1/transactions/{transaction_id}/events` | Audit Ledger `/v1/transactions/{transaction_id}/events` |

`transaction_id` must be a hyphenated UUID; `user_id` and `reward_id` are up to 128 characters of `A-Z a-z 0-9 . _ -` and may not start with a dot. HTTP redirects to HTTPS. Requests over 64 KB are rejected.

2PC cannot be started through this API: `POST /v1/redemptions` only records the request, and 2PC runs through the Coordinator's internal `execute-2pc` route, which is not public and is disabled outside `local` and `test`.

## 3. Provision

`infra/staging/provision.sh` creates everything with `gcloud`. It is idempotent: each resource is looked up and created only if missing, so re-running it is safe and also refreshes the VM's startup script and metadata. Only a `NOT_FOUND` / "was not found" answer counts as missing: any other lookup error (permission denied, API disabled, network) stops the script, so a re-run never adds a second password version or a duplicate budget because a list call failed.

**Who runs it:** a project Owner (or someone with Compute Admin, Security Admin, Secret Manager Admin, Service Account Admin and Billing Account Costs Manager on the billing account). Billing must be enabled on the project.

```bash
gcloud auth login
gcloud config set project deven-cloud-lab

# 1. See every command first. Nothing runs.
make staging-provision-dry-run
bash infra/staging/provision.sh up --dry-run \
  --billing-account <BILLING_ACCOUNT_ID> --acme-email <contact email> \
  --admin-member user:<teammate email> --admin-member user:<teammate email>

# 2. Create it.
bash infra/staging/provision.sh up \
  --billing-account <BILLING_ACCOUNT_ID> --acme-email <contact email> \
  --admin-member user:<teammate email> --admin-member user:<teammate email>
```

`up` does, in order:

1. Enables the Compute, IAP, Secret Manager, Billing Budgets, Resource Manager, OS Login and IAM APIs.
2. Creates the VPC `ledgermesh-staging` and subnet `ledgermesh-staging-us-west1` (`10.20.0.0/24`).
3. Creates two firewall rules for the network tag `ledgermesh-staging`: `allow-web` (TCP 80 and 443 from anywhere) and `allow-iap-ssh` (TCP 22 from `35.235.240.0/20`).
4. Reserves the static IPv4 `ledgermesh-staging-ip`.
5. Creates the VM service account `ledgermesh-staging-vm`, with only `roles/logging.logWriter` at project level.
6. Creates the four secrets `ledgermesh-staging-{coordinator,wallet,catalog,audit-ledger}-db-password`. If a secret has no enabled version, it adds a random 64-hex-character value (`openssl rand -hex 32`), piped straight to Secret Manager and never printed. The VM service account gets `secretAccessor` on each of these four secrets only.
7. Creates the daily snapshot schedule `ledgermesh-data-daily` (7 days, snapshots kept if the disk is deleted) and the 30 GB `pd-balanced` disk `ledgermesh-data`.
8. Creates the VM: Shielded VM, the VM service account, the data disk attached with `auto-delete=no`, OS Login on, project SSH keys blocked, `vm-startup.sh` as the startup script, and the metadata `ledgermesh-site-address=<ip-with-dashes>.sslip.io` and `ledgermesh-acme-email`.
9. Grants each `--admin-member` IAP tunnelling (only to port 22), `compute.viewer`, OS Login admin on the VM, and `serviceAccountUser` on the VM service account (OS Login requires it for a VM that runs as a service account).
10. Creates the billing budget `ledgermesh-staging` (default 50 USD a month; `--budget-amount` changes it) with alerts at 50%, 90% and 100%, filtered to this project. Budget alerts are emailed to the billing account's admins. A budget does **not** stop spending.

It prints the staging URL at the end.

**Startup script.** On every boot, `vm-startup.sh` installs Docker Engine and the Compose plugin from Docker's apt repository if they are missing, formats the data disk only if it has no filesystem, mounts it at `/var/lib/ledgermesh` (fstab by UUID), makes `docker.service` require that mount (so containers never start on an empty boot-disk directory), creates the data directories, and enables Docker log rotation. Follow it with:

```bash
gcloud compute ssh ledgermesh-staging --zone us-west1-b --tunnel-through-iap \
  --command 'sudo journalctl -u google-startup-scripts.service --no-pager | tail -50'
```

It ends with `ledgermesh-startup: ready: docker …`.

**For LM-049**, run once afterwards: `bash infra/staging/bootstrap-wif.sh` (use `--dry-run` first). See [§12](#12-github-staging-environment).

## 4. Deploy a commit by hand

LM-049 automates this. By hand, pick a commit on `main` whose **Service images** workflow published all four images (`infra/IMAGES.md`).

```bash
SHA=<full 40-character commit sha>
ZONE=us-west1-b

# Copy this checkout's infra/staging to the VM.
gcloud compute scp --recurse --tunnel-through-iap --zone "$ZONE" \
  infra/staging ledgermesh-staging:/tmp/ledgermesh-staging
gcloud compute ssh ledgermesh-staging --zone "$ZONE" --tunnel-through-iap
```

On the VM:

```bash
sudo install -d -m 0755 /opt/ledgermesh
sudo rm -rf /opt/ledgermesh/staging
sudo mv /tmp/ledgermesh-staging /opt/ledgermesh/staging

# Secrets from Secret Manager -> /etc/ledgermesh/staging.env (root only, 0600).
sudo bash /opt/ledgermesh/staging/render-env.sh

sudo bash /opt/ledgermesh/staging/stack.sh --sha "$SHA" up
```

`stack.sh up`:

1. starts Kafka and the four databases and waits until they are healthy;
2. checks `max_prepared_transactions > 0` on every database;
3. runs `alembic upgrade head` for each service in a one-off container of that service's image, with that service's own `DATABASE_URL`;
4. runs the LM-018 topic bootstrap (`ledgermesh-topics --bootstrap-servers kafka:19092 --replication-factor 1`, the messaging library's console script for `ledgermesh_messaging.topics`) and checks the 15 topics;
5. starts the APIs and Caddy with `docker compose up --wait` (bounded by `--wait-timeout`, default 300 s), then the four workers (Compose's `--wait` rejects containers whose health check is disabled, so they are checked by `verify workers` instead);
6. runs every check in [§5](#5-verify).

It stops at the first failure. Running it again for the same SHA is safe: migrations and the topic bootstrap are no-ops, and Compose leaves unchanged containers alone.

Migrations must stay backward compatible with the previous image (add first, remove in a later release), because the previous image cannot run against, or migrate down from, a schema revision it does not know.

## 5. Verify

On the VM:

```bash
S="sudo bash /opt/ledgermesh/staging/stack.sh"
$S verify all            # everything below
$S verify health         # Docker health of every container, then HTTPS /health and /health/<service> through Caddy
$S verify ports          # only caddy publishes host ports, and only 80/tcp and 443/tcp
$S verify topics         # the 15 LM-018 topics with the right partition counts, no others; auto-create off
$S verify prepared-transactions  # max_prepared_transactions > 0; count and age of pg_prepared_xacts
$S verify db-isolation   # each service's credentials connect to its own database and are rejected by the other three
$S verify workers        # the four consumers stay running with no restarts for 20 s
```

From your laptop:

```bash
URL=https://<ip-with-dashes>.sslip.io
curl -fsS "$URL/health"
for s in coordinator wallet catalog audit-ledger; do curl -fsS "$URL/health/$s"; echo; done
curl -s -o /dev/null -w '%{http_code}\n' "$URL/internal/v1/2pc/in-doubt"   # 404
curl -s -o /dev/null -w '%{http_code}\n' -X POST "$URL/wallet/v1/wallets/x/deductions"  # 404

# Nothing but 80 and 443 answers from the internet (22 is IAP-only).
IP=<static ip>
for p in 22 5432 8000 8001 8002 8003 9092 19092 29092 2019; do
  nc -z -w 3 "$IP" "$p" && echo "OPEN $p (unexpected)" || echo "closed $p"
done
```

## 6. Stop, start and cost

```bash
bash infra/staging/provision.sh stop    # VM stopped; disks, snapshots and the IP are still billed
bash infra/staging/provision.sh start
bash infra/staging/provision.sh status  # state, IP, URL
```

After a start, the startup script remounts the data disk, Docker starts, and every container comes back because of `restart: unless-stopped`. Check with `stack.sh verify all`.

Estimated cost (ADR-002 §3, estimates to confirm in the pricing calculator): about **$108 a month** running continuously, about **$13 to $15 a month** while stopped. Stop the VM whenever it is not needed for a demo or a test run.

## 7. Inspect logs and failures

Everything below runs on the VM (`gcloud compute ssh ledgermesh-staging --zone us-west1-b --tunnel-through-iap`).

```bash
S="sudo bash /opt/ledgermesh/staging/stack.sh"
DC="sudo docker compose -p ledgermesh-staging -f /opt/ledgermesh/staging/compose.yaml --env-file /etc/ledgermesh/staging.env"

$S ps                                 # state, health and restarts of every container
$S logs --tail 200 coordinator        # one service (also: wallet-saga, kafka, caddy, ...)
$S logs --follow coordinator-saga
$S collect-logs /tmp/ledgermesh-logs.tgz   # every service's logs with passwords redacted (checked)
```

To download the bundle: `gcloud compute scp --tunnel-through-iap --zone us-west1-b ledgermesh-staging:/tmp/ledgermesh-logs.tgz .`

Service logs are structured JSON (LM-032, [docs/logging.md](../logging.md)); filter one redemption with `grep <transaction_id>`. Each service logs `starting <service> revision=<sha>` at startup, and `docker inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' <container>` shows the running revision.

**Kafka**

```bash
$DC exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe
$DC exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --all-groups   # lag per consumer group
$DC exec coordinator python -m ledgermesh_messaging.dlq wallet.commands.dlq \
  --bootstrap-servers kafka:19092 --limit 20                                     # dead letters (LM-024)
```

**PostgreSQL** (connects over the container's local socket; no password is typed)

```bash
$DC exec wallet-db psql -U wallet -d wallet
$DC exec coordinator-db psql -U coordinator -d coordinator -c \
  "SELECT gid, prepared, owner, database FROM pg_prepared_xacts ORDER BY prepared"
```

Before resolving a prepared transaction by hand, follow `infra/README.md` ("Prepared-transaction operations"): never bulk-clean in staging.

**Common failures**

| Symptom | Look at |
|---|---|
| `stack.sh up` stops at "starting the data tier" | `$S logs kafka` (a changed `CLUSTER_ID` makes Kafka refuse its existing data); `$S logs wallet-db`; is `/var/lib/ledgermesh` mounted (`mountpoint /var/lib/ledgermesh`)? |
| A migration fails | The output of that `alembic` run; the schema is left at the last successful revision |
| `verify health` waits on HTTPS | `$S logs caddy`: certificate issuance needs ports 80 and 443 reachable from the internet and the sslip.io name resolving to the VM's IP. If Let's Encrypt fails, Caddy retries and can fall back to its other default issuer. |
| A worker keeps restarting | `$S logs <worker>`: usually the database or Kafka was unavailable |
| Redemptions stay `REQUESTED` | `coordinator-saga` logs and consumer-group lag |

## 8. Backup and reset

**Snapshots.** The data disk is snapshotted daily (7 days kept). On demand:

```bash
gcloud compute snapshots create ledgermesh-data-manual-$(date -u +%Y%m%d%H%M) \
  --source-disk ledgermesh-data --source-disk-zone us-west1-b
```

For a consistent snapshot, stop the stack first (`$S down`), or accept a crash-consistent copy (PostgreSQL and Kafka recover from one).

**Logical backup** of one database, on the VM:

```bash
$DC exec -T wallet-db pg_dump -U wallet -d wallet --format custom > /tmp/wallet.dump
```

**Reset staging data** (destructive: deletes every redemption, balance, reward and Kafka message):

```bash
$S down
sudo sh -c 'rm -rf /var/lib/ledgermesh/kafka/* /var/lib/ledgermesh/postgres/*/*'
sudo bash /opt/ledgermesh/staging/stack.sh --sha <sha> up    # recreates schemas and topics
```

Keep `caddy/` so the certificate is not requested again. `infra/scripts/seed.py` refuses to run outside `local` and `test`, so staging is not seeded with the local baseline data.

## 9. Disaster recovery

| Failure | Recovery |
|---|---|
| Container crash | Docker restarts it (`unless-stopped`). |
| VM reboot or stop/start | The startup script remounts the disk; Docker starts after the mount and restarts every container. Verify with `stack.sh verify all`. |
| VM deleted or broken | The data disk survives (`auto-delete=no`). Re-run `provision.sh up` (it recreates the VM and attaches the existing disk; the startup script finds the filesystem and does not format it), then deploy the last known good SHA as in §4. |
| Data disk lost or corrupted | Create a disk from the newest snapshot: `gcloud compute disks create ledgermesh-data --zone us-west1-b --source-snapshot <snapshot> --type pd-balanced`, then re-run `provision.sh up` and deploy. Data written after that snapshot is lost. |
| Wrong release deployed | Deploy the previous SHA as in §4 (LM-049 automates rollback). Migrations are not rolled back. |

## 10. Secrets and rotation

- The four database passwords live only in Secret Manager. `render-env.sh` reads them at deploy time with the VM's service account and writes `/etc/ledgermesh/staging.env`, mode `0600`, owned by root. It never prints them. The file is never committed, copied into a log bundle, or baked into an image.
- Docker passes the passwords to containers as environment variables, so anyone with root or Docker access on the VM can read them: the named admins and, through OS Login admin, the deploy service account used by the GitHub workflows (§12).
- There is no GHCR token: the packages are public. If they become private, add a secret `ledgermesh-staging-ghcr-read-token` holding a token with only `read:packages`, grant the VM service account access to it, and run `docker login ghcr.io` from it before pulling (see `infra/IMAGES.md`, "Pulling from a staging host").

**Rotate a database password** (for example after it was printed by mistake):

```bash
NEW="$(openssl rand -hex 32)"
printf '%s' "$NEW" | gcloud secrets versions add ledgermesh-staging-wallet-db-password --data-file -
```

Then on the VM, change the role's password inside that database and redeploy, because `POSTGRES_PASSWORD` is only applied when the data directory is first created. `\password` prompts for the value without echoing it or saving it in shell history:

```bash
$DC exec wallet-db psql -U wallet -d wallet      # then: \password wallet
sudo bash /opt/ledgermesh/staging/render-env.sh
sudo bash /opt/ledgermesh/staging/stack.sh --sha <running sha> up
```

Between the `ALTER` and the redeploy the running containers still hold the old password, so do this while staging is idle.

Disable the old secret version afterwards (`gcloud secrets versions disable <n> --secret …`).

## 11. Admin access

- Admins are the `--admin-member` principals given to `provision.sh up`. Re-run it with an extra `--admin-member` to add one. They get:
  - `roles/iap.tunnelResourceAccessor` with the condition `destination.port == 22`;
  - `roles/compute.viewer`;
  - `roles/compute.osAdminLogin` on the VM (sudo through OS Login);
  - `roles/iam.serviceAccountUser` on `ledgermesh-staging-vm`.
- SSH is only through IAP: `gcloud compute ssh ledgermesh-staging --zone us-west1-b --tunnel-through-iap`. There are no SSH keys in project metadata (`block-project-ssh-keys=TRUE`) and port 22 is closed to the internet.
- To remove someone, delete those bindings in IAM.

## 12. GitHub `staging` environment

Creating an environment needs repository admin. **Sonali** creates it under **Settings → Environments → New environment** before the first LM-049 workflow run (otherwise GitHub creates it automatically with no branch rule):

| Setting | Value |
|---|---|
| Name | `staging` |
| Deployment branches and tags | **Selected branches and tags**, one rule: `main` |
| Required reviewers | None (deploys from `main` are automatic) |
| Variable `STAGING_URL` | `https://<ip-with-dashes>.sslip.io` (from `provision.sh status`) |
| Variable `GCP_PROJECT_ID` | `deven-cloud-lab` |
| Variable `GCP_ZONE` | `us-west1-b` |
| Variable `STAGING_VM_NAME` | `ledgermesh-staging` |
| Variable `GCP_WORKLOAD_IDENTITY_PROVIDER` | `projects/<project number>/locations/global/workloadIdentityPools/github/providers/ledgermesh-github` (printed by `bootstrap-wif.sh`) |
| Variable `GCP_DEPLOY_SERVICE_ACCOUNT` | `ledgermesh-deployer@deven-cloud-lab.iam.gserviceaccount.com` |
| Secrets | None. The workflow authenticates with OIDC; database passwords stay in Secret Manager. Add `GHCR_READ_TOKEN` only if the packages become private. |

`bootstrap-wif.sh` creates the Workload Identity pool `github` and provider `ledgermesh-github`. The provider only accepts GitHub tokens whose claims match exactly:

```text
assertion.repository=='sonalilonkar1/LedgerMesh' && assertion.repository_id=='1370369282' && assertion.ref=='refs/heads/main' && assertion.environment=='staging'
```

so workflows from forks, pull requests and other branches cannot obtain Google credentials, and neither can a job on `main` that does not run in the `staging` environment (a scheduled job, an `issue_comment` or another `workflow_run` workflow). Every job that needs Google credentials declares `environment: staging`, which puts the `environment` claim into its token; the environment's `main`-only branch rule therefore also gates Google access.

The deploy service account `ledgermesh-deployer` has no Secret Manager role. Its permissions are IAP SSH to port 22 on the VM, OS Login **admin** on the VM, Compute metadata reads, and acting as the VM's service account. OS Login admin means root (sudo) on the VM, and root can use the VM's service account, which can read the four database password secrets. **Treat the deploy credentials, and anyone who can change a workflow on `main` that runs in `staging`, as able to read the staging database passwords.**

## 13. Teardown

```bash
bash infra/staging/provision.sh teardown --dry-run --confirm deven-cloud-lab \
  --billing-account <BILLING_ACCOUNT_ID> --admin-member user:<email> ...
bash infra/staging/provision.sh teardown --confirm deven-cloud-lab \
  --billing-account <BILLING_ACCOUNT_ID> --admin-member user:<email> ...
bash infra/staging/bootstrap-wif.sh teardown --confirm deven-cloud-lab
```

Teardown is safe to re-run: what is already gone, including an IAM binding someone removed by hand, is skipped. Any lookup error other than "not found" stops it with an error instead of reporting success while resources are still billed. It deletes the VM, the data disk **and all its data**, the snapshots, the snapshot schedule, the IP, firewall rules, subnet, VPC, secrets, the VM service account, the admins' project-level bindings and the budget. Take a snapshot or `pg_dump` first if anything should be kept. Deleted Workload Identity pools stay soft-deleted for 30 days. Then remove `STAGING_URL` from the GitHub environment and update this page.

## 14. Local validation

The same Compose file runs on a laptop with images built from the checkout (native architecture), placeholder passwords and Caddy's local CA:

```bash
cp infra/staging/.env.local.example infra/staging/.env.local   # gitignored
make staging-images-local    # build the four images, tag them ghcr.io/sonalilonkar1/ledgermesh-*:<HEAD sha>
make staging-local-up        # render artifacts/staging/staging.env, then stack.sh up
make staging-local-verify
curl --cacert artifacts/staging/data/caddy/data/caddy/pki/authorities/local/root.crt https://localhost/health
make staging-local-down      # data stays in artifacts/staging/data
make staging-local-reset     # deletes it
```

It needs host ports 80 and 443 free (`LEDGERMESH_HTTP_PORT` and `LEDGERMESH_HTTPS_PORT` override them, and `stack.sh --https-port` must match). Differences from the VM: images are not `linux/amd64` on Apple silicon, TLS uses Caddy's local CA, secrets come from a file instead of Secret Manager, and there is no cloud firewall or IAP.

## 15. Optional web UI

The web UI (`services/web`, PR #97) is not merged and staging does not depend on it. To add it after it merges:

1. Add a `web` service to `infra/staging/compose.yaml` with `profiles: ["web"]`, its published image, the `edge` network only, and the same `restart`, `logging`, `deploy.resources` and `healthcheck` settings as the APIs. It must call the public API paths, not internal service URLs.
2. Create `infra/staging/caddy.d/web.caddy` with a `handle` block for its paths, mount `./caddy.d:/etc/caddy/caddy.d:ro` into the `caddy` service, and uncomment `import /etc/caddy/caddy.d/*.caddy` in the Caddyfile. The import sits before the catch-all 404.
3. Start it with `COMPOSE_PROFILES=web`. Update `tests/infra/test_staging_compose.py` and `test_staging_caddyfile.py` for the new service and routes.

## 16. Known limitations

- One VM in one zone: no high availability (ADR-002 §5).
- Inside its own PostgreSQL instance each service user owns its database, because the images run migrations with the runtime `DATABASE_URL`. Cross-service isolation is by separate instances and passwords (`verify db-isolation`). A separate migration role is a follow-up.
- Prepared-transaction age and count are checked by `stack.sh verify prepared-transactions` (it fails when the oldest is older than 300 s, configurable with `--prepared-max-age`), but there is no paging alert yet (`infra/README.md` asks for one).
- Logs stay on the VM in rotated `json-file` logs; there is no central log store. `collect-logs` produces a redacted bundle for sharing.
- `sslip.io` is a third-party DNS service. If it is unavailable the URL does not resolve, although the VM keeps running; a team-owned domain removes that dependency (ADR-002 §6).
