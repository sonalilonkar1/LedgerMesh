# Hosted staging runbook (LM-048, LM-049, LM-050)

Staging is a single Google Compute Engine VM that runs the LedgerMesh images with Docker Compose behind Caddy. The decision, alternatives and cost estimate are in [ADR-002](../architecture/ADR-002-hosted-staging.md). ADR-001 wins on any conflict.

| | |
|---|---|
| **URL** | **Pending provisioning.** It will be `https://<static-ip-with-dashes>.sslip.io`; `provision.sh up` and `provision.sh status` print it. Record it here, in the README and in the GitHub `staging` environment's `STAGING_URL` variable. |
| Project, region, zone | `deven-cloud-lab`, `us-west1`, `us-west1-b` |
| VM | `ledgermesh-staging`, `e2-standard-4`, Debian 12, `linux/amd64` |
| Data | Disk `ledgermesh-data`, mounted at `/var/lib/ledgermesh`, snapshot daily, 7-day retention |
| Files | `infra/staging/` (Compose file, Caddyfile and scripts) |

Every commit on `main` whose images are published is deployed automatically (LM-049, [§4](#4-deploy-and-roll-back)), smoke-tested through the public URL by the hosted smoke suite (LM-050), and rolled back automatically if anything fails. This runbook covers provisioning, deployment, rollback and operating the environment by hand.

## Contents

1. [Architecture](#1-architecture)
2. [Public API](#2-public-api)
3. [Provision](#3-provision)
4. [Deploy and roll back](#4-deploy-and-roll-back)
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
Internet ──TCP 80/443──▶ caddy ──proxy──▶ coordinator :8000, wallet :8001, catalog :8002, audit-ledger :8003
        (edge: egress, Caddy only)   (internal)      │
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
  | `releases/` | Release records, bundles, `current`, `last-known-good` and `history.jsonl` (LM-049, [§4.2](#42-release-records-and-last-known-good)) |
  | `deploy-logs/` | One log per deploy, promote and rollback, and the failing release's redacted log bundle |

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

## 4. Deploy and roll back

### 4.1 Automatic deploy from `main`

`.github/workflows/deploy-staging.yml` runs when the **Service images** workflow (`.github/workflows/images.yml`) completes for `main`, and by hand (**Actions → Deploy staging → Run workflow**, from `main`, with a full commit SHA on `main`). Only one staging change runs at a time (concurrency group `staging-deploy`, never cancelled mid-way; the manual rollback workflow uses the same group). On the VM, every `deploy.sh` command that changes state takes a lock in `/var/lib/ledgermesh/releases/.lock`. A second invocation waits for it with backoff, for up to `--lock-timeout` seconds (default 300), and then fails. Short operations such as `promote`, or a manually started smoke run's fixture setup, therefore queue instead of failing a deployment. A lock whose process is gone is removed.

| Job | What it does |
|---|---|
| **Gate** | `infra/staging/ci/gate.sh`. For a `workflow_run`: the Service images run must have succeeded, been triggered by `core-ci` (`event == workflow_run`) for `main` in this repository, and all four `publish / publish ledgermesh-<image>` jobs must have succeeded. If `core-ci` failed, the publish jobs were skipped but the run still "succeeded": the gate ends green with `deploy=false` and a notice. The commit deployed is the one named in the run's title, `Service images <sha>` (`run-name` in `images.yml`), which is the commit `core-ci` tested and the run built. The run's `head_sha` is not used: for a `workflow_run`-triggered run it is `main`'s tip when the run was queued, so after quick merges it names a newer commit whose images may not exist yet. The commit must also still be `main`'s tip: image runs for consecutive commits run in parallel and can finish in any order, and an older commit's run ends with `deploy=false` and a notice instead of replacing a newer release (the tip's own run deploys it). For a manual run: the run must be on `main`; any published commit on `main` may be deployed, older ones included (recovery). In both cases the commit must be an ancestor of `origin/main` and contain `infra/staging/deploy.sh`, and the four GHCR digests are looked up with `infra/scripts/ghcr_digest.sh`. |
| **Deploy** | Records a GitHub deployment for the exact SHA (`in_progress`), authenticates to Google Cloud with Workload Identity Federation (no stored key), and runs `infra/staging/ci/remote.sh deploy <sha> --no-promote`: it uploads `infra/staging` exactly as committed at `<sha>` (`git archive`), checks its sha256 on the VM, and runs `sudo bash deploy.sh deploy …` over IAP SSH with OS Login. Then `remote.sh status --expect <sha>`. |
| **Hosted smoke** | Calls `.github/workflows/hosted-smoke.yml` (LM-050) with `base_url` = `STAGING_URL` and `expected_revision` = the SHA. |
| **Promote** | `deploy.sh promote <sha>`: the release becomes last-known-good. Marks the GitHub deployment `success` with the staging URL. |
| **Restore last known good** | Runs if any job after the gate failed or was cancelled: collects a redacted log bundle (`deploy.sh logs`) as the artifact `staging-deploy-logs-<sha>-<attempt>` (kept 14 days), runs `deploy.sh rollback last-known-good --after-failed <sha>`, shows `status`, and marks the GitHub deployment `failure`. The run stays red. If the failed SHA is itself last-known-good (an idempotent redeploy of the live release that then failed smoke), nothing older is restored: `deploy.sh` logs that and records `smoke_failed_on_last_known_good` in `history.jsonl`, and the deployment's description says the release is still live. Restore an older SHA with the manual rollback if needed. |

`deploy.sh deploy` on the VM, step by step:

1. **Images.** Pulls `ghcr.io/sonalilonkar1/ledgermesh-{coordinator,wallet,catalog,audit-ledger}:<sha>`, reads each registry digest, and checks it equals the digest the gate looked up and that the image's `LEDGERMESH_REVISION` is `<sha>`. If any image is missing or wrong it exits **10** before anything changes: the running release keeps running.
2. **Record.** Copies the uploaded bundle to `releases/<sha>/bundle/`, writes `releases/<sha>/images.env` with the digest-pinned references (`…@sha256:…`), and starts `releases/<sha>/release.json` (`status: deploying`, previous release, actor, run URL, start time).
3. **Bring-up**, with that bundle's `stack.sh up` and the pinned images: render `/etc/ledgermesh/staging.env` from Secret Manager; start Kafka and the databases (`compose up --wait`, bounded by `--wait-timeout`, default 300 s); check `max_prepared_transactions`; run `alembic upgrade head` for each service; run the LM-018 topic bootstrap (`ledgermesh-topics`) before any consumer starts and check the 15 topics; start the APIs and Caddy (`--wait`, bounded); start the four workers; run every check (`verify all`: health through Caddy over HTTPS, ports, prepared transactions, topics, database isolation, workers stable for 20 s).
4. **Revision.** Every API and worker container must run exactly the recorded image reference with `LEDGERMESH_REVISION=<sha>`.
5. **Done.** `status: healthy`, `current` points at `<sha>`, `/opt/ledgermesh/staging` links to its bundle. The workflow promotes it after the smoke suite; by hand, `deploy` promotes it immediately unless `--no-promote` is given.

If anything in steps 2 to 4 fails, `deploy.sh` saves the failing containers' logs (`deploy-logs/<sha>-<time>-failed.tgz`, passwords redacted), restores the last-known-good release from **its own** bundle and image digests, re-pulling by digest if needed, with **no rebuild, no migrations and no topic bootstrap**, and exits **20**. It exits **21** if the restore failed too, and **22** on the very first deploy, when there is nothing to restore.

**Idempotent.** Deploying the SHA that is already running again is safe: images are already present, migrations and the topic bootstrap are no-ops, Compose leaves unchanged containers alone, and the record's `attempts` goes up by one.

**Why promote after the smoke suite.** If the release were last-known-good as soon as it was healthy, a smoke failure could only "restore" the release that just failed. Promoting after the smoke suite means a smoke failure restores the previous good release.

**Migrations must stay backward compatible** with the previous release (add first, remove in a later release): a rollback runs the previous image against the newer schema, and never migrates down.

**Skipped commits.** GitHub keeps only the newest pending run per concurrency group, so after several quick merges an intermediate commit may never be deployed; the newest one is. Image runs can also finish out of order; the gate then skips the older commit (see **Gate** above), so staging never moves back to an older commit by itself.

**Pull requests can never deploy:** the workflow has no `pull_request` trigger; the gate checks the triggering run's event and repository (a fork's pull request can have a branch called `main`); the Workload Identity provider only accepts tokens for `refs/heads/main` of `sonalilonkar1/LedgerMesh` (§12); and the `staging` environment allows only `main`. A `workflow_run` workflow only runs once its file is on the default branch, so nothing deploys before this workflow is merged.

### 4.2 Release records and last known good

On the VM, under `/var/lib/ledgermesh/releases/` (on the persistent disk, so they survive VM recreation):

| Path | Contents |
|---|---|
| `<sha>/release.json` | `sha`, `status`, `actor`, `run_url`, `url`, `started_at`, `healthy_at`, `finished_at`, `promoted_at` (UTC), `attempts`, `images` and `digests` per service, `previous_last_known_good`, `failed_phase`, `rolled_back_to` |
| `<sha>/images.env` | The four digest-pinned image references (no secrets) |
| `<sha>/bundle/` | `infra/staging` as deployed: Compose file, Caddyfile, scripts |
| `current` | The SHA that should be running |
| `last-known-good` | The SHA a failure restores |
| `history.jsonl` | One line per event: `deploy_started`, `healthy`, `promoted`, `deploy_failed`, `rollback_started`, `rolled_back`, `rollback_failed` |

`status` values: `deploying`, `healthy` (up and verified, not yet promoted), `promoted` (last-known-good), `failed`, `failed_rolled_back`, `smoke_failed_rolled_back`, `rollback_failed`, `failed_no_rollback`.

```bash
gcloud compute ssh ledgermesh-staging --zone us-west1-b --tunnel-through-iap
sudo bash /opt/ledgermesh/staging/deploy.sh status          # current, last known good, every container's image and revision
sudo cat /var/lib/ledgermesh/releases/<sha>/release.json
sudo tail -n 20 /var/lib/ledgermesh/releases/history.jsonl
```

From a laptop with the same access (§11): `bash infra/staging/ci/remote.sh status`.

`deploy.sh` keeps the images of the newest three releases plus the current and last-known-good ones and removes older ones (`--keep`); a rollback to an older release pulls its images again by digest.

### 4.3 When a deploy fails

1. Open the failed **Deploy staging** run. The **Restore last known good** job shows whether the restore worked and what is running now (`remote.sh status`).
2. Download the artifact `staging-deploy-logs-<sha>-<attempt>`. It holds:
   - `summary.txt`: current and last-known-good SHAs;
   - `releases/<sha>.json` (look at `failed_phase`) and `releases/history.jsonl`;
   - `deploy-logs/<sha>-<time>.log`: the deploy's own output, including `stack.sh`'s phases and the failing command's output;
   - `deploy-logs/<sha>-<time>-failed/`: every container's logs, `ps.txt` and `containers.txt` as they were **before** the rollback;
   - `now/`: the containers' logs after the rollback.

   Every database password is replaced by `[REDACTED]`, and the bundle is not written if one would remain.
3. Read the phase:

   | `failed_phase` | Where to look |
   |---|---|
   | (exit 10, no record) | The **Deploy on the VM** step: an image is missing or its digest or revision does not match. Nothing changed. |
   | `env` | Secret Manager access of the VM service account; instance metadata `ledgermesh-site-address`. Nothing was changed; the automatic rollback reuses the existing `/etc/ledgermesh/staging.env` (mode `0600`, all keys set) instead of reading Secret Manager again, so it does not fail at the same step. |
   | `images` | Pulling a recorded digest (registry reachable?) |
   | `data-tier`, `prepared-transactions` | `kafka.log`, `<service>-db.log`; is the data disk mounted? |
   | `migrations` | The `alembic` output in the deploy log. The schema stays at the last successful revision, and the restored release must work with it. |
   | `topics` | The `ledgermesh-topics` output in the deploy log; `kafka.log` |
   | `services` | The service's log and `containers.txt` (health status); `compose up --wait` timed out or a container became unhealthy |
   | `workers`, `verify` | The `verify` output in the deploy log: which check failed; the worker's log if it restarted |
   | `revision` | A container is not running the recorded image (see the `FAIL` lines) |
   | smoke failed (`smoke_failed_rolled_back`) | The **Hosted smoke** job's test output |

4. If the restore failed too (`rollback_failed`, exit 21), staging may be down: follow §7 on the VM and roll back by hand (§4.4) to an older release.

### 4.4 Roll back

**Workflow:** **Actions → Roll back staging → Run workflow** on `main`, `target`:

- `previous`: the newest release that was last-known-good, other than the one running now;
- `last-known-good`;
- a full commit SHA that staging ran and promoted before (`deploy.sh status`, `history.jsonl`).

It resolves the target on the VM, restores it from its own bundle and image digests (no rebuild, no migrations, no topic bootstrap), checks every container's revision, runs the hosted smoke suite, and records a GitHub deployment for that SHA. The restored release becomes last-known-good. If the smoke suite fails, the run is red and nothing else is rolled back: pick another target.

**By hand** (an admin, §11):

```bash
bash infra/staging/ci/remote.sh rollback previous          # or last-known-good, or <sha>
bash infra/staging/ci/remote.sh status --expect <sha>
```

A release that never became last-known-good (for example one that failed) cannot be a target.

### 4.5 Deploy by hand

From a checkout of `main` with full history, as an admin (§11). The commit's images must be published.

```bash
bash infra/staging/ci/remote.sh deploy <sha> --actor <your GitHub login>   # promotes when healthy
bash infra/staging/ci/remote.sh status --expect <sha>
```

Add `--no-promote` to promote later with `remote.sh promote <sha>`. `GCP_PROJECT_ID`, `GCP_ZONE` and `STAGING_VM_NAME` (or `--project`, `--zone`, `--vm`) select the VM; the defaults are `deven-cloud-lab`, `us-west1-b` and `ledgermesh-staging`.

`stack.sh` can still be run directly on the VM (`sudo bash /opt/ledgermesh/staging/stack.sh …`, §5 and §7), but `stack.sh up` does not write release records, so the next failed deploy would not know about it. Prefer `deploy.sh`.

## 5. Verify

On the VM:

```bash
S="sudo bash /opt/ledgermesh/staging/stack.sh"
$S verify all            # everything below
$S verify health         # Docker health of every container, then HTTPS /health and /health/<service> through Caddy
$S verify ports          # only caddy publishes host ports, and only 80/tcp and 443/tcp
$S verify topics         # the 15 LM-018 topics with the right partition counts, no others; auto-create off
$S verify prepared-transactions  # max_prepared_transactions > 0; count and age of pg_prepared_xacts
$S verify egress         # only caddy is on a network with internet egress; no container reaches the GCE metadata server
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

### 5.1 Hosted smoke suite (LM-050)

The suite in `tests/hosted/` checks a deployed environment end to end through the public URL only: health and service identity, the gateway allowlist (40 forbidden requests must get Caddy's empty 404), closed ports, and Saga redemptions to `COMPLETED`, `FAILED` (insufficient points) and `COMPENSATED` (exhausted inventory) with exactly-once wallet and catalog effects, idempotency-key replay and conflict, the audit trail and the correlation id. 2PC cannot be started through the public API and is skipped with the reason. Details, the full case list and every setting: [tests/hosted/README.md](../../tests/hosted/README.md).

It runs automatically after every deploy and rollback (`.github/workflows/hosted-smoke.yml`). To run it by hand, choose the environment explicitly:

- **GitHub:** Actions, **Hosted smoke**, *Run workflow*, `base_url` = `STAGING_URL`.
- **Laptop:** `bash infra/staging/ci/remote.sh fixtures <run_id>`, then `LEDGERMESH_HOSTED_BASE_URL=<STAGING_URL> LEDGERMESH_HOSTED_RUN_ID=<run_id> LEDGERMESH_HOSTED_REQUIRE_FIXTURES=true make hosted-smoke`.

Each run first creates its own fixtures on the VM (`deploy.sh fixtures <run_id>`, run by the smoke workflow over IAP SSH). It uses the running release's `fixtures.py`, piped into the running `wallet` and `catalog` containers, so each service's rows are written with that service's own credentials and nothing leaves the containers. Only `smoke-<run_id>-*` rows are inserted, with a plain `INSERT` (a reused run id fails), and the script refuses unless `LEDGERMESH_ENV` is `staging`, `local` or `test`. When `expected_revision` is given, the workflow also runs `remote.sh status --expect <sha>` first. It also checks out that commit, so the tests in `tests/hosted/` come from the deployed release and match its `fixtures.py` even when `main` has moved on. Without `expected_revision` (a manual run), the tests come from the workflow's own commit. The fixture step takes the VM's deploy lock, which waits (bounded) for a running deploy, promote or rollback, so a manual run cannot make one of them fail.

When it fails, open the run's `hosted-smoke-<run_id>` artifact: `junit.xml` and `diagnostics.json` (per scenario: transaction and correlation ids, the statuses seen while polling, audit events, final balances; only public API data). Then look the transaction up on the VM (§7), for example `stack.sh logs coordinator-saga | grep <transaction_id>`.

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
| VM deleted or broken | The data disk survives (`auto-delete=no`), and with it the release records. Re-run `provision.sh up` (it recreates the VM and attaches the existing disk; the startup script finds the filesystem and does not format it), then `remote.sh rollback last-known-good` (§4.4), which pulls the recorded images by digest. |
| Data disk lost or corrupted | Create a disk from the newest snapshot: `gcloud compute disks create ledgermesh-data --zone us-west1-b --source-snapshot <snapshot> --type pd-balanced`, then re-run `provision.sh up` and deploy. Data written after that snapshot is lost. |
| Wrong release deployed | Run **Roll back staging** with `previous` (§4.4). Migrations are not rolled back. |

## 10. Secrets and rotation

- The four database passwords live only in Secret Manager. `render-env.sh` reads them at deploy time with the VM's service account and writes `/etc/ledgermesh/staging.env`, mode `0600`, owned by root. It never prints them. The file is never committed, copied into a log bundle, or baked into an image.
- No container can read the secrets itself: only Caddy is on a Docker network with internet egress, and the startup script installs a `DOCKER-USER` iptables rule that rejects container traffic to the metadata server `169.254.169.254`, so no container can obtain the VM service account's token (`stack.sh verify egress` checks both).
- Docker passes the passwords to containers as environment variables, so anyone with root or Docker access on the VM can read them: the named admins and, through OS Login admin, the deploy service account used by the GitHub workflows (§12).
- There is no GHCR token: the packages are public. If they become private, add a secret `ledgermesh-staging-ghcr-read-token` holding a token with only `read:packages`, grant the VM service account access to it, and run `docker login ghcr.io` from it before pulling (see `infra/IMAGES.md`, "Pulling from a staging host").

**Rotate a database password** (for example after it was printed by mistake):

```bash
NEW="$(openssl rand -hex 32)"
printf '%s' "$NEW" | gcloud secrets versions add ledgermesh-staging-wallet-db-password --data-file -
```

Then change the role's password inside that database and redeploy, because `POSTGRES_PASSWORD` is only applied when the data directory is first created. On the VM (`\password` prompts for the value without echoing it or saving it in shell history):

```bash
$DC exec wallet-db psql -U wallet -d wallet      # then: \password wallet
```

Then, from your laptop, redeploy the running release; it re-renders the env file and recreates the containers whose `DATABASE_URL` changed:

```bash
bash infra/staging/ci/remote.sh deploy <running sha> --actor <your GitHub login>
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

How the LM-049 workflows use the environment:

- Every job that reaches the VM (`deploy`, `promote`, `rollback` in `deploy-staging.yml`, and `rollback-staging.yml`) declares `environment: {name: staging, deployment: false}`. That gives it the variables above and applies the `main`-only branch rule, without GitHub creating an automatic deployment, which would name the workflow's own commit rather than the one deployed. The workflows record the deployment themselves for the exact SHA (`infra/staging/ci/github-deployment.sh`, `deployments: write`): `in_progress`, then `success` with `STAGING_URL` after the smoke suite, or `failure`. The environment page therefore shows which commit is live. `github-deployment.sh` first creates the deployment with `ref` = the exact SHA. GitHub applies the environment's deployment branch rule to deployments created through the API too, and may reject a bare SHA because it is not the branch `main` ("is not allowed to deploy to staging due to environment protection rules"). In that case the script records the deployment against `ref: main` instead. The exact SHA is then in the deployment's description (`Deploy <sha12> to staging`) and in `payload.revision`, and the commit GitHub shows next to it is `main`'s tip when the deployment was created. For automatic deploys that is the same commit, because the gate deploys only `main`'s tip. For a manual deploy or rollback of an older commit, it is not. Any other API error fails the job.
- Custom deployment protection rules are not compatible with `deployment: false`; do not add one. Required reviewers would still apply, and would make every deploy wait for approval.
- The hosted smoke workflow (LM-050) also runs in `environment: {name: staging, deployment: false}` and is called with `id-token: write`: for `base_url` = `STAGING_URL` it authenticates as the same deploy service account and uses IAP SSH (`remote.sh fixtures` and `remote.sh status --expect`) to create its run fixtures and check the revision. It needs no extra variable, secret or IAM role. It must not join the `staging-deploy` concurrency group, because its caller holds it.
- The job permissions are declared in the workflows (`contents: read` by default; `actions: read` and `packages: read` for the gate; `id-token: write` and `deployments: write` only where needed), so the repository's default `GITHUB_TOKEN` setting does not need to change.

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

The deploy, rollback and release-record logic of §4 runs locally too, with the same `deploy.sh` and images already tagged on this machine (`--image-source local`; records in `artifacts/staging/data/releases`):

```bash
make staging-images-local                          # tags ghcr.io/sonalilonkar1/ledgermesh-*:<HEAD sha>
make staging-local-deploy                          # deploy.sh deploy <HEAD sha>, promoted when healthy
make staging-local-deploy STAGING_SHA=<other sha>  # any other locally tagged revision
make staging-local-status
make staging-local-rollback STAGING_TARGET=previous   # or last-known-good, or a SHA
```

The hosted smoke suite runs against the local stack as well; it needs the local CA file and its own fixtures (`make staging-local-fixtures RUN_ID=<id>`), see [tests/hosted/README.md](../../tests/hosted/README.md#running-it).

It needs host ports 80 and 443 free (`LEDGERMESH_HTTP_PORT` and `LEDGERMESH_HTTPS_PORT` override them, and `stack.sh --https-port` must match). Differences from the VM: images are not `linux/amd64` on Apple silicon, TLS uses Caddy's local CA, secrets come from a file instead of Secret Manager, and there is no cloud firewall or IAP.

## 15. Optional web UI

The web UI (`services/web`, PR #97) is not merged and staging does not depend on it. To add it after it merges:

1. Add a `web` service to `infra/staging/compose.yaml` with `profiles: ["web"]`, its published image, the `proxy` network only (internal: Caddy reaches it, it has no egress), and the same `restart`, `logging`, `deploy.resources` and `healthcheck` settings as the APIs. It must call the public API paths, not internal service URLs.
2. Create `infra/staging/caddy.d/web.caddy` with a `handle` block for its paths, mount `./caddy.d:/etc/caddy/caddy.d:ro` into the `caddy` service, and uncomment `import /etc/caddy/caddy.d/*.caddy` in the Caddyfile. The import sits before the catch-all 404.
3. Start it with `COMPOSE_PROFILES=web`. `deploy.sh` keeps the whole `infra/staging` directory, including `caddy.d/`, in each release bundle, so a rollback restores the matching gateway configuration. Update `tests/infra/test_staging_compose.py` and `test_staging_caddyfile.py` for the new service and routes.

## 16. Known limitations

- One VM in one zone: no high availability (ADR-002 §5).
- Inside its own PostgreSQL instance each service user owns its database, because the images run migrations with the runtime `DATABASE_URL`. Cross-service isolation is by separate instances and passwords (`verify db-isolation`). A separate migration role is a follow-up.
- Prepared-transaction age and count are checked by `stack.sh verify prepared-transactions` (it fails when the oldest is older than 300 s, configurable with `--prepared-max-age`), but there is no paging alert yet (`infra/README.md` asks for one).
- Logs stay on the VM in rotated `json-file` logs; there is no central log store. `collect-logs` produces a redacted bundle for sharing.
- Deploys are not zero-downtime: containers whose image changed are recreated in place, and Caddy is recreated on every new release (its Caddyfile path is in the release bundle). Certificates persist on the data disk.
- A rollback never migrates down, so every migration must work with the previous release (§4.1). A release that fails in its migration step leaves the schema at its last successful revision.
- The first deploy has nothing to roll back to (exit 22); fix forward or run **Roll back staging** once a good release exists.
- `deploy.sh status` and the deploy workflow check the running revision on the VM, because `/health` does not report one (`services/*/app/main.py`).
- The hosted smoke suite cannot exercise 2PC: it can only be started through the coordinator's internal route, which is 404 outside `local`/`test` and blocked by the gateway (tests/hosted/README.md). 2PC is covered by `make test-2pc-integration`.
- Hosted smoke fixture rows (`smoke-<run_id>-*` wallets and rewards, and the redemptions, operations and audit events made with them) accumulate: they are never deleted, because the history refers to them. They are a few rows per deploy; `make staging-local-reset` or a fresh data disk removes them.
- After quick successive merges only the newest pending deploy runs (GitHub concurrency); intermediate commits are skipped.
- `sslip.io` is a third-party DNS service. If it is unavailable the URL does not resolve, although the VM keeps running; a team-owned domain removes that dependency (ADR-002 §6).
