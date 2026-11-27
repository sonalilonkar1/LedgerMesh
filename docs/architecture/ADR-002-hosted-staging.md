# ADR-002: Hosted staging environment

- **Status:** Proposed
- **Issue:** LM-048 / GitHub issue #56 (deployment automation is LM-049 / #57, hosted smoke tests are LM-050 / #58)
- **Date:** 2026-11-27
- **Owner:** Deven

ADR-001 is the canonical architecture record. If this document conflicts with it, ADR-001 wins and this ADR must be corrected.

## 1. Context

The course requires a LedgerMesh deployment that runs off every team member's laptop and can be demonstrated over HTTPS. Staging must keep the properties the local stack (`infra/compose.yaml`, LM-003) already has:

- four services, each with its own PostgreSQL database and credentials (ADR-001 §2);
- a Kafka broker in KRaft mode with topic auto-creation disabled, and topics created by the LM-018 bootstrap (`ledgermesh_messaging.topics`);
- `max_prepared_transactions > 0` for 2PC (LM-012);
- the Saga workers and the audit consumer, which run as long-lived Kafka consumers.

It must also be cheap enough for a course project, reproducible from the repository, and simple enough that a second team member can inspect it.

## 2. Decision

| Aspect | Decision |
|---|---|
| Provider | Google Cloud, Compute Engine |
| Project | `deven-cloud-lab` by default; every script takes `--project` |
| Region and zone | `us-west1`, `us-west1-b` |
| Host | One `e2-standard-4` VM (4 vCPU, 16 GB), Debian 12, `linux/amd64`, Shielded VM |
| Runtime | Docker Engine and the Compose plugin, running `infra/staging/compose.yaml` |
| Images | The immutable LM-047 images `ghcr.io/sonalilonkar1/ledgermesh-{coordinator,wallet,catalog,audit-ledger}:<full commit sha>` (`infra/IMAGES.md`), which are built for `linux/amd64` |
| Durable data | A separate 30 GB `pd-balanced` disk, `ledgermesh-data`, mounted at `/var/lib/ledgermesh`. Kafka, the four PostgreSQL data directories and Caddy's certificates are bind-mounted from it. The disk has `auto-delete=no` and a daily snapshot schedule with 7-day retention. |
| Public entry point | Caddy, the only container with host ports (TCP 80 and 443), terminating HTTPS with automatic certificates. It forwards an explicit allowlist of read and redemption routes; everything else is `404`. |
| Hostname | A reserved static external IPv4 address and `<ip-with-dashes>.sslip.io`, so the name is stable without buying a domain |
| Network | A dedicated VPC, `ledgermesh-staging`. Firewall: TCP 80 and 443 from anywhere; TCP 22 only from Google's IAP range `35.235.240.0/20`. Nothing else. The `default` network, which allows SSH from anywhere, is not used. |
| Admin access | OS Login and IAP TCP forwarding (`gcloud compute ssh --tunnel-through-iap`). Named team members get IAM roles; there are no SSH keys in project metadata. |
| Secrets | One Secret Manager secret per database password. The VM service account can read only those four secrets. `infra/staging/render-env.sh` renders them into a root-only `0600` file outside Git at deploy time. |
| Container registry access | The GHCR packages are public, so no pull credential is needed. If they become private, a `read:packages` token goes into Secret Manager (`infra/IMAGES.md`, "Pulling from a staging host"). |
| Cost control | A billing budget with alerts at 50%, 90% and 100%, a documented stop and start procedure, and a full teardown |
| Provisioning | `infra/staging/provision.sh`, an idempotent, version-controlled `gcloud` script with `--dry-run`, plus a separate one-time `infra/staging/bootstrap-wif.sh` for GitHub Actions Workload Identity Federation (used by LM-049) |

### 2.1 Inside the VM

```text
Internet ──TCP 80/443──▶ caddy ──(edge network)──▶ coordinator / wallet / catalog / audit-ledger APIs
                                                           │
                                              (backend network, internal: no egress)
                                                           │
            kafka ◀── coordinator-saga, wallet-saga, catalog-saga, audit-consumer
            coordinator-db, wallet-db, catalog-db, audit-ledger-db
```

- `backend` is an `internal` Docker network: Kafka, the databases and the workers have no route to the internet and nothing on it publishes a port.
- `edge` connects Caddy to the four APIs only. Caddy cannot reach Kafka or any database.
- The four Saga workers and the audit consumer are not in `infra/compose.yaml`; locally they run through the Make targets `run-coordinator-saga`, `run-wallet-saga`, `run-catalog-saga` and `run-audit-consumer`. Staging runs the same modules from the service images by overriding the command, as `infra/IMAGES.md` documents.
- Every container has a restart policy, a memory and CPU limit, and `json-file` log rotation. The APIs, databases, Kafka and Caddy have health checks. The workers have no HTTP server, so their image health check is disabled and `infra/staging/stack.sh verify workers` checks that they keep running without restarts.

## 3. Cost estimate

These are **estimates** from Google Cloud's public list prices for `us-west1` (on-demand, no committed-use discount) as understood at the time of writing. Confirm them in the [Google Cloud pricing calculator](https://cloud.google.com/products/calculator) before relying on them; prices change and taxes are excluded. A month is taken as 730 hours.

| Item | Assumption | Approximate monthly cost |
|---|---|---:|
| `e2-standard-4` VM | about $0.134 per hour × 730 h | about $98 |
| Boot disk | 20 GB `pd-balanced` at about $0.10 per GB-month | about $2 |
| Data disk | 30 GB `pd-balanced` at about $0.10 per GB-month | about $3 |
| Static external IPv4, attached to a running VM | about $0.005 per hour × 730 h | about $3.65 |
| Snapshots | incremental; at most about 30 GB stored at about $0.05 per GB-month | up to about $1.50 |
| Egress | demo traffic only | under $1 |
| **Running all month** | | **about $108** |

When the VM is **stopped**, the vCPU and memory charges stop. What remains is the two disks, the snapshots and the reserved IP, which is billed at a higher idle rate (about $0.01 per hour, so about $7.30 per month) while it is not attached to a running VM: **about $13 to $15 per month**. Running the VM only for demos and test runs, for example 40 hours a month, costs roughly 40 × $0.134 ≈ $5.40 plus the stopped cost.

E2 machine types do not receive sustained-use discounts. A billing budget only sends alerts; it does not cap spending. The stop and teardown procedures in `docs/deployment/staging.md` are the spending controls.

## 4. Alternatives considered

| Option | Why it was not chosen |
|---|---|
| **Cloud Run** | Runs request-driven containers. The Saga workers and the audit consumer are long-lived Kafka consumers, there is no Kafka, and PostgreSQL would have to be Cloud SQL. Splitting the stack across several managed products costs more and is harder for a team member to inspect. |
| **GKE Autopilot** | Workable, but a cluster plus persistent volumes for Kafka and four databases costs more than one VM for a course project, and the team would maintain Kubernetes manifests in addition to the Compose files it already has. Managed Kafka and Cloud SQL would raise the cost further, and a managed PostgreSQL service may restrict `max_prepared_transactions`. |
| **AWS EC2** | Technically equivalent to the chosen design (one VM with Docker Compose). Not chosen because the team's existing cloud project (`deven-cloud-lab`) is on Google Cloud, and IAP plus OS Login gives SSH access without a bastion or open port 22. |
| **Fly.io, Render and similar PaaS hosts** | Convenient for HTTP services, but Kafka with private networking and four persistent PostgreSQL databases with `max_prepared_transactions` are not first-class, and the long-running consumers and persistent volumes would make the result less portable than Compose. |

## 5. Consequences

- **Single host.** Staging is one VM. A VM or zone outage takes staging down until the VM is restarted or recreated; the data disk and its snapshots survive (`docs/deployment/staging.md`, disaster recovery). This is acceptable for a demonstration environment and is not production availability.
- **Same images everywhere.** Staging runs the exact images CI published for a commit, pinned by SHA. Nothing is built on the VM.
- **Migrations are explicit.** The API containers keep `RUN_MIGRATIONS=false`. Operators (and later LM-049) run `alembic upgrade head` as a one-off step before starting the new version, which means migrations must stay backward compatible with the previous image (expand, then contract), because an older image cannot downgrade from a revision it does not know.
- **Database credentials.** Each service has its own PostgreSQL instance, user and password; a service's credentials are rejected by the other three instances (`stack.sh verify db-isolation`). Within its own instance a service user is the instance owner, because the images run migrations with the same `DATABASE_URL`. Separating a migration role from a runtime role is a follow-up.
- **Internal routes are closed twice.** Caddy never forwards `/internal/*` or any participant write route, and the APIs run with `LEDGERMESH_ENV=staging`, so the Coordinator's internal routes also return `404` at the application (`services/coordinator/app/main.py`, `require_local_internal_routes`). The 2PC path therefore cannot be triggered through the public URL; this is intended.
- **Prepared-transaction alerting.** `infra/README.md` says hosted environments need alerting on the age and count of prepared transactions. `stack.sh verify prepared-transactions` reports both and fails when the oldest is older than a threshold; paging-style alerting is a follow-up and is not claimed here.

## 6. Upgrade path

1. Move PostgreSQL to Cloud SQL (one instance per service) after confirming its `max_prepared_transactions` support, and Kafka to a managed Kafka service. The services already take `DATABASE_URL` and `KAFKA_BOOTSTRAP_SERVERS`, so only configuration changes.
2. Replace Compose with GKE when more than one host is needed; the images and health checks carry over unchanged.
3. Replace `sslip.io` with a team-owned domain by changing the site address the VM metadata provides.
