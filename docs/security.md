# Security configuration and operational safeguards (LM-042)

This page records what LedgerMesh does to keep a demonstration deployment safe, which
settings control it, and what the dependency, secret and container scans found. It
covers the four services in `services/`; the hosted staging gateway and Kubernetes
manifests are documented with their own issues.

## Summary

| Control | Where | Behaviour |
|---|---|---|
| Internal and operator routes closed outside local/test | `SafeguardMiddleware` (`libs/messaging/src/ledgermesh_messaging/safeguards.py:240`), plus `require_internal_access` on each Coordinator route (`services/coordinator/app/main.py:94`) | Every `/internal/...` path answers `404`, exactly like an unknown route, unless `LEDGERMESH_ENV` is `local` or `test` or the request carries the operator token |
| API docs closed outside local/test | same middleware | `/docs`, `/redoc`, `/openapi.json` and `/docs/oauth2-redirect` follow the same rule |
| No local database password in a deployment | `require_safe_database_url` (`safeguards.py:120`), called by every `database_url()` | Unless `LEDGERMESH_ENV` is `local` or `test`, a database URL must carry a non-empty password (in the userinfo or a `password` query parameter) that is not the Compose password `ledgermesh_dev`; an empty `POSTGRES_PASSWORD` counts as unset |
| Refuse to start with local defaults | `check_startup_configuration` (`safeguards.py:143`), run by `install_safeguards` when `main.py` is imported | With `LEDGERMESH_ENV` set to anything but `local`/`test`, the service exits at startup if its database URL has no password or uses the development password, or the operator token is too short |
| Safe error bodies | handlers registered by `install_safeguards` (`safeguards.py:331`) | Validation, HTTP and unhandled errors are `application/problem+json` with only `type`, `title`, `status`, `detail`: no submitted input, stack trace or SQL |
| Bounded input | request models; `SafeguardMiddleware` | Numbers are bounded by their PostgreSQL column types; request bodies are limited to 1 MiB |
| No `--reload` outside local | `Makefile` `run-*` targets | The only targets that use `--reload` default to `LEDGERMESH_ENV=local` and refuse any other value. Images (`infra/docker/start-service.sh`) and Compose never reload (checked by `tests/security/test_operational_config.py`) |
| Secret scanning | `.github/workflows/security.yml`, `infra/scripts/secret_scan.sh` | gitleaks over the PR's own commits on every PR, and over every commit on every ref on `main`, weekly and on manual runs |
| Dependency audit | same workflow, `infra/scripts/dependency_audit.sh` | pip-audit over both `uv.lock` files, PyPI and OSV advisories |
| Container scan | `.github/workflows/image-build.yml:89-101` (LM-047) | Trivy fails the image on a fixable HIGH or CRITICAL finding |
| Pinned CI actions, least-privilege token | every workflow | Every third-party action is pinned to a full commit SHA; every top-level workflow declares `permissions` |

## `LEDGERMESH_ENV`

One variable decides whether development conveniences exist. The accepted values are
`LOCAL_ENVIRONMENTS = {"local", "test"}` (`safeguards.py:51`), the same set the
database scripts use (`infra/scripts/db.py:28`).

| `LEDGERMESH_ENV` | Internal routes and docs | Development DB password | Startup |
|---|---|---|---|
| `local`, `test` | open | accepted | always starts |
| unset or empty | closed (operator token opens them) | refused on first database use | starts, with a warning, so an image answers `/health` on its own (LM-047, `infra/scripts/check_image.sh`) |
| anything else (`staging`, `production`, `LOCAL`, ...) | closed (operator token opens them) | refused | **refuses to start** if a local default is still configured |

Where it is set:

- `infra/compose.yaml:127, 154, 177, 200` set `local` on all four app services, because
  Compose uses the development password.
- The images (`infra/Dockerfile.service`, `infra/docker/start-service.sh`) never set it,
  so an image fails closed until a deployment says what it is.
- `make run-*` default it to `local` (`Makefile`, `LOCAL_ENV`).
- The test suite leaves it unset unless a test sets it with `monkeypatch`.

Staging (built in parallel) runs with `LEDGERMESH_ENV=staging`, so every rule above
applies there: it must provide real database secrets.

## Routes and how each is protected

Public routes are what a client or the web UI calls. Service-to-service routes are
called by the Coordinator in every environment and must keep working, so they are not
gated in the application: they are protected by network placement. In Compose they are
on the `backend` network; in staging, the gateway publishes only an allowlist of public
routes, and these are not on it.

| Service | Route | Class | Protection |
|---|---|---|---|
| all | `GET /health` | probe | public |
| Coordinator | `POST /v1/redemptions`, `GET /v1/redemptions/{id}` | public | validation, safe errors, body limit |
| Coordinator | `GET /internal/v1/redemptions/{id}/transitions` | operator (read-only) | **closed outside local/test** (it was the only ungated internal route before LM-042) |
| Coordinator | `POST /internal/v1/redemptions/{id}/execute-2pc` | test control | closed outside local/test |
| Coordinator | `GET /internal/v1/2pc/in-doubt`, `POST /internal/v1/2pc/recovery/run` | operator | closed outside local/test |
| Wallet | `GET /v1/wallets/{user_id}` | public read | validation, safe errors |
| Wallet | `POST /v1/wallets/{user_id}/deductions`, `/refunds` | internal service API (no application caller; the Saga uses Kafka) | network placement |
| Wallet, Catalog | `POST /v1/transactions/{id}/prepare`, `/commit`, `/abort`, `GET /v1/transactions/{id}` | service-to-service (Coordinator 2PC, `services/coordinator/app/two_phase_commit.py`) | network placement; deliberately open in every environment |
| Catalog | `GET /v1/rewards`, `GET /v1/rewards/{id}` | public read | validation, safe errors |
| Catalog | `POST /v1/rewards/{id}/reservations`, `/releases`, `/fulfillments` | internal service API (no application caller) | network placement |
| Audit Ledger | `POST /v1/audit-events` | internal service API (the ledger consumes Kafka) | network placement |
| Audit Ledger | `GET /v1/transactions/{id}/events` | operator read | network placement |

There are no HTTP failure-injection endpoints. Fault injection (LM-035) stops, pauses and
kills containers from outside, and only against the local Compose stack.

The 2PC workflow does not depend on `execute-2pc` outside local/test: the recovery
worker (`COORDINATOR_RECOVERY_ENABLED`) starts and resumes durable transactions
(`services/coordinator/README.md`).

### Operator token

An operator can reach the internal routes of a deployment without changing
`LEDGERMESH_ENV`:

```bash
# Generate once and store it in the deployment's secret store, never in Git.
python -c 'import secrets; print(secrets.token_urlsafe(48))'

curl -H "X-LedgerMesh-Operator-Token: $LEDGERMESH_OPERATOR_TOKEN" \
  http://coordinator:8000/internal/v1/2pc/in-doubt
```

- Set with `LEDGERMESH_OPERATOR_TOKEN`; sent in `X-LedgerMesh-Operator-Token`
  (`safeguards.py:53-55`).
- Unset by default, so nothing is open. A token shorter than 32 characters never
  matches, and a deployment refuses to start with one.
- Compared in constant time on SHA-256 digests (`operator_token_matches`,
  `safeguards.py:99`), so neither the token nor its length leaks through timing.
- The token is never logged: no LedgerMesh code logs request headers. The unhandled-error
  log records only the method and path (`safeguards.py:405-411`), and
  `CorrelationMiddleware` reads only `X-Correlation-Id` and `X-Transaction-Id`
  (`libs/messaging/src/ledgermesh_messaging/logs.py:249-253`).
  `tests/security/test_safeguards.py` checks that a 500 carrying the token does not log it.
- A wrong or missing token gets the same `404` as an unknown route.

The token opens all internal routes and the docs. It is meant for an operator behind the
gateway, not for clients: the gateway should not forward the header from the internet.

## Unsafe defaults removed

- **Database password.** Every `database_url()` used to fall back to `ledgermesh_dev` in
  any environment. It now passes the URL through `require_safe_database_url`
  (`services/coordinator/app/repository.py:156`, `services/wallet/app/repository.py:87`,
  `services/catalog/app/repository.py:129`, `services/audit_ledger/app/repository.py:40`).
  That covers the HTTP services and the Kafka workers, which use the same function. The
  error names the variable to set (for example `WALLET_DATABASE_URL`) and never the URL,
  which may contain a password. A URL with no password, or an empty one, is refused
  the same way, and so is `ledgermesh_dev` passed as a `password` query parameter. An
  empty `POSTGRES_PASSWORD` is treated as unset. With `LEDGERMESH_ENV` unset the error
  also says to set `LEDGERMESH_ENV=local` for local development, which is what a
  developer running a worker by hand usually needs (`make run-*` already does it).
- **API docs** are closed outside local/test (see above). FastAPI still builds the
  schema, so `app.openapi()` and the contract tests are unaffected.
- **`--reload`** only appears in the `make run-*` targets, which now refuse a non-local
  `LEDGERMESH_ENV`.
- `infra/.env.example` and `infra/README.md` already document `ledgermesh_dev` as a
  local-only credential. The CI database password `ledgermesh` (`ci.yml`,
  `messaging.yml`, `libs/messaging/docker-compose.test.yml`) belongs to throwaway
  service containers.

## Input validation and error responses

Every error a service returns is `application/problem+json` with the fields the
contracts' `Problem` schema allows (`libs/contracts/openapi/*.yaml`, `additionalProperties:
false`):

| Case | Status | `type` | Notes |
|---|---|---|---|
| Domain error (insufficient points, conflict, not found, ...) | 4xx | `urn:ledgermesh:<service>:...` | unchanged, from each service's handler |
| Invalid body, header, path or query | 422 | `urn:ledgermesh:<service>:invalid-request` | `detail` lists field locations and messages only. The submitted value and pydantic's `ctx` are never echoed |
| Malformed JSON | 422 | same | |
| Body over the limit | 413 | `about:blank` | rejected before any handler runs |
| Unknown route, closed internal route, closed docs | 404 | `about:blank` | identical bodies |
| Wrong method | 405 | `about:blank` | keeps the `Allow` header |
| Unhandled exception | 500 | `urn:ledgermesh:<service>:internal-error` | generic `detail` with a correlation id, also returned in `X-Correlation-Id` |

For a 500, the service logs the traceback at `ERROR` with the same `correlation_id`,
so `jq 'select(.correlation_id == "<id>")'` finds it (`docs/logging.md`). Credentials in
the traceback are masked by the log formatter. The correlation id is the caller's
`X-Correlation-Id` when it looks like an id (`^[A-Za-z0-9._:-]{1,128}$`), otherwise a
new UUID.

Bounds added to the request models, each taken from the column the value is stored in or
compared with, so no value that could have succeeded before is refused:

| Model | Field | Bound | Column |
|---|---|---|---|
| `CreateRedemptionRequest` | `quantity` | `<= 2^31-1` | `redemptions.quantity INTEGER` |
| `DeductRequest`, `WalletPrepareRequest` | `points` | `<= 2^63-1` | `BIGINT` |
| `ReserveRequest`, `ReleaseRequest`, `CatalogPrepareRequest` | `quantity` | `<= 2^31-1` | `INTEGER` |
| `CatalogPrepareRequest` | `expected_reward_version`, `expected_point_cost` | `<= 2^63-1` | `BIGINT` |
| `AuditEventRequest` | `event_type`, `aggregate_type` | `max_length=255` | `VARCHAR(255)` |
| `AuditEventRequest` | `workflow_step`; `aggregate_version`, `fencing_token` | `<= 2^31-1`; `<= 2^63-1` | `INTEGER`; `BIGINT` |

Unbounded text fields (`user_id`, `reward_id`, `aggregate_id`, the audit `payload`) are
limited by the request body limit, `LEDGERMESH_MAX_REQUEST_BYTES` (default `1048576`;
a non-positive or non-integer value refuses to start).

Contract notes:

- The bounds were added to the application only. The OpenAPI files are unchanged because
  the compatibility checker treats a new `maximum`/`maxLength` on a request as breaking
  (`libs/contracts/src/ledgermesh_contracts/compatibility.py`). Values above the bounds
  never worked, so clients see no difference except a 422 instead of a 500.
- `coordinator.v1.yaml` lists `400` for an invalid `POST /v1/redemptions`; the service
  returns `422`, as it did before LM-042. Aligning the two is left to a contract change.

Tests: `tests/security/test_safeguards.py` covers the mechanism once;
`services/*/tests/test_security.py` cover each service's wiring and models (malformed,
negative and oversized input, no internals in any error body, docs and internal routes
closed by default, participant routes open in every environment, database password
refused); `tests/security/test_service_startup.py` imports each real service in a fresh
interpreter to prove it refuses to start in `staging` without a real secret.

## Scan results

All scans ran on **2026-11-27** against `3e9e162` (`origin/main`, which LM-042 starts
from). LM-042 adds no dependencies, so the dependency results also hold for this branch;
they were re-run on the branch with the same result.

### Python dependencies: no known vulnerabilities

| Scope | Tool | Packages | Result |
|---|---|---|---|
| Root `uv.lock`, all groups (what the images install, plus dev tools) | pip-audit 2.10.1, PyPI and OSV | 52 runtime, plus dev | 0 |
| `libs/messaging/uv.lock`, all groups | pip-audit 2.10.1, PyPI and OSV | 19 | 0 |

Notable runtime versions: fastapi 0.162.0, starlette 1.10.0, uvicorn 0.54.0, pydantic
2.14.2, sqlalchemy 2.1.8, psycopg 3.3.8, alembic 1.20.1, aiokafka 0.15.0, httpx 0.28.1,
h11 0.16.0.

Open PRs that add dependencies (LM-035 #85: locust; LM-033 #101: OpenTelemetry) are
covered by the CI job once they are rebased onto this.

### Secrets: none found

| Scope | Tool | Result |
|---|---|---|
| Every commit on every ref (186 commits with content, 47 refs) | gitleaks v8.30.1, default rules | no leaks |
| Working tree | gitleaks v8.30.1, `dir` mode | no leaks |
| Image layers (coordinator) | Trivy secret scanner | 0 secrets |

Re-run on this branch with `make security-secrets`: every pushed ref plus this branch is
clean. Because the local scan mirrors *every* local branch, it also flagged three
fixture values (`*_DB_PASSWORD` test passwords such as `wallet-test-pw-0123456789`) in
unpushed staging tests on another working branch (`tests/infra/test_staging_*.py`,
LM-048). They are fake. On a pull request CI scans only that PR's commits
(`SCAN_RANGE=<base sha>..<head sha>`), so they fail only the LM-048 PR, never an
unrelated one; the full-history scan on `main` and the weekly run would still report them
once that branch is pushed.

How to clear a finding on fake values:

- **Before the commit is pushed:** add a trailing `# gitleaks:allow` comment to the line
  (amend or rebase the commit). This only works in the commit that introduces the value:
  a later commit that adds the comment leaves the earlier commit in history, and that
  commit is still scanned.
- **After it is pushed:** add the finding's fingerprint (printed by gitleaks, in the form
  `<commit>:<file>:<rule>:<line>`) to a `.gitleaksignore` file at the repository root,
  with a comment saying why the value is safe. `secret_scan.sh` passes that file to
  gitleaks explicitly, because the bare mirror it scans has no working tree.

Never allowlist whole files or paths.

The default rules do not flag the local placeholders (`ledgermesh_dev`, `ledgermesh`),
so the repository needs no `.gitleaks.toml`. If one is ever needed for a known-safe
placeholder, `secret_scan.sh` picks it up automatically; each allowlist entry must say
why the value is safe.

### Container image (coordinator; all four share the base and dependencies)

Trivy 0.70.0 with the CI policy (`--severity HIGH,CRITICAL --ignore-unfixed`,
`.trivyignore` has no active entries) on the `linux/amd64` image (Debian 12.15).

| Finding | Severity | Fixed in | Triage |
|---|---|---|---|
| CVE-2025-6020 in `libpam-modules`, `libpam-modules-bin`, `libpam-runtime`, `libpam0g` 1.5.2-6+deb12u2 | HIGH | 1.5.2-6+deb12u3 | **Fixed**: `infra/Dockerfile.service` upgrades the four packages. Not reachable in practice (the service runs as uid 10001 with no login, `su` or `sshd`), but it failed the CI gate. The change is byte-identical to PR #93, so the two merge cleanly. |
| CVE-2024-22365 in the same libpam packages | MEDIUM | 1.5.2-6+deb12u3 | Fixed by the same upgrade. |
| pip 25.0.1 in the base image: CVE-2025-8869, CVE-2026-3219, CVE-2026-6357, CVE-2026-8643, CVE-2026-13346 (MEDIUM), CVE-2026-1703 (LOW) | MEDIUM/LOW | pip 25.3 to 26.2.0 | Accepted. The services run from `/app/.venv` built by uv and never call pip. Removing pip from the runtime stage is a possible follow-up. |
| 2 CRITICAL, 96 HIGH, 144 MEDIUM, 114 LOW, 18 UNKNOWN Debian findings with no fix | all | none | Accepted until Debian ships fixes (the policy ignores unfixed findings). The CRITICALs are zlib1g CVE-2023-45853 (minizip, `will_not_fix`, unused) and libsqlite3-0 CVE-2025-7458 (the services use PostgreSQL). Most HIGHs are base-OS tools the services never run (krb5, util-linux, ncurses, systemd libs, perl-base, gzip). |

The application packages in `/app/.venv` had no findings, which agrees with pip-audit.

Re-run on this branch (2026-11-27, `make security-images` equivalent for the coordinator,
`linux/amd64`): with the libpam upgrade, Trivy reports **0** fixable HIGH/CRITICAL
findings and exits 0, and `infra/scripts/check_image.sh` passes (the image starts with
`LEDGERMESH_ENV` unset and answers `/health`, runs as uid 10001, contains no secrets). The
same image started with `LEDGERMESH_ENV=staging` and no database secret exits with
`UnsafeConfigurationError ... set COORDINATOR_DATABASE_URL to a URL with a real secret`.

## Re-running the scans

| Command | What | Needs |
|---|---|---|
| `make security-scan` | `security-deps` and `security-secrets`, as CI runs them | network, Docker |
| `make security-deps` | pip-audit over both lockfiles (`infra/scripts/dependency_audit.sh`) | network |
| `make security-secrets` | gitleaks over every commit on every ref (`infra/scripts/secret_scan.sh`); `SCAN_RANGE=<sha>..<sha>` limits it to a range, as CI does on a PR | Docker |
| `make security-images` | builds the four images, then Trivy with the CI policy (`infra/scripts/image_scan.sh`) | Docker |

The tools are pinned: pip-audit `2.10.1`, `ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0…`
and `aquasec/trivy:0.70.0@sha256:be1190af…` (the Trivy version `aquasecurity/trivy-action`
v0.36.0 installs in CI). The secret scan uses a bare mirror of the repository, so it
behaves the same in a normal clone, a git worktree and CI.

In CI, `.github/workflows/security.yml` runs the same scripts on every pull request, on
`main` and every Monday, so new advisories are noticed even without code changes.

## CI supply chain

Every third-party action is pinned to the commit its tag pointed to when it was pinned,
checked with `gh api repos/<owner>/<repo>/git/ref/tags/<tag>`:

| Action | Tag | Commit |
|---|---|---|
| `actions/checkout` | v4.4.0 | `11d5960a326750d5838078e36cf38b85af677262` |
| `actions/upload-artifact` | v4.6.2 | `ea165f8d65b6e75b540449e92b4886f43607fa02` |
| `astral-sh/setup-uv` | v6.8.0 | `d0cc045d04ccac9d8b7881df0226f9e82c39688e` |
| `astral-sh/setup-uv` | v7.6.0 | `37802adc94f370d6bfd71619e3f0bf239e1f3b78` |
| `aquasecurity/trivy-action` | v0.36.0 | `ed142fd0673e97e23eac54620cfb913e5ce36c25` |
| `anchore/sbom-action` | v0.24.3 | `66cbf4bc1f1c0d2edc94016e65bc221b6bb0ad6c` |

Every top-level workflow declares `permissions: contents: read` (the reusable
`image-build.yml` inherits its caller's); only the image publish job adds
`packages: write`, and the tag job `actions: read` (`images.yml`).

## Known limitations

- **No end-user authentication.** Public routes are open to anyone who can reach them.
  This is a course demonstration; an authentication service is separate work.
- **Network placement is the only protection** for the service-to-service and internal
  service APIs listed above. A deployment must not publish the service ports directly.
- **Kafka is PLAINTEXT** and the Compose databases publish their ports on the host. Both
  are local-only conveniences.
- **The web UI (PR #97)** reads the transition history from
  `/internal/v1/redemptions/{id}/transitions`. Locally nothing changes. In staging, the
  web service must send the operator token, or show the timeline from another source.
