# Service images (LM-047)

Every deployable service is published as its own immutable image on GitHub Container Registry (GHCR):

| Image | Service | Port | `/health` service |
|---|---|---:|---|
| `ghcr.io/sonalilonkar1/ledgermesh-coordinator` | Coordinator | 8000 | `coordinator` |
| `ghcr.io/sonalilonkar1/ledgermesh-wallet` | Wallet | 8001 | `wallet` |
| `ghcr.io/sonalilonkar1/ledgermesh-catalog` | Catalog | 8002 | `catalog` |
| `ghcr.io/sonalilonkar1/ledgermesh-audit-ledger` | Audit Ledger | 8003 | `audit-ledger` |

All four are built from `infra/Dockerfile.service`, which Docker Compose also uses locally.

## What is in an image

- **Python 3.12** (`python:3.12-slim-bookworm`), with a virtualenv built from `uv.lock` using `uv sync --locked --no-dev --no-editable`. The same commit always resolves to the same dependency graph.
- **Only runtime code:** the `services/` tree and the virtualenv, which includes the installed messaging library. There are no build tools (the image has no `uv`), no tests, no docs and no `libs/contracts`, which is used only for contract validation in CI.
- **Not in the image:** `.git`, `.github`, any `.env` file, `infra/.env`, `.netrc`, `.git-credentials`, keys or local virtualenvs. `.dockerignore` excludes them from the build context, and `check_image.sh` verifies they're absent.
- **A non-root user**, UID/GID 10001:10001.
- **An OCI `HEALTHCHECK`** that calls the service's own `/health`.

### Start command and migrations

The default command, `infra/docker/start-service.sh`:
1. logs `starting <service> revision=<git sha> version=<version>`;
2. runs migrations only if `RUN_MIGRATIONS=true` (default `false`, so an image can start without a database);
3. starts uvicorn for that service.

Deployments set `RUN_MIGRATIONS=true` and `DATABASE_URL`. Any other module in the image can be run by overriding the command, for example the Saga worker:

```bash
docker run --rm -e DATABASE_URL=... -e KAFKA_BOOTSTRAP_SERVERS=... \
  ghcr.io/sonalilonkar1/ledgermesh-wallet:<sha> python -m services.wallet.app.saga_worker
```

## Tags and labels

- **`<full commit sha>`** is the deployment source of truth. Deploy by SHA, so a rollback is just the previous SHA.
- **`<semver>`** (e.g. `1.2.0`) is added when a `v1.2.0` tag is pushed. Create release tags only on commits main has already published.
- **There is no `latest` tag.**
- **OCI labels:**
  - `org.opencontainers.image.source`
  - `org.opencontainers.image.revision`
  - `org.opencontainers.image.created`
  - `org.opencontainers.image.title`
  - `org.opencontainers.image.version`

To see which revision a running container is:

```bash
docker inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' <container>
docker logs <container> | grep revision=
```

## CI (`.github/workflows/images.yml`)

| Event | What happens |
|---|---|
| Pull request | Build every image, then run `check_image.sh`, a Trivy scan and an SBOM. **Nothing is pushed.** |
| Push to `main` | Runs only after `core-ci` (lint, unit, database and 2PC integration tests) **succeeds for that commit**, then repeats the checks and publishes SHA-tagged images |
| `v*` tag | Checks, then publishes with the SHA and semver tags |

- `core-ci` only runs when service, test or infra paths change, so a docs-only merge doesn't republish images. That's expected, since the images haven't changed.
- **Platform:** `linux/amd64`, the staging architecture. If staging (LM-048) uses ARM, add `linux/arm64` to `PLATFORM` in the workflow.
- **Permissions:**
  - The check job has `contents: read` only.
  - Only the publish job has `packages: write`.
  - It logs in with the short-lived `GITHUB_TOKEN`; no personal tokens or secrets are used, and none are passed as build arguments.

### Vulnerability policy

- **Trivy fails the build** on any **HIGH or CRITICAL vulnerability that has a fix available**.
- **Unfixed** findings are reported but don't fail.
- **Exceptions** go in `.trivyignore`, with the CVE, the reason, the approver and a review date.

The scan report (`trivy-<image>`) and the SPDX SBOM (`sbom-<image>.spdx.json`) are attached to every workflow run.

## Build and check locally

```bash
make images-check
```

This builds all four images (tagged `ledgermesh-<service>:<current sha>`), then for each one:
- starts it,
- checks its `/health`,
- checks that it runs as a non-root user,
- checks that no `.env`, git metadata or credentials are present,
- checks that it logs its revision.

To build only one image:

```bash
docker build -f infra/Dockerfile.service \
  --build-arg SERVICE=wallet --build-arg SERVICE_NAME=wallet --build-arg PORT=8001 \
  -t ledgermesh-wallet:local .
bash infra/scripts/check_image.sh ledgermesh-wallet:local wallet 8001
```

## Pulling from a staging host

The first publish creates the four packages under the repository owner's account. Each image carries `org.opencontainers.image.source`, so GitHub links the package to the repository.

- **Visibility:** a new package may start private. The repository owner sets it under **GitHub → Your profile → Packages → `<package>` → Package settings → Change visibility**.
  - **Public:** anyone can pull, with no login. Fine for a course project with no secrets in the images.
  - **Private:** the staging host needs a token with only the `read:packages` scope, stored as a secret on that host, never in the repository:
    ```bash
    echo "$GHCR_READ_TOKEN" | docker login ghcr.io -u <github-user> --password-stdin
    docker pull ghcr.io/sonalilonkar1/ledgermesh-wallet:<sha>
    ```
- **Manage Actions access:** in the same package settings, make sure the LedgerMesh repository has **Write** access, so later workflow runs can push new tags.

Provisioning the host and deploying are LM-048 and LM-049.
