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
- **Pinned bases:** the Python and `uv` base images are pinned by digest in `infra/Dockerfile.service`, so an upstream tag moving can't change what a commit builds. To take base-image security fixes, bump the tag and digest together in a PR; the image checks and Trivy then run on the new base before it can publish.
- **Only runtime code:** the `services/` tree and the virtualenv, which includes the installed messaging library. There are no build tools (the image has no `uv`), no tests, no docs and no `libs/contracts`, which is used only for contract validation in CI.
- **Not in the image:** `.git`, `.github`, any `.env` file, `infra/.env`, `.netrc`, `.git-credentials`, private keys or local virtualenvs. `.dockerignore` excludes them from the build context, and `check_image.sh` verifies they're absent: it fails on any `*.key` file, and on any `*.pem` that contains a private key (public CA bundles such as certifi's `cacert.pem` are expected).
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

- **`<full commit sha>`** is the deployment source of truth. Deploy by SHA, so a rollback is just the previous SHA. **SHA tags are immutable:** if a tag already exists, a re-run leaves it untouched instead of pushing a new digest.
- **`<semver>`** (e.g. `1.2.0`) is added when a `v1.2.0` tag is pushed. It is an alias for the digest already published under that commit's SHA, never a rebuild, so `:1.2.0` and `:<sha>` are the identical tested image. The release job refuses tags that aren't `vMAJOR.MINOR.PATCH`, aren't on `main`, have no successful `core-ci` run, or whose SHA image was never published.
- **There is no `latest` tag.**
- **OCI labels:**
  - `org.opencontainers.image.source`
  - `org.opencontainers.image.revision`
  - `org.opencontainers.image.created`
  - `org.opencontainers.image.title`
  - `org.opencontainers.image.version` (the commit SHA; semver tags are aliases added after the build)

To see which revision a running container is:

```bash
docker inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' <container>
docker logs <container> | grep revision=
```

## CI (`.github/workflows/images.yml`, `image-build.yml`)

Each image is **built once per run**. `check_image.sh`, Trivy and the SBOM all run against that local image, and on `main` that same image is pushed. Nothing is rebuilt between scanning and publishing.

| Event | What happens |
|---|---|
| Pull request | Build each image once, then run `check_image.sh`, a Trivy scan and an SBOM. **Nothing is pushed.** |
| Push to `main` | Runs only after `core-ci` (lint, unit, database and 2PC integration tests) **succeeds for that commit**. Builds once, checks, scans, then pushes that exact image under the SHA. The step fails unless the registry digest equals the digest that was pushed, and the job summary records the scanned image id and the published digest. |
| `v*` tag | **No build.** Verifies the commit is on `main` and passed `core-ci`, then adds the semver tag to the already-published SHA digest and checks the digests match. |

- `core-ci` has **no path filter on pushes to `main`**, so every merge, including one that only changes `infra/docker/` or `.dockerignore`, is tested and published. A docs-only merge publishes a new SHA tag with the same contents, which is harmless.
- **Pinned actions:** every third-party action is pinned to a full commit SHA, with its version in a comment. Login, tagging and pushing use the `docker` CLI directly, so there are no extra actions in the publish path.
- **Platform:** `linux/amd64`, the staging architecture. If staging (LM-048) uses ARM, add `linux/arm64` to `PLATFORM` in `image-build.yml`.
- **Permissions:**
  - Pull-request runs have `contents: read` only.
  - Only the `main` publish job and the release job have `packages: write`; the release job also has `actions: read` to look up the `core-ci` result.
  - It logs in with the short-lived `GITHUB_TOKEN`; no personal tokens or secrets are used, and none are passed as build arguments.

### Releasing

1. Merge to `main` and wait for **Service images / publish** to finish for that commit.
2. Tag that commit and push the tag:
   ```bash
   git tag v1.2.0 <sha>
   git push origin v1.2.0
   ```
3. **Service images / release** adds `1.2.0` to the four SHA images.

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
