.PHONY: install format lint test check contracts-check contracts-compat security-scan security-deps security-secrets security-images run-coordinator run-coordinator-saga run-wallet run-wallet-saga run-catalog run-catalog-saga run-ledger run-audit-consumer infra-up infra-status infra-smoke infra-logs infra-down infra-reset db-upgrade db-downgrade db-reset db-test test-2pc test-2pc-integration db-seed db-baseline db-prepared-list db-prepared-clean images-build images-check

COMPOSE_FILE ?= infra/compose.yaml
COMPOSE := docker compose -f $(COMPOSE_FILE)
BASE_REF ?= origin/main

install:
	uv sync --python 3.12

format:
	uv run ruff format services libs/contracts infra/scripts tests
	uv run ruff check --fix services libs/contracts infra/scripts tests

lint:
	uv run ruff format --check services libs/contracts infra/scripts tests
	uv run ruff check services libs/contracts infra/scripts tests

test:
	uv run pytest services libs/contracts/tests tests -q

contracts-check:
	uv run ruff format --check libs/contracts
	uv run ruff check libs/contracts
	uv run pytest libs/contracts/tests -q
	$(MAKE) contracts-compat BASE_REF=$(BASE_REF)

contracts-compat:
	PYTHONPATH=libs/contracts/src uv run python -m ledgermesh_contracts.compatibility --base-ref $(BASE_REF)

check: lint test

# Security scans (LM-042, docs/security.md). security-scan is what CI runs:
#   security-deps     pip-audit over both uv lockfiles (needs network)
#   security-secrets  gitleaks over every commit on every ref, or SCAN_RANGE=a..b (needs Docker)
#   security-images   build the images, then Trivy with the CI policy (needs Docker)
security-scan: security-deps security-secrets

security-deps:
	bash infra/scripts/dependency_audit.sh

security-secrets:
	bash infra/scripts/secret_scan.sh

# Local development only (LM-042): --reload and the Compose database password
# are refused anywhere but LEDGERMESH_ENV=local or test, which these default to.
LOCAL_ENV = LEDGERMESH_ENV=$${LEDGERMESH_ENV:-local}
REQUIRE_LOCAL = @case "$${LEDGERMESH_ENV:-local}" in local|test) ;; \
	*) echo "run-* targets are for local development; LEDGERMESH_ENV=$$LEDGERMESH_ENV" >&2; exit 1;; esac

run-coordinator:
	$(REQUIRE_LOCAL)
	$(LOCAL_ENV) uv run uvicorn services.coordinator.app.main:app --reload --port 8000

run-coordinator-saga:
	$(REQUIRE_LOCAL)
	$(LOCAL_ENV) COORDINATOR_DATABASE_URL=$${COORDINATOR_DATABASE_URL:-postgresql+psycopg://coordinator:ledgermesh_dev@localhost:54321/coordinator} \
	  CATALOG_URL=$${CATALOG_URL:-http://localhost:8002} \
	  uv run python -m services.coordinator.app.saga_worker

run-wallet:
	$(REQUIRE_LOCAL)
	$(LOCAL_ENV) uv run uvicorn services.wallet.app.main:app --reload --port 8001

run-wallet-saga:
	$(REQUIRE_LOCAL)
	$(LOCAL_ENV) WALLET_DATABASE_URL=$${WALLET_DATABASE_URL:-postgresql+psycopg://wallet:ledgermesh_dev@localhost:54322/wallet} \
	  uv run python -m services.wallet.app.saga_worker

run-catalog:
	$(REQUIRE_LOCAL)
	$(LOCAL_ENV) uv run uvicorn services.catalog.app.main:app --reload --port 8002

run-catalog-saga:
	$(REQUIRE_LOCAL)
	$(LOCAL_ENV) CATALOG_DATABASE_URL=$${CATALOG_DATABASE_URL:-postgresql+psycopg://catalog:ledgermesh_dev@localhost:54323/catalog} \
	  uv run python -m services.catalog.app.saga_worker

run-ledger:
	$(REQUIRE_LOCAL)
	$(LOCAL_ENV) uv run uvicorn services.audit_ledger.app.main:app --reload --port 8003

run-audit-consumer:
	$(REQUIRE_LOCAL)
	$(LOCAL_ENV) AUDIT_LEDGER_DATABASE_URL=$${AUDIT_LEDGER_DATABASE_URL:-postgresql+psycopg://audit_ledger:ledgermesh_dev@localhost:54324/audit_ledger} \
	  uv run python -m services.audit_ledger.app.event_worker

infra-up:
	$(COMPOSE) up --build -d --wait

infra-status:
	$(COMPOSE) ps

infra-smoke:
	bash infra/scripts/smoke-test.sh

infra-logs:
	$(COMPOSE) logs --follow --tail=200

infra-down:
	$(COMPOSE) down --remove-orphans

infra-reset:
	$(COMPOSE) down --volumes --remove-orphans

# Database migrations (LM-005). They target the Compose databases on their host
# ports; pass SERVICES="wallet catalog" to limit them. Rolling back deletes data,
# so it only runs when LEDGERMESH_ENV is explicitly local or test.
SERVICES ?=

db-upgrade:
	uv run python infra/scripts/db.py upgrade $(SERVICES)

# Destructive: the caller must say so explicitly, e.g. LEDGERMESH_ENV=local make db-reset.
# There is deliberately no default, so a URL override can never be reset by accident.
db-downgrade:
	uv run python infra/scripts/db.py downgrade $(SERVICES)

db-reset:
	uv run python infra/scripts/db.py reset $(SERVICES)

db-test:
	uv run pytest tests/db -q

# LM-017 uses four isolated databases and real PostgreSQL prepared transactions.
test-2pc-integration:
	mkdir -p artifacts
	uv run pytest tests/integration -q \
		--junitxml=artifacts/2pc-integration.xml

# Deterministic seed data (LM-006). Both change data, so both need an explicit
# LEDGERMESH_ENV=local or test, e.g. LEDGERMESH_ENV=local make db-baseline.
#   db-seed      write the baseline users and rewards (repeatable)
#   db-baseline  wipe every service database, migrate it, then seed it
db-seed:
	uv run python -m infra.scripts.seed seed $(SERVICES)

db-baseline:
	uv run python -m infra.scripts.seed baseline $(SERVICES)

# PostgreSQL prepared-transaction operations (LM-012). Listing is read-only.
# Cleanup rolls back unresolved transactions and therefore requires the same
# explicit local/test guard as the destructive migration targets.
db-prepared-list:
	uv run python -m infra.scripts.prepared_transactions list $(SERVICES)

db-prepared-clean:
	uv run python -m infra.scripts.prepared_transactions cleanup $(SERVICES)

# Production service images (LM-047). Builds the same images CI publishes,
# tagged locally with the current commit, then runs the image checks.
IMAGE_REVISION ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)

images-build:
	for spec in coordinator:coordinator:8000 wallet:wallet:8001 catalog:catalog:8002 audit_ledger:audit-ledger:8003; do \
	  service=$${spec%%:*}; rest=$${spec#*:}; name=$${rest%%:*}; port=$${rest#*:}; \
	  docker build -f infra/Dockerfile.service \
	    --build-arg SERVICE=$$service --build-arg SERVICE_NAME=$$name --build-arg PORT=$$port \
	    --build-arg LEDGERMESH_REVISION=$(IMAGE_REVISION) \
	    -t ledgermesh-$$name:$(IMAGE_REVISION) . || exit 1; \
	done

security-images: images-build
	for name in coordinator wallet catalog audit-ledger; do \
	  bash infra/scripts/image_scan.sh ledgermesh-$$name:$(IMAGE_REVISION) || exit 1; \
	done

images-check: images-build
	for spec in coordinator:8000 wallet:8001 catalog:8002 audit-ledger:8003; do \
	  name=$${spec%%:*}; port=$${spec#*:}; \
	  bash infra/scripts/check_image.sh ledgermesh-$$name:$(IMAGE_REVISION) $$name $$port || exit 1; \
	done

