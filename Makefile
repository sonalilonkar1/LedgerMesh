.PHONY: install format lint test check contracts-check contracts-compat run-coordinator run-coordinator-saga run-wallet run-wallet-saga run-catalog run-catalog-saga run-ledger run-audit-consumer infra-up infra-status infra-smoke infra-logs infra-down infra-reset db-upgrade db-downgrade db-reset db-test test-2pc test-2pc-integration db-seed db-baseline db-prepared-list db-prepared-clean

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

run-coordinator:
	uv run uvicorn services.coordinator.app.main:app --reload --port 8000

run-coordinator-saga:
	COORDINATOR_DATABASE_URL=$${COORDINATOR_DATABASE_URL:-postgresql+psycopg://coordinator:ledgermesh_dev@localhost:54321/coordinator} \
	  CATALOG_URL=$${CATALOG_URL:-http://localhost:8002} \
	  uv run python -m services.coordinator.app.saga_worker

run-wallet:
	uv run uvicorn services.wallet.app.main:app --reload --port 8001

run-wallet-saga:
	WALLET_DATABASE_URL=$${WALLET_DATABASE_URL:-postgresql+psycopg://wallet:ledgermesh_dev@localhost:54322/wallet} \
	  uv run python -m services.wallet.app.saga_worker

run-catalog:
	uv run uvicorn services.catalog.app.main:app --reload --port 8002

run-catalog-saga:
	CATALOG_DATABASE_URL=$${CATALOG_DATABASE_URL:-postgresql+psycopg://catalog:ledgermesh_dev@localhost:54323/catalog} \
	  uv run python -m services.catalog.app.saga_worker

run-ledger:
	uv run uvicorn services.audit_ledger.app.main:app --reload --port 8003

run-audit-consumer:
	AUDIT_LEDGER_DATABASE_URL=$${AUDIT_LEDGER_DATABASE_URL:-postgresql+psycopg://audit_ledger:ledgermesh_dev@localhost:54324/audit_ledger} \
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

