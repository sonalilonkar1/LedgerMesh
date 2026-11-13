.PHONY: install format lint test check run-coordinator run-wallet run-catalog run-ledger infra-up infra-status infra-smoke infra-logs infra-down infra-reset db-upgrade db-downgrade db-reset db-test

COMPOSE_FILE ?= infra/compose.yaml
COMPOSE := docker compose -f $(COMPOSE_FILE)

install:
	uv sync --python 3.12

format:
	uv run ruff format services libs/contracts tests
	uv run ruff check --fix services libs/contracts tests

lint:
	uv run ruff format --check services libs/contracts tests
	uv run ruff check services libs/contracts tests

test:
	uv run pytest services libs/contracts/tests tests -q

contracts-check:
	uv run ruff format --check libs/contracts
	uv run ruff check libs/contracts
	uv run pytest libs/contracts/tests -q

check: lint test

run-coordinator:
	uv run uvicorn services.coordinator.app.main:app --reload --port 8000

run-wallet:
	uv run uvicorn services.wallet.app.main:app --reload --port 8001

run-catalog:
	uv run uvicorn services.catalog.app.main:app --reload --port 8002

run-ledger:
	uv run uvicorn services.audit_ledger.app.main:app --reload --port 8003

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

