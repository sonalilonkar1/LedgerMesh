.PHONY: install format lint test check run-coordinator run-wallet run-catalog run-ledger

install:
	uv sync --python 3.12

format:
	uv run ruff format services libs/contracts tests
	uv run ruff check --fix services libs/contracts tests

lint:
	uv run ruff format --check services libs/contracts tests
	uv run ruff check services libs/contracts tests

test:
	uv run pytest services -q

check: lint test

run-coordinator:
	uv run uvicorn services.coordinator.app.main:app --reload --port 8000

run-wallet:
	uv run uvicorn services.wallet.app.main:app --reload --port 8001

run-catalog:
	uv run uvicorn services.catalog.app.main:app --reload --port 8002

run-ledger:
	uv run uvicorn services.audit_ledger.app.main:app --reload --port 8003
