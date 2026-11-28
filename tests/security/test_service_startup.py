"""Each service refuses to start in a deployment that still uses local defaults (LM-042).

Imports the real ``main`` module in a fresh interpreter, which is what
``uvicorn services.<service>.app.main:app`` does at startup. No database is
needed: the check runs before any connection is made.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVICES = {
    "coordinator": "COORDINATOR_DATABASE_URL",
    "wallet": "WALLET_DATABASE_URL",
    "catalog": "CATALOG_DATABASE_URL",
    "audit_ledger": "AUDIT_LEDGER_DATABASE_URL",
}
CLEARED = {
    "LEDGERMESH_ENV",
    "LEDGERMESH_OPERATOR_TOKEN",
    "DATABASE_URL",
    "POSTGRES_PASSWORD",
    *SERVICES.values(),
}


def start(service: str, **environment: str) -> subprocess.CompletedProcess:
    env = {name: value for name, value in os.environ.items() if name not in CLEARED}
    env.update(environment, PYTHONPATH=str(ROOT))
    return subprocess.run(
        [sys.executable, "-c", f"import services.{service}.app.main"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.mark.parametrize("service", SERVICES)
def test_deployment_without_a_real_database_secret_refuses_to_start(service):
    result = start(service, LEDGERMESH_ENV="staging")

    assert result.returncode != 0
    assert "UnsafeConfigurationError" in result.stderr
    assert SERVICES[service] in result.stderr
    assert "ledgermesh_dev" not in result.stderr


@pytest.mark.parametrize(
    "secret",
    [
        {"POSTGRES_PASSWORD": ""},
        {"DATABASE_URL": "postgresql+psycopg://svc@db:5432/svc"},
        {"DATABASE_URL": "postgresql+psycopg://svc:@db:5432/svc"},
        {"DATABASE_URL": "postgresql+psycopg://svc:x@db/svc?password=ledgermesh_dev"},
        {"DATABASE_URL": "postgresql+psycopg:///svc?host=db&user=svc&password=ledgermesh_dev"},
    ],
    ids=["empty-postgres-password", "no-password", "empty-password", "query", "query-only"],
)
def test_deployment_with_an_empty_or_hidden_dev_password_refuses_to_start(secret):
    result = start("wallet", LEDGERMESH_ENV="staging", **secret)

    assert result.returncode != 0
    assert "UnsafeConfigurationError" in result.stderr
    assert "ledgermesh_dev" not in result.stderr


@pytest.mark.parametrize("service", SERVICES)
def test_deployment_with_real_secrets_starts(service):
    result = start(
        service,
        LEDGERMESH_ENV="staging",
        DATABASE_URL="postgresql+psycopg://svc:a-real-secret@db:5432/svc",
        LEDGERMESH_OPERATOR_TOKEN="x" * 40,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("environment", ["local", None])
def test_local_and_unset_environments_start_without_secrets(environment):
    """Local development uses Compose defaults; an unset image must answer /health (LM-047)."""
    extra = {"LEDGERMESH_ENV": environment} if environment else {}

    result = start("wallet", **extra)

    assert result.returncode == 0, result.stderr
