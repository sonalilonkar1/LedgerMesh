"""Isolated four-database test environment for LM-017."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from infra.scripts.prepared_transactions import cleanup_prepared
from tests.db.conftest import upgrade
from tests.integration.invariants import check_project_invariants

SERVICES = ("coordinator", "wallet", "catalog", "audit_ledger")
REPORT: dict[str, object] = {
    "scenarios": [],
    "invariant_violations": [],
    "invariant_checks": [],
}
SERVER_URL = os.environ.get(
    "LM_TEST_POSTGRES_URL",
    "postgresql+psycopg://wallet:ledgermesh_dev@localhost:54322/wallet",
)


@dataclass
class ServiceDatabases:
    urls: dict[str, str]
    engines: dict[str, Engine]


@pytest.fixture(scope="session")
def lm017_report() -> dict[str, object]:
    return REPORT


@pytest.fixture(autouse=True)
def verify_invariants_after_integration_test(request, service_databases):
    """Automatically check each integration test's final cross-service state."""
    yield
    if request.node.get_closest_marker("invariant_checker"):
        return

    report = check_project_invariants(service_databases.engines)
    REPORT["invariant_checks"].append(
        {
            "test": request.node.nodeid,
            "count": report.count,
            "details": list(report.violations),
        }
    )
    assert report.count == 0, "\n".join(report.violations)


def pytest_sessionfinish(session, exitstatus) -> None:
    del session, exitstatus
    if not REPORT["scenarios"] and not REPORT["invariant_checks"]:
        return
    path = Path(os.environ.get("LM017_REPORT_PATH", "artifacts/2pc-integration.json"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(REPORT, indent=2, sort_keys=True) + "\n")


@pytest.fixture
def service_databases() -> Iterator[ServiceDatabases]:
    """Create one migrated database per service and always remove prepared xacts."""
    admin = create_engine(SERVER_URL, isolation_level="AUTOCOMMIT")
    suffix = uuid4().hex[:10]
    names = {service: f"lm017_{service}_{suffix}" for service in SERVICES}
    created_names: list[str] = []
    urls: dict[str, str] = {}
    engines: dict[str, Engine] = {}
    try:
        try:
            with admin.connect() as connection:
                for name in names.values():
                    connection.execute(text(f'CREATE DATABASE "{name}"'))
                    created_names.append(name)
        except OperationalError as error:
            if "LM_TEST_POSTGRES_URL" in os.environ:
                raise
            pytest.skip(f"PostgreSQL is not reachable; run make infra-up first ({error.orig})")

        for service, name in names.items():
            url = make_url(SERVER_URL).set(database=name).render_as_string(hide_password=False)
            upgrade(service, url)
            urls[service] = url
            engines[service] = create_engine(url, pool_pre_ping=True)
        yield ServiceDatabases(urls, engines)
    finally:
        for engine in engines.values():
            cleanup_prepared(engine, report=False)
            engine.dispose()
        if created_names:
            with admin.connect() as connection:
                for name in created_names:
                    connection.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()
