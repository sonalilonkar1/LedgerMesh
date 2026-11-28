"""LM-042 safeguards as wired into the Audit Ledger Service."""

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from ledgermesh_messaging.safeguards import UnsafeConfigurationError, check_startup_configuration

from services.audit_ledger.app.main import app, get_audit_ledger_service
from services.audit_ledger.app.repository import database_url
from services.audit_ledger.tests.test_core_api import envelope_payload

LEAK = "postgresql+psycopg://audit_ledger:hunter2@audit-ledger-db/x: SELECT * FROM audit_events"
PROBLEM_FIELDS = {"type", "title", "status", "detail"}


class FailingDependency:
    """Every call fails the way a broken database would; must never leak."""

    def __getattr__(self, _name):
        def fail(*_args, **_kwargs):
            raise RuntimeError(LEAK)

        return fail


@pytest.fixture(autouse=True)
def failing_dependencies(monkeypatch):
    monkeypatch.delenv("LEDGERMESH_ENV", raising=False)
    app.dependency_overrides[get_audit_ledger_service] = FailingDependency
    yield
    app.dependency_overrides.clear()


def client() -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


def assert_safe_problem(response, status: int) -> dict:
    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/problem+json")
    assert set(response.json()) == PROBLEM_FIELDS
    for leak in ("hunter2", "SELECT", "Traceback", "RuntimeError", "postgresql", '"input"'):
        assert leak not in response.text
    return response.json()


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": 2},
        {"event_type": "Not.Valid"},
        {"event_type": "a." + "b" * 300},
        {"aggregate_type": "x" * 256},
        {"aggregate_id": ""},
        {"aggregate_version": -1},
        {"aggregate_version": 2**63},
        {"workflow_step": 2**31},
        {"fencing_token": -1},
        {"occurred_at": "2026-11-27T10:00:00"},
        {"payload": "not an object"},
        {"unexpected": True},
    ],
)
def test_invalid_audit_events_are_rejected(changes):
    response = client().post("/v1/audit-events", json={**envelope_payload(), **changes})

    assert_safe_problem(response, 422)


def test_malformed_json_bad_ids_and_oversized_bodies_are_rejected():
    malformed = client().post(
        "/v1/audit-events", headers={"content-type": "application/json"}, content=b"{]"
    )
    bad_id = client().get("/v1/transactions/not-a-uuid/events")
    oversized = client().post(
        "/v1/audit-events",
        json={**envelope_payload(), "payload": {"blob": "x" * 2_000_000}},
    )

    assert_safe_problem(malformed, 422)
    assert_safe_problem(bad_id, 422)
    assert_safe_problem(oversized, 413)


def test_unhandled_errors_are_generic_problems():
    response = client().get(f"/v1/transactions/{uuid4()}/events")

    body = assert_safe_problem(response, 500)
    assert body["type"] == "urn:ledgermesh:audit-ledger:internal-error"


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json", "/internal/anything"])
def test_docs_and_internal_paths_are_closed_by_default(path):
    assert_safe_problem(client().get(path), 404)


def test_deployment_refuses_the_development_database_password(monkeypatch):
    monkeypatch.setenv("LEDGERMESH_ENV", "staging")
    for name in ("AUDIT_LEDGER_DATABASE_URL", "DATABASE_URL", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(UnsafeConfigurationError, match="AUDIT_LEDGER_DATABASE_URL"):
        check_startup_configuration("audit-ledger", database_url=database_url)

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://audit_ledger:s3cret@db:5432/a")
    check_startup_configuration("audit-ledger", database_url=database_url)
