"""LM-042 safeguards as wired into the Rewards Catalog Service."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from ledgermesh_messaging.safeguards import UnsafeConfigurationError, check_startup_configuration

from services.catalog.app.main import app, get_catalog_service, get_participant_service
from services.catalog.app.models import ParticipantResponse
from services.catalog.app.repository import database_url

LEAK = "postgresql+psycopg://catalog:hunter2@catalog-db/catalog: SELECT * FROM rewards"
PROBLEM_FIELDS = {"type", "title", "status", "detail"}


class FailingDependency:
    """Every call fails the way a broken database would; must never leak."""

    def __getattr__(self, _name):
        def fail(*_args, **_kwargs):
            raise RuntimeError(LEAK)

        return fail


class CommittingParticipant:
    def commit(self, transaction_id):
        return ParticipantResponse(
            transaction_id=transaction_id, state="COMMITTED", updated_at=datetime.now(UTC)
        )


@pytest.fixture(autouse=True)
def failing_dependencies(monkeypatch):
    monkeypatch.delenv("LEDGERMESH_ENV", raising=False)
    app.dependency_overrides[get_catalog_service] = FailingDependency
    app.dependency_overrides[get_participant_service] = FailingDependency
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


@pytest.mark.parametrize("operation", ["reservations", "releases"])
@pytest.mark.parametrize("quantity", [0, -1, 2**31, 2**64, "two"])
def test_invalid_quantities_are_rejected(operation, quantity):
    response = client().post(
        f"/v1/rewards/reward-1/{operation}",
        json={"transaction_id": str(uuid4()), "quantity": quantity},
    )

    assert_safe_problem(response, 422)


@pytest.mark.parametrize(
    "changes",
    [
        {"reward_id": ""},
        {"quantity": 2**31},
        {"expected_reward_version": -1},
        {"expected_reward_version": 2**63},
        {"expected_point_cost": 0},
        {"expected_point_cost": 2**63},
        {"unexpected": True},
    ],
)
def test_invalid_prepare_requests_are_rejected(changes):
    body = {
        "correlation_id": str(uuid4()),
        "reward_id": "reward-1",
        "quantity": 1,
        "expected_reward_version": 0,
        "expected_point_cost": 100,
        **changes,
    }

    response = client().post(f"/v1/transactions/{uuid4()}/prepare", json=body)

    assert_safe_problem(response, 422)


def test_malformed_json_bad_ids_bad_queries_and_oversized_bodies_are_rejected():
    malformed = client().post(
        "/v1/rewards/reward-1/fulfillments",
        headers={"content-type": "application/json"},
        content=b"[",
    )
    bad_id = client().get("/v1/transactions/12345")
    bad_query = client().get("/v1/rewards", params={"available_only": "perhaps"})
    oversized = client().post(
        "/v1/rewards/reward-1/reservations",
        headers={"content-type": "application/json"},
        content=b'{"x": "' + b"x" * 2_000_000 + b'"}',
    )

    assert_safe_problem(malformed, 422)
    assert_safe_problem(bad_id, 422)
    assert_safe_problem(bad_query, 422)
    assert_safe_problem(oversized, 413)


def test_unhandled_errors_are_generic_problems():
    response = client().get("/v1/rewards/reward-1")

    body = assert_safe_problem(response, 500)
    assert body["type"] == "urn:ledgermesh:catalog:internal-error"


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json", "/internal/anything"])
def test_docs_and_internal_paths_are_closed_by_default(path):
    assert_safe_problem(client().get(path), 404)


def test_participant_routes_stay_open_in_every_environment(monkeypatch):
    """The Coordinator calls prepare/commit/abort in staging too (network-isolated)."""
    monkeypatch.setenv("LEDGERMESH_ENV", "production")
    app.dependency_overrides[get_participant_service] = CommittingParticipant

    response = client().post(f"/v1/transactions/{uuid4()}/commit")

    assert response.status_code == 200
    assert response.json()["state"] == "COMMITTED"


def test_deployment_refuses_the_development_database_password(monkeypatch):
    monkeypatch.setenv("LEDGERMESH_ENV", "staging")
    for name in ("CATALOG_DATABASE_URL", "DATABASE_URL", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(UnsafeConfigurationError, match="CATALOG_DATABASE_URL"):
        check_startup_configuration("catalog", database_url=database_url)

    monkeypatch.setenv(
        "CATALOG_DATABASE_URL", "postgresql+psycopg://catalog:s3cret@db:5432/catalog"
    )
    check_startup_configuration("catalog", database_url=database_url)
