"""LM-042 safeguards as wired into the Wallet Service."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from ledgermesh_messaging.safeguards import UnsafeConfigurationError, check_startup_configuration

from services.wallet.app.main import app, get_participant_service, get_wallet_service
from services.wallet.app.models import ParticipantResponse
from services.wallet.app.repository import database_url

LEAK = "postgresql+psycopg://wallet:hunter2@wallet-db/wallet: SELECT * FROM wallets"
PROBLEM_FIELDS = {"type", "title", "status", "detail"}


class FailingDependency:
    """Every call fails the way a broken database would; must never leak."""

    def __getattr__(self, _name):
        def fail(*_args, **_kwargs):
            raise RuntimeError(LEAK)

        return fail


class PreparingParticipant:
    def prepare(self, transaction_id, _request):
        return ParticipantResponse(
            transaction_id=transaction_id, state="PREPARED", updated_at=datetime.now(UTC)
        )


@pytest.fixture(autouse=True)
def failing_dependencies(monkeypatch):
    monkeypatch.delenv("LEDGERMESH_ENV", raising=False)
    app.dependency_overrides[get_wallet_service] = FailingDependency
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


@pytest.mark.parametrize("points", [0, -1, 2**63, 10**30, "many", None])
def test_invalid_deduction_amounts_are_rejected(points):
    response = client().post(
        "/v1/wallets/user-1/deductions",
        json={"transaction_id": str(uuid4()), "points": points},
    )

    assert_safe_problem(response, 422)


@pytest.mark.parametrize(
    "body",
    [
        {"correlation_id": "not-a-uuid", "user_id": "u", "points": 1},
        {"correlation_id": str(uuid4()), "user_id": "", "points": 1},
        {"correlation_id": str(uuid4()), "user_id": "u", "points": 2**63},
        {"correlation_id": str(uuid4()), "user_id": "u", "points": -1},
        {"correlation_id": str(uuid4()), "user_id": "u", "points": 1, "extra": 1},
    ],
)
def test_invalid_prepare_requests_are_rejected(body):
    response = client().post(f"/v1/transactions/{uuid4()}/prepare", json=body)

    assert_safe_problem(response, 422)


def test_malformed_json_bad_ids_and_oversized_bodies_are_rejected():
    malformed = client().post(
        "/v1/wallets/user-1/refunds",
        headers={"content-type": "application/json"},
        content=b"{",
    )
    bad_id = client().post("/v1/transactions/not-a-uuid/commit")
    oversized = client().post(
        "/v1/wallets/user-1/deductions",
        headers={"content-type": "application/json"},
        content=b'{"x": "' + b"x" * 2_000_000 + b'"}',
    )

    assert_safe_problem(malformed, 422)
    assert_safe_problem(bad_id, 422)
    assert_safe_problem(oversized, 413)


def test_unhandled_errors_are_generic_problems():
    response = client().get("/v1/wallets/user-1")

    body = assert_safe_problem(response, 500)
    assert body["type"] == "urn:ledgermesh:wallet:internal-error"


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json", "/internal/anything"])
def test_docs_and_internal_paths_are_closed_by_default(path):
    assert_safe_problem(client().get(path), 404)


def test_participant_routes_stay_open_in_every_environment(monkeypatch):
    """The Coordinator calls prepare/commit/abort in staging too (network-isolated)."""
    monkeypatch.setenv("LEDGERMESH_ENV", "production")
    app.dependency_overrides[get_participant_service] = PreparingParticipant
    transaction_id = uuid4()

    response = client().post(
        f"/v1/transactions/{transaction_id}/prepare",
        json={"correlation_id": str(uuid4()), "user_id": "u", "points": 10},
    )

    assert response.status_code == 200
    assert response.json()["state"] == "PREPARED"


def test_deployment_refuses_the_development_database_password(monkeypatch):
    monkeypatch.setenv("LEDGERMESH_ENV", "staging")
    for name in ("WALLET_DATABASE_URL", "DATABASE_URL", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(UnsafeConfigurationError, match="WALLET_DATABASE_URL"):
        check_startup_configuration("wallet", database_url=database_url)

    monkeypatch.setenv("WALLET_DATABASE_URL", "postgresql+psycopg://wallet:s3cret@db:5432/wallet")
    check_startup_configuration("wallet", database_url=database_url)
