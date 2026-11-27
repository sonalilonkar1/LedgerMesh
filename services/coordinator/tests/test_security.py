"""LM-042 safeguards as wired into the Coordinator."""

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from ledgermesh_messaging.safeguards import (
    OPERATOR_TOKEN_HEADER,
    UnsafeConfigurationError,
    check_startup_configuration,
)

from services.coordinator.app.main import (
    app,
    get_coordinator_service,
    get_two_phase_commit_recovery,
    get_two_phase_commit_workflow,
)
from services.coordinator.app.repository import database_url

LEAK = (
    "postgresql+psycopg://coordinator:hunter2@coordinator-db/coordinator: SELECT * FROM redemptions"
)
TOKEN = "o" * 40
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
    monkeypatch.delenv("LEDGERMESH_OPERATOR_TOKEN", raising=False)
    for dependency in (
        get_coordinator_service,
        get_two_phase_commit_workflow,
        get_two_phase_commit_recovery,
    ):
        app.dependency_overrides[dependency] = FailingDependency
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


INTERNAL_ROUTES = [
    ("get", f"/internal/v1/redemptions/{uuid4()}/transitions"),
    ("post", f"/internal/v1/redemptions/{uuid4()}/execute-2pc"),
    ("get", "/internal/v1/2pc/in-doubt"),
    ("post", "/internal/v1/2pc/recovery/run"),
]


@pytest.mark.parametrize("environment", [None, "staging", "production"])
@pytest.mark.parametrize(("method", "path"), INTERNAL_ROUTES)
def test_internal_routes_are_hidden_outside_local_and_test(monkeypatch, environment, method, path):
    if environment:
        monkeypatch.setenv("LEDGERMESH_ENV", environment)

    response = getattr(client(), method)(path)

    # 404, not 500: the failing dependency was never reached.
    assert_safe_problem(response, 404)


@pytest.mark.parametrize(("method", "path"), INTERNAL_ROUTES)
def test_operator_token_reaches_internal_routes_in_a_deployment(monkeypatch, method, path):
    monkeypatch.setenv("LEDGERMESH_ENV", "staging")
    monkeypatch.setenv("LEDGERMESH_OPERATOR_TOKEN", TOKEN)

    response = getattr(client(), method)(path, headers={OPERATOR_TOKEN_HEADER: TOKEN})

    # The route ran (and hit the failing dependency), and still leaked nothing.
    assert_safe_problem(response, 500)


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json"])
def test_api_docs_are_closed_by_default_and_open_locally(monkeypatch, path):
    assert_safe_problem(client().get(path), 404)

    monkeypatch.setenv("LEDGERMESH_ENV", "local")
    assert client().get(path).status_code == 200


def test_health_stays_public():
    assert client().get("/health").json() == {"status": "ok", "service": "coordinator"}


@pytest.mark.parametrize(
    ("headers", "body"),
    [
        ({}, {"user_id": "u", "reward_id": "r", "quantity": 1, "coordination_mode": "saga"}),
        (
            {"Idempotency-Key": "k" * 129},
            {"user_id": "u", "reward_id": "r", "quantity": 1, "coordination_mode": "saga"},
        ),
        ({"Idempotency-Key": "k"}, {"user_id": "", "reward_id": "r", "quantity": 1}),
        (
            {"Idempotency-Key": "k"},
            {"user_id": "u", "reward_id": "r", "quantity": 0, "coordination_mode": "saga"},
        ),
        (
            {"Idempotency-Key": "k"},
            {"user_id": "u", "reward_id": "r", "quantity": -3, "coordination_mode": "saga"},
        ),
        (
            {"Idempotency-Key": "k"},
            {"user_id": "u", "reward_id": "r", "quantity": 2**31, "coordination_mode": "saga"},
        ),
        (
            {"Idempotency-Key": "k"},
            {"user_id": "u", "reward_id": "r", "quantity": 2**40, "coordination_mode": "saga"},
        ),
        (
            {"Idempotency-Key": "k"},
            {"user_id": "u", "reward_id": "r", "quantity": 1, "coordination_mode": "xa"},
        ),
        (
            {"Idempotency-Key": "k"},
            {
                "user_id": "u",
                "reward_id": "r",
                "quantity": 1,
                "coordination_mode": "saga",
                "is_admin": True,
            },
        ),
    ],
)
def test_invalid_redemption_requests_are_rejected_before_the_service(headers, body):
    response = client().post("/v1/redemptions", headers=headers, json=body)

    assert_safe_problem(response, 422)


def test_malformed_json_and_bad_ids_are_rejected():
    malformed = client().post(
        "/v1/redemptions",
        headers={"Idempotency-Key": "k", "content-type": "application/json"},
        content=b'{"user_id": ',
    )
    bad_id = client().get("/v1/redemptions/not-a-uuid")

    assert_safe_problem(malformed, 422)
    assert_safe_problem(bad_id, 422)
    assert "not-a-uuid" not in bad_id.text


def test_oversized_request_body_is_rejected():
    body = b'{"user_id": "' + b"u" * 2_000_000 + b'"}'

    response = client().post(
        "/v1/redemptions",
        headers={"Idempotency-Key": "k", "content-type": "application/json"},
        content=body,
    )

    assert_safe_problem(response, 413)


def test_unhandled_errors_are_generic_problems():
    response = client().get(f"/v1/redemptions/{uuid4()}")

    body = assert_safe_problem(response, 500)
    assert body["type"] == "urn:ledgermesh:coordinator:internal-error"
    assert response.headers["x-correlation-id"] in body["detail"]


@pytest.mark.parametrize("environment", ["staging", "production"])
def test_deployment_refuses_the_development_database_password(monkeypatch, environment):
    monkeypatch.setenv("LEDGERMESH_ENV", environment)
    for name in ("COORDINATOR_DATABASE_URL", "DATABASE_URL", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(UnsafeConfigurationError, match="COORDINATOR_DATABASE_URL"):
        check_startup_configuration("coordinator", database_url=database_url)

    monkeypatch.setenv(
        "DATABASE_URL", "postgresql+psycopg://coordinator:ledgermesh_dev@db:5432/coordinator"
    )
    with pytest.raises(UnsafeConfigurationError):
        check_startup_configuration("coordinator", database_url=database_url)

    monkeypatch.setenv("POSTGRES_PASSWORD", "a-real-secret")
    monkeypatch.delenv("DATABASE_URL")
    check_startup_configuration("coordinator", database_url=database_url)
