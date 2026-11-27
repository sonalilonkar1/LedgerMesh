"""The shared LM-042 safeguards, exercised on a minimal FastAPI app.

Each service's own ``test_security.py`` checks the wiring and its request
models; this file covers the mechanism once.
"""

import json
import logging

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from ledgermesh_messaging.logs import JsonFormatter
from ledgermesh_messaging.safeguards import (
    DEV_DATABASE_PASSWORD,
    OPERATOR_TOKEN_HEADER,
    OPERATOR_TOKEN_VARIABLE,
    UnsafeConfigurationError,
    check_startup_configuration,
    install_safeguards,
    operator_token_matches,
    require_safe_database_url,
)
from pydantic import BaseModel, ConfigDict, Field

TOKEN = "t" * 40
SECRET_URL = "postgresql+psycopg://svc:hunter2-secret@db:5432/svc"
DEV_URL = f"postgresql+psycopg://svc:{DEV_DATABASE_PASSWORD}@db:5432/svc"
PROBLEM_FIELDS = {"type", "title", "status", "detail"}


class Item(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=8)
    quantity: int = Field(gt=0, le=10)


def make_app(**kwargs) -> FastAPI:
    app = FastAPI()

    @app.get("/internal/v1/things")
    def internal_things() -> list[str]:
        return ["thing"]

    @app.post("/v1/items")
    def create_item(item: Item) -> Item:
        return item

    @app.get("/v1/boom")
    def boom() -> None:
        raise RuntimeError(f"could not connect to {SECRET_URL}: SELECT * FROM wallets")

    install_safeguards(app, "example", **kwargs)
    return app


@pytest.fixture
def environment(monkeypatch):
    def set_environment(value: str | None, token: str | None = None) -> None:
        for name, setting in (("LEDGERMESH_ENV", value), (OPERATOR_TOKEN_VARIABLE, token)):
            if setting is None:
                monkeypatch.delenv(name, raising=False)
            else:
                monkeypatch.setenv(name, setting)

    set_environment(None)
    return set_environment


def assert_safe_problem(response, status: int) -> dict:
    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert set(body) == PROBLEM_FIELDS
    assert body["status"] == status
    text = response.text
    for leak in ("hunter2", "SELECT", "Traceback", "RuntimeError", "postgresql"):
        assert leak not in text
    return body


# ---- restricted surfaces ------------------------------------------------------


@pytest.mark.parametrize("value", [None, "", "staging", "production", "LOCAL", "dev"])
def test_internal_routes_and_docs_are_closed_outside_local_and_test(environment, value):
    environment(value)
    client = TestClient(make_app())
    unknown = client.get("/no-such-route")

    for path in ("/internal/v1/things", "/internal", "/docs", "/redoc", "/openapi.json"):
        response = client.get(path)
        assert_safe_problem(response, 404)
        # A closed route is indistinguishable from one that does not exist:
        # same bytes, same headers in the same order.
        assert response.content == unknown.content
        assert response.headers.raw == unknown.headers.raw


@pytest.mark.parametrize("value", ["local", "test"])
def test_internal_routes_and_docs_are_open_in_local_and_test(environment, value):
    environment(value)
    client = TestClient(make_app())

    assert client.get("/internal/v1/things").json() == ["thing"]
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").status_code == 200


def test_operator_token_opens_internal_routes_in_a_deployment(environment):
    environment("staging", token=TOKEN)
    client = TestClient(make_app())

    assert client.get("/internal/v1/things").status_code == 404
    wrong = client.get("/internal/v1/things", headers={OPERATOR_TOKEN_HEADER: TOKEN + "x"})
    assert wrong.status_code == 404
    right = client.get("/internal/v1/things", headers={OPERATOR_TOKEN_HEADER: TOKEN})
    assert right.json() == ["thing"]


def test_operator_token_shorter_than_the_minimum_never_matches(environment):
    environment(None, token="short")

    assert not operator_token_matches("short")
    assert not operator_token_matches(None)


def test_public_routes_are_not_restricted(environment):
    environment("production")
    client = TestClient(make_app())

    response = client.post("/v1/items", json={"name": "a", "quantity": 1})

    assert response.json() == {"name": "a", "quantity": 1}


# ---- safe error bodies ---------------------------------------------------------


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "", "quantity": 1},
        {"name": "a", "quantity": 0},
        {"name": "a", "quantity": -5},
        {"name": "a", "quantity": 11},
        {"name": "much-too-long-name", "quantity": 1},
        {"name": "a", "quantity": 1, "extra": "field"},
        {"name": "a"},
    ],
)
def test_validation_errors_are_problems_without_the_submitted_input(environment, payload):
    client = TestClient(make_app())

    response = client.post("/v1/items", json=payload)

    body = assert_safe_problem(response, 422)
    assert body["type"] == "urn:ledgermesh:example:invalid-request"
    assert body["detail"].startswith("body")
    assert "much-too-long-name" not in response.text
    assert '"field"' not in response.text


def test_malformed_json_is_a_problem(environment):
    client = TestClient(make_app())

    response = client.post(
        "/v1/items", content=b'{"name": "a",', headers={"content-type": "application/json"}
    )

    assert_safe_problem(response, 422)


def test_unknown_route_and_wrong_method_are_problems(environment):
    client = TestClient(make_app())

    assert assert_safe_problem(client.get("/nope"), 404)["title"] == "Not Found"
    wrong_method = client.delete("/v1/items")
    assert assert_safe_problem(wrong_method, 405)["title"] == "Method Not Allowed"
    assert wrong_method.headers["allow"] == "POST"


def test_unhandled_error_returns_a_generic_problem_and_logs_the_cause(environment, caplog):
    client = TestClient(make_app(), raise_server_exceptions=False)

    with caplog.at_level(logging.ERROR, logger="ledgermesh_messaging.safeguards"):
        response = client.get("/v1/boom", headers={"X-Correlation-Id": "req-123"})

    body = assert_safe_problem(response, 500)
    assert body["type"] == "urn:ledgermesh:example:internal-error"
    assert "req-123" in body["detail"]
    assert response.headers["x-correlation-id"] == "req-123"
    record = next(
        r
        for r in caplog.records
        if r.name == "ledgermesh_messaging.safeguards" and r.levelno == logging.ERROR
    )
    assert record.correlation_id == "req-123"
    # The traceback goes to the log, with the password masked by the JSON formatter.
    line = json.loads(JsonFormatter("example").format(record))
    assert "RuntimeError" in line["exception"]
    assert "hunter2" not in json.dumps(line)


def test_unsafe_correlation_id_is_replaced(environment):
    client = TestClient(make_app(), raise_server_exceptions=False)

    response = client.get("/v1/boom", headers={"X-Correlation-Id": "<script>" * 30})

    assert_safe_problem(response, 500)
    assert "<script>" not in response.text
    assert len(response.headers["x-correlation-id"]) == 36


# ---- request size ---------------------------------------------------------------


def test_oversized_body_is_rejected_before_the_handler(environment):
    client = TestClient(make_app())
    large = json.dumps({"name": "a", "quantity": 1, "pad": "x" * 2_000_000})

    response = client.post("/v1/items", content=large, headers={"content-type": "application/json"})

    assert_safe_problem(response, 413)


def test_streamed_body_over_the_limit_is_rejected(environment, monkeypatch):
    monkeypatch.setenv("LEDGERMESH_MAX_REQUEST_BYTES", "64")
    client = TestClient(make_app())

    def chunks():
        for _ in range(10):
            yield b"x" * 16

    response = client.post("/v1/items", content=chunks())

    assert_safe_problem(response, 413)


def test_body_within_the_limit_still_reaches_the_handler(environment, monkeypatch):
    monkeypatch.setenv("LEDGERMESH_MAX_REQUEST_BYTES", "64")
    client = TestClient(make_app())

    assert client.post("/v1/items", json={"name": "a", "quantity": 2}).status_code == 200


@pytest.mark.parametrize("value", ["0", "-1", "lots"])
def test_invalid_body_limit_refuses_to_start(environment, monkeypatch, value):
    monkeypatch.setenv("LEDGERMESH_MAX_REQUEST_BYTES", value)

    with pytest.raises(UnsafeConfigurationError, match="LEDGERMESH_MAX_REQUEST_BYTES"):
        make_app()


# ---- unsafe defaults -------------------------------------------------------------


@pytest.mark.parametrize("value", [None, "staging", "production"])
def test_development_password_is_refused_outside_local_and_test(environment, value):
    environment(value)

    with pytest.raises(UnsafeConfigurationError, match="SVC_DATABASE_URL") as raised:
        require_safe_database_url(DEV_URL, setting="SVC_DATABASE_URL")

    assert DEV_DATABASE_PASSWORD not in str(raised.value)
    assert require_safe_database_url(SECRET_URL, setting="SVC_DATABASE_URL") == SECRET_URL


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg://svc@db:5432/svc",
        "postgresql+psycopg://svc:@db:5432/svc",
        "postgresql+psycopg:///svc?host=db&user=svc",
        "postgresql+psycopg:///svc?host=db&user=svc&password=",
    ],
)
@pytest.mark.parametrize("value", [None, "staging"])
def test_missing_or_empty_password_is_refused_outside_local_and_test(environment, value, url):
    environment(value)

    with pytest.raises(UnsafeConfigurationError, match="has no password"):
        require_safe_database_url(url, setting="SVC_DATABASE_URL")


@pytest.mark.parametrize(
    "url",
    [
        f"postgresql+psycopg://svc:x@db/svc?password={DEV_DATABASE_PASSWORD}",
        f"postgresql+psycopg:///svc?host=db&user=svc&password={DEV_DATABASE_PASSWORD}",
    ],
)
def test_development_password_in_the_query_is_refused(environment, url):
    environment("staging")

    with pytest.raises(UnsafeConfigurationError, match="local development password") as raised:
        require_safe_database_url(url, setting="SVC_DATABASE_URL")

    assert DEV_DATABASE_PASSWORD not in str(raised.value)


def test_real_password_in_the_query_is_accepted(environment):
    environment("staging")
    url = "postgresql+psycopg:///svc?host=db&user=svc&password=hunter2-secret"

    assert require_safe_database_url(url, setting="SVC_DATABASE_URL") == url


def test_unset_environment_error_points_developers_at_local(environment):
    with pytest.raises(UnsafeConfigurationError, match="set LEDGERMESH_ENV=local"):
        require_safe_database_url(DEV_URL, setting="SVC_DATABASE_URL")


def test_deployment_error_does_not_suggest_local(environment):
    environment("staging")

    with pytest.raises(UnsafeConfigurationError) as raised:
        require_safe_database_url(DEV_URL, setting="SVC_DATABASE_URL")

    assert "LEDGERMESH_ENV=local" not in str(raised.value)


@pytest.mark.parametrize("value", ["local", "test"])
def test_development_password_is_accepted_in_local_and_test(environment, value):
    environment(value)

    assert require_safe_database_url(DEV_URL, setting="SVC_DATABASE_URL") == DEV_URL


def test_deployment_refuses_to_start_with_the_development_password(environment):
    environment("staging")

    def database_url() -> str:
        return require_safe_database_url(DEV_URL, setting="SVC_DATABASE_URL")

    with pytest.raises(UnsafeConfigurationError, match="refuses to start"):
        make_app(database_url=database_url)


def test_deployment_refuses_to_start_with_a_short_operator_token(environment):
    environment("staging", token="too-short")

    with pytest.raises(UnsafeConfigurationError, match=OPERATOR_TOKEN_VARIABLE):
        check_startup_configuration("example", database_url=lambda: SECRET_URL)


def test_deployment_with_real_secrets_starts(environment):
    environment("staging", token=TOKEN)

    check_startup_configuration("example", database_url=lambda: SECRET_URL)


def test_unset_environment_starts_so_the_image_can_answer_health(environment):
    """LM-047: an image starts on its own; the password is refused on first use."""

    def database_url() -> str:
        return require_safe_database_url(DEV_URL, setting="SVC_DATABASE_URL")

    app = make_app(database_url=database_url)

    assert TestClient(app).get("/internal/v1/things").status_code == 404
    with pytest.raises(UnsafeConfigurationError):
        database_url()
