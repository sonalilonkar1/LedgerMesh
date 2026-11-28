"""Logs and audit rows never carry tokens, session secrets or email addresses (LM-052)."""

import json
import logging
from dataclasses import astuple

import pytest
from ledgermesh_messaging.logs import JsonFormatter
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from services.identity.app.repository import make_engine

from .fakes import make_harness


class Capture(logging.Handler):
    # Same pattern as tests/integration/test_log_correlation.py.
    def __init__(self):
        super().__init__()
        self.setFormatter(JsonFormatter("identity"))
        self.lines: list[str] = []

    def emit(self, record):
        self.lines.append(self.format(record))


@pytest.fixture
def captured():
    capture = Capture()
    root = logging.getLogger()
    previous = root.level
    root.addHandler(capture)
    root.setLevel(logging.DEBUG)
    yield capture.lines
    root.removeHandler(capture)
    root.setLevel(previous)


def full_flow(harness):
    """Sign in, replay, fail delivery, hit a rate limit and log out."""
    harness.request_login("Secret.Person@Example.com")
    token = harness.mailer.last_token()
    signed_in = harness.verify(token)
    harness.verify(token)
    harness.client.get("/v1/me")
    harness.logout()
    harness.mailer.fail = True
    for _ in range(6):
        harness.request_login("secret.person@example.com")
    return token, signed_in.cookies["lm_session"]


def test_logs_contain_no_token_session_secret_or_email(captured):
    harness = make_harness()

    token, session_secret = full_flow(harness)

    output = "\n".join(captured)
    messages = [json.loads(line)["message"] for line in captured]
    assert "login link issued" in messages
    assert "login succeeded" in messages
    assert "login failed" in messages
    assert "login email delivery failed" in messages
    assert "rate limited" in messages
    assert "logout" in messages
    for secret in (token, session_secret, "secret.person", "Secret.Person", "@example.com"):
        assert secret.lower() not in output.lower(), secret


def test_audit_events_contain_no_token_session_secret_or_full_email(captured):
    harness = make_harness()

    token, session_secret = full_flow(harness)

    stored = repr([astuple(event) for event in harness.repository.events])
    assert token not in stored
    assert session_secret not in stored
    assert "secret.person@example.com" not in stored
    assert {e.event_type for e in harness.repository.events} == {
        "login_requested",
        "login_succeeded",
        "login_failed",
        "logout",
        "rate_limited",
    }
    masked = {e.email_masked for e in harness.repository.events if e.email_masked}
    assert masked == {"s***@example.com"}


def test_database_errors_do_not_carry_the_bound_email(captured):
    # A failing statement's exception text normally lists its bound values;
    # uvicorn logs that text with the traceback ("Exception in ASGI application").
    engine = make_engine("sqlite://")
    statement = text("INSERT INTO missing_users (email) VALUES (:email)")
    # Built at runtime so the traceback's source lines do not contain it.
    address = "@".join(("secret.person", "example.com"))

    with pytest.raises(DBAPIError) as failure, engine.begin() as connection:
        connection.execute(statement, {"email": address})
    logging.getLogger("uvicorn.error").error(
        "Exception in ASGI application", exc_info=failure.value
    )

    assert "SQL parameters hidden" in str(failure.value)
    output = "\n".join(captured)
    assert "Traceback" in output
    assert address not in output
    assert "secret.person" not in output
    engine.dispose()
