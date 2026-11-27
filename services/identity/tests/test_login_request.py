"""Login requests: generic responses, single-use hashed tokens, rate limits (LM-052)."""

import hashlib
from datetime import timedelta

import pytest

from .fakes import make_harness

ACCEPTED = {
    "status": "accepted",
    "message": (
        "If this address can receive email, a sign-in link is on its way. It expires in 15 minutes."
    ),
}
TOO_MANY = {
    "type": "urn:ledgermesh:identity:rate-limited",
    "title": "Too many requests",
    "status": 429,
    "detail": "Too many requests. Try again later.",
}


def comparable(response):
    headers = {k: v for k, v in response.headers.items() if k not in {"date", "x-request-id"}}
    return response.status_code, response.json(), headers


def test_new_and_existing_users_get_identical_responses():
    harness = make_harness()
    harness.sign_in("known@example.com")

    known = harness.request_login("known@example.com")
    unknown = harness.request_login("unknown@example.com")

    assert known.status_code == 202
    assert known.json() == ACCEPTED
    assert comparable(known) == comparable(unknown)
    # Both get exactly one email; only the recipient differs.
    assert [sent.to for sent in harness.mailer.sent[-2:]] == [
        "known@example.com",
        "unknown@example.com",
    ]


def test_request_creates_user_and_stores_only_the_token_hash():
    harness = make_harness()

    harness.request_login("alice@example.com")

    token = harness.mailer.last_token()
    stored = harness.repository.tokens
    assert len(stored) == 1
    assert stored[0].token_hash == hashlib.sha256(token.encode()).hexdigest()
    assert token not in repr(harness.repository)
    assert len(token) >= 43
    assert set(token) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
    assert stored[0].expires_at - stored[0].created_at == timedelta(seconds=900)


def test_link_points_to_the_confirm_page_with_the_token_in_the_fragment():
    harness = make_harness()

    harness.request_login("alice@example.com")

    sent = harness.mailer.sent[-1]
    assert sent.link.startswith("http://localhost:8005/v1/auth/verify#token=")
    assert "?" not in sent.link
    assert sent.expires_in_minutes == 15


def test_new_request_revokes_the_previous_link():
    harness = make_harness()
    harness.request_login("alice@example.com")
    first = harness.mailer.last_token()
    harness.request_login("alice@example.com")
    second = harness.mailer.last_token()

    active = [t for t in harness.repository.tokens if not t.consumed_at and not t.revoked_at]
    assert len(active) == 1
    assert harness.verify(first).status_code == 400
    assert harness.verify(second).status_code == 200


def test_token_ttl_is_configurable():
    harness = make_harness(login_token_ttl_seconds=300)

    response = harness.request_login("alice@example.com")

    assert "expires in 5 minutes" in response.json()["message"]
    token = harness.repository.tokens[0]
    assert token.expires_at - token.created_at == timedelta(seconds=300)


def test_mail_failure_does_not_change_the_response():
    harness = make_harness()
    harness.mailer.fail = True

    response = harness.request_login("alice@example.com")

    assert response.status_code == 202
    assert response.json() == ACCEPTED


def test_sixth_request_for_one_address_is_rate_limited():
    harness = make_harness()
    for _ in range(5):
        assert harness.request_login("alice@example.com").status_code == 202

    limited = harness.request_login("ALICE@example.com")

    assert limited.status_code == 429
    assert limited.json() == TOO_MANY
    assert 1 <= int(limited.headers["retry-after"]) <= 900
    assert len(harness.mailer.sent) == 5
    event = harness.repository.events[-1]
    assert (event.event_type, event.reason) == ("rate_limited", "login_email")
    assert event.email_masked == "a***@example.com"
    assert event.user_id is None
    # Another address from the same client still works.
    assert harness.request_login("bob@example.com").status_code == 202


def test_repeated_denials_add_one_audit_event_per_window():
    # auth_events is append-only: hammering must not grow it without bound.
    harness = make_harness()
    for _ in range(5):
        harness.request_login("alice@example.com")

    statuses = {harness.request_login("alice@example.com").status_code for _ in range(10)}

    assert statuses == {429}
    assert harness.repository.event_types().count("rate_limited") == 1
    harness.clock.advance(900)
    for _ in range(7):
        harness.request_login("alice@example.com")
    assert harness.repository.event_types().count("rate_limited") == 2


def test_unknown_and_known_addresses_are_limited_identically():
    harness = make_harness()
    harness.sign_in("known@example.com")
    for _ in range(4):
        harness.request_login("known@example.com")
    for _ in range(5):
        harness.request_login("new@example.com")

    known = harness.request_login("known@example.com")
    unknown = harness.request_login("new@example.com")

    assert comparable(known) == comparable(unknown)
    assert known.status_code == 429


def test_twenty_first_request_from_one_ip_is_rate_limited_even_when_invalid():
    harness = make_harness()
    for index in range(20):
        harness.request_login(f"user{index}@example.com" if index % 2 else "not-an-email")

    limited = harness.request_login("fresh@example.com")

    assert limited.status_code == 429
    assert limited.json() == TOO_MANY
    event = harness.repository.events[-1]
    assert (event.event_type, event.reason, event.email_masked) == (
        "rate_limited",
        "login_ip",
        None,
    )


def test_rate_limit_window_rolls_over():
    harness = make_harness()
    for _ in range(5):
        harness.request_login("alice@example.com")
    assert harness.request_login("alice@example.com").status_code == 429

    harness.clock.advance(900)

    assert harness.request_login("alice@example.com").status_code == 202


@pytest.mark.parametrize(
    ("headers", "content"),
    [
        ({"Content-Type": "application/x-www-form-urlencoded"}, "email=alice%40example.com"),
        ({"Content-Type": "text/plain"}, '{"email": "alice@example.com"}'),
    ],
)
def test_non_json_requests_are_rejected(headers, content):
    harness = make_harness()

    response = harness.client.post("/v1/auth/login-requests", headers=headers, content=content)

    assert response.status_code == 415
    assert response.json()["type"] == "urn:ledgermesh:identity:unsupported-media-type"
    assert harness.mailer.sent == []


def test_unknown_request_fields_are_rejected_without_echoing_values():
    harness = make_harness()

    response = harness.client.post(
        "/v1/auth/login-requests", json={"email": "alice@example.com", "secret": "hunter2"}
    )

    assert response.status_code == 422
    assert response.json()["invalid_fields"] == ["secret"]
    assert "hunter2" not in response.text
    assert harness.repository.users == {}


def test_login_responses_are_not_cached():
    harness = make_harness()

    assert harness.request_login("alice@example.com").headers["cache-control"] == "no-store"
