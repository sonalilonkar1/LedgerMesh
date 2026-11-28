"""Link verification, session lifetime and logout (LM-052)."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from services.identity.app.errors import InvalidOrExpiredLink

from .fakes import make_harness

INVALID_LINK = {
    "type": "urn:ledgermesh:identity:invalid-or-expired-link",
    "title": "Invalid or expired sign-in link",
    "status": 400,
    "detail": "This sign-in link is invalid or has expired. Request a new one.",
}


def failure_reasons(harness):
    return [e.reason for e in harness.repository.events if e.event_type == "login_failed"]


def test_valid_link_signs_in_and_marks_the_address_verified():
    harness = make_harness()
    harness.request_login("alice@example.com")

    response = harness.verify(harness.mailer.last_token())

    assert response.status_code == 200
    body = response.json()
    assert body["user"]["email"] == "alice@example.com"
    assert body["user"]["role"] == "user"
    assert body["session"]["expires_at"] == "2026-11-28T00:00:00Z"
    assert body["session"]["idle_expires_at"] == "2026-11-27T12:30:00Z"
    assert harness.repository.users["alice@example.com"].email_verified_at is not None
    me = harness.client.get("/v1/me")
    assert me.status_code == 200
    assert me.json()["email"] == "alice@example.com"
    assert harness.repository.event_types() == ["login_requested", "login_succeeded"]


def test_session_cookie_stores_only_a_hash_server_side():
    harness = make_harness()

    response = harness.sign_in()

    secret = response.cookies["lm_session"]
    assert len(secret) >= 43
    assert secret not in repr(harness.repository)


def test_reused_link_is_rejected():
    harness = make_harness()
    harness.request_login("alice@example.com")
    token = harness.mailer.last_token()
    assert harness.verify(token).status_code == 200

    replay = harness.verify(token)

    assert replay.status_code == 400
    assert replay.json() == INVALID_LINK
    assert failure_reasons(harness) == ["used"]
    assert len(harness.repository.sessions) == 1


def test_expired_link_is_rejected():
    harness = make_harness()
    harness.request_login("alice@example.com")
    harness.clock.advance(901)

    response = harness.verify(harness.mailer.last_token())

    assert response.status_code == 400
    assert response.json() == INVALID_LINK
    assert failure_reasons(harness) == ["expired"]


def test_link_still_works_just_before_expiry():
    harness = make_harness()
    harness.request_login("alice@example.com")
    harness.clock.advance(899)

    assert harness.verify(harness.mailer.last_token()).status_code == 200


def test_unknown_expired_used_and_revoked_links_look_identical():
    harness = make_harness()
    harness.request_login("a@example.com")
    used = harness.mailer.last_token()
    harness.verify(used)
    harness.request_login("b@example.com")
    revoked = harness.mailer.last_token()
    harness.request_login("b@example.com")
    harness.request_login("c@example.com")
    expired = harness.mailer.last_token()
    harness.clock.advance(1000)

    responses = [harness.verify(t) for t in (used, revoked, expired, "x" * 43)]

    assert {r.status_code for r in responses} == {400}
    assert all(r.json() == INVALID_LINK for r in responses)
    assert failure_reasons(harness) == ["used", "revoked", "expired", "unknown"]


@pytest.mark.parametrize("token", ["short", "x" * 129, "has spaces and more than 32 chars!!", ""])
def test_malformed_tokens_are_rejected_without_echo(token):
    harness = make_harness()

    response = harness.verify(token)

    assert response.status_code == 422
    assert response.json()["invalid_fields"] == ["token"]
    if token:
        assert token not in response.text


def test_verify_requires_json():
    harness = make_harness()
    harness.request_login("alice@example.com")
    token = harness.mailer.last_token()

    response = harness.client.post("/v1/auth/verify", data={"token": token})

    assert response.status_code == 415
    assert harness.repository.tokens[0].consumed_at is None


def test_concurrent_verifications_have_exactly_one_winner():
    harness = make_harness()
    harness.request_login("alice@example.com")
    token = harness.mailer.last_token()
    barrier = Barrier(10)

    def attempt(_):
        barrier.wait()
        try:
            harness.service.verify(token, "198.51.100.1", None)
        except InvalidOrExpiredLink:
            return "rejected"
        return "signed-in"

    with ThreadPoolExecutor(max_workers=10) as pool:
        outcomes = list(pool.map(attempt, range(10)))

    assert outcomes.count("signed-in") == 1
    assert len(harness.repository.sessions) == 1


def test_thirty_first_verification_from_one_ip_is_rate_limited():
    harness = make_harness()
    for _ in range(30):
        assert harness.verify("y" * 43).status_code == 400

    response = harness.verify("y" * 43)

    assert response.status_code == 429
    assert response.json()["detail"] == "Too many requests. Try again later."
    assert "retry-after" in response.headers


def test_sign_in_rotates_an_existing_session():
    harness = make_harness()
    old = harness.sign_in().cookies["lm_session"]

    new = harness.sign_in().cookies["lm_session"]

    assert new != old
    revoked = [s for s in harness.repository.sessions if s.revoked_at is not None]
    assert len(revoked) == 1
    harness.client.cookies.set("lm_session", old)
    assert harness.client.get("/v1/me").status_code == 401


def test_session_expires_after_idle_timeout():
    harness = make_harness()
    harness.sign_in()

    harness.clock.advance(1800)

    assert harness.client.get("/v1/me").status_code == 401


def test_activity_extends_idle_expiry_but_not_past_absolute_expiry():
    harness = make_harness()
    harness.sign_in()

    # Active every 20 minutes for almost 12 hours: the session stays alive.
    for _ in range(35):
        harness.clock.advance(1200)
        response = harness.client.get("/v1/me")
        assert response.status_code == 200
    assert response.json()["session"]["idle_expires_at"] <= response.json()["session"]["expires_at"]

    harness.clock.advance(1200)  # 12 h after sign-in

    assert harness.client.get("/v1/me").status_code == 401


def test_logout_invalidates_the_session_server_side():
    harness = make_harness()
    secret = harness.sign_in().cookies["lm_session"]

    response = harness.logout()

    assert response.status_code == 204
    assert 'lm_session=""' in response.headers["set-cookie"]
    assert "Max-Age=0" in response.headers["set-cookie"]
    assert harness.repository.sessions[0].revoked_at is not None
    # Replaying the old cookie does not work: the server forgot the session.
    harness.client.cookies.set("lm_session", secret)
    assert harness.client.get("/v1/me").status_code == 401
    assert harness.repository.event_types()[-1] == "logout"


def test_logout_without_a_session_is_idempotent():
    harness = make_harness()

    assert harness.logout().status_code == 204
    assert harness.logout().status_code == 204
    assert "logout" not in harness.repository.event_types()


def test_logout_requires_json():
    harness = make_harness()
    harness.sign_in()

    response = harness.client.post("/v1/auth/logout")

    assert response.status_code == 415
    assert harness.repository.sessions[0].revoked_at is None


def test_confirm_page_does_not_consume_the_link():
    harness = make_harness()
    harness.request_login("alice@example.com")

    page = harness.client.get("/v1/auth/verify")

    assert page.status_code == 200
    assert page.headers["content-type"].startswith("text/html")
    assert page.headers["referrer-policy"] == "no-referrer"
    assert page.headers["cache-control"] == "no-store"
    assert "default-src 'none'" in page.headers["content-security-policy"]
    assert "Sign in to LedgerMesh" in page.text
    assert harness.repository.tokens[0].consumed_at is None
    assert harness.verify(harness.mailer.last_token()).status_code == 200
