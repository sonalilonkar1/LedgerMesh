"""Guest, user and operator access (LM-052)."""

from dataclasses import replace

from .fakes import make_harness


def test_guest_is_unauthenticated_everywhere_that_needs_a_session():
    harness = make_harness()

    for path in ("/v1/me", "/v1/sessions/introspect", "/v1/operator/auth-events"):
        response = harness.client.get(path)
        assert response.status_code == 401, path
        assert response.json()["type"] == "urn:ledgermesh:identity:unauthenticated"


def test_unknown_cookie_is_a_guest():
    harness = make_harness()
    harness.client.cookies.set("lm_session", "z" * 43)

    assert harness.client.get("/v1/me").status_code == 401


def test_health_and_login_need_no_session():
    harness = make_harness()

    assert harness.client.get("/health").status_code == 200
    assert harness.request_login("guest@example.com").status_code == 202
    assert harness.client.get("/v1/auth/verify").status_code == 200


def test_user_can_read_their_profile_but_not_operator_routes():
    harness = make_harness()
    harness.sign_in("alice@example.com")

    me = harness.client.get("/v1/me")
    events = harness.client.get("/v1/operator/auth-events")

    assert me.status_code == 200
    assert me.json()["role"] == "user"
    assert events.status_code == 403
    assert events.json()["type"] == "urn:ledgermesh:identity:forbidden"


def test_allowlisted_operator_can_read_auth_events():
    harness = make_harness()
    harness.sign_in("someone@example.com")
    harness.sign_in("Operator@Example.com")

    response = harness.client.get("/v1/operator/auth-events", params={"limit": 3})

    assert harness.client.get("/v1/me").json()["role"] == "operator"
    assert response.status_code == 200
    events = response.json()
    assert [e["event_type"] for e in events] == [
        "login_succeeded",
        "login_requested",
        "login_succeeded",
    ]
    assert set(events[0]) == {
        "id",
        "occurred_at",
        "event_type",
        "reason",
        "user_id",
        "session_id",
        "email_masked",
    }
    older = harness.client.get(
        "/v1/operator/auth-events", params={"before_id": events[-1]["id"]}
    ).json()
    assert [e["event_type"] for e in older] == ["login_requested"]


def test_operator_list_limit_is_bounded():
    harness = make_harness()
    harness.sign_in("operator@example.com")

    assert harness.client.get("/v1/operator/auth-events", params={"limit": 0}).status_code == 422
    assert harness.client.get("/v1/operator/auth-events", params={"limit": 201}).status_code == 422


def test_removing_an_operator_from_the_allowlist_demotes_at_once():
    harness = make_harness()
    harness.sign_in("operator@example.com")
    assert harness.client.get("/v1/operator/auth-events").status_code == 200

    harness.service.settings = replace(harness.service.settings, operator_emails=frozenset())

    assert harness.client.get("/v1/operator/auth-events").status_code == 403
    assert harness.client.get("/v1/me").json()["role"] == "user"


def test_introspection_returns_the_principal_for_a_session():
    harness = make_harness()
    user_id = harness.sign_in("alice@example.com").json()["user"]["id"]

    response = harness.client.get("/v1/sessions/introspect")

    assert response.status_code == 200
    body = response.json()
    assert body["active"] is True
    assert body["user_id"] == user_id
    assert body["role"] == "user"
    assert "email" not in body


def test_introspection_after_logout_is_unauthenticated():
    harness = make_harness()
    secret = harness.sign_in().cookies["lm_session"]
    harness.logout()
    harness.client.cookies.set("lm_session", secret)

    assert harness.client.get("/v1/sessions/introspect").status_code == 401
