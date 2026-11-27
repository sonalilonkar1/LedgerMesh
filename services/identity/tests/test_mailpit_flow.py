"""End-to-end sign-in through the running service and Mailpit (LM-052).

Opt-in: skipped unless ``IDENTITY_E2E_BASE_URL`` is set. Run it against the
Compose stack with:

    make infra-up
    make identity-e2e

It requests a link for a fresh address, reads the email from Mailpit's API,
opens the confirm page, signs in, replays the link, and logs out.
"""

import os
import re
import time
from uuid import uuid4

import httpx
import pytest

BASE_URL = os.environ.get("IDENTITY_E2E_BASE_URL")
MAILPIT_URL = os.environ.get("MAILPIT_API_URL", "http://localhost:8025")

pytestmark = pytest.mark.skipif(not BASE_URL, reason="set IDENTITY_E2E_BASE_URL to run")

JSON = {"Content-Type": "application/json"}
LINK = re.compile(r"(https?://\S+/v1/auth/verify)#token=([A-Za-z0-9_-]+)")
RATE_LIMITED = (
    "rate limited by the running service: wait for Retry-After, or restart the stack with "
    "IDENTITY_LOGIN_LIMIT_PER_EMAIL, IDENTITY_LOGIN_LIMIT_PER_IP and "
    "IDENTITY_VERIFY_LIMIT_PER_IP raised (docs/authentication.md)"
)


def accepted(response: httpx.Response) -> bool:
    assert response.status_code != 429, RATE_LIMITED
    return response.status_code == 202


def wait_for_email(mailpit: httpx.Client, address: str) -> str:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        found = mailpit.get("/api/v1/search", params={"query": f"to:{address}"}).json()
        if found.get("messages"):
            message_id = found["messages"][0]["ID"]
            return mailpit.get(f"/api/v1/message/{message_id}").json()["Text"]
        time.sleep(0.5)
    raise AssertionError(f"no email for {address} in Mailpit")


def session_cookie(response: httpx.Response) -> str:
    header = response.headers["set-cookie"]
    assert "HttpOnly" in header and "SameSite=lax" in header and "Path=/" in header
    return header.split(";", 1)[0]


def test_sign_in_through_mailpit():
    address = f"e2e-{uuid4().hex[:12]}@example.com"
    with (
        httpx.Client(base_url=BASE_URL, timeout=10) as api,
        httpx.Client(base_url=MAILPIT_URL, timeout=10) as mailpit,
    ):
        requested = api.post(
            "/v1/auth/login-requests", json={"email": address.upper()}, headers=JSON
        )
        assert accepted(requested)

        text = wait_for_email(mailpit, address)
        match = LINK.search(text)
        assert match, text
        page_url, token = match.groups()
        assert page_url.startswith(BASE_URL)

        # Opening the link (or a scanner prefetching it) consumes nothing.
        page = api.get("/v1/auth/verify")
        assert page.status_code == 200
        assert "Sign in to LedgerMesh" in page.text

        verified = api.post("/v1/auth/verify", json={"token": token}, headers=JSON)
        assert verified.status_code == 200, verified.text
        assert verified.json()["user"]["email"] == address
        cookie = session_cookie(verified)

        me = api.get("/v1/me", headers={"Cookie": cookie})
        assert me.status_code == 200
        assert me.json()["email"] == address
        assert me.json()["email_verified_at"] is not None
        assert api.get("/v1/sessions/introspect", headers={"Cookie": cookie}).status_code == 200
        assert api.get("/v1/operator/auth-events", headers={"Cookie": cookie}).status_code == 403

        replay = api.post("/v1/auth/verify", json={"token": token}, headers=JSON)
        assert replay.status_code == 400

        assert api.post("/v1/auth/logout", headers=JSON | {"Cookie": cookie}).status_code == 204
        assert api.get("/v1/me", headers={"Cookie": cookie}).status_code == 401


def test_operator_from_the_allowlist_can_read_auth_events():
    operator = os.environ.get("IDENTITY_E2E_OPERATOR_EMAIL", "operator@example.com")
    with (
        httpx.Client(base_url=BASE_URL, timeout=10) as api,
        httpx.Client(base_url=MAILPIT_URL, timeout=10) as mailpit,
    ):
        before = mailpit.get("/api/v1/search", params={"query": f"to:{operator}"}).json()
        seen = {m["ID"] for m in before.get("messages", [])}
        assert accepted(api.post("/v1/auth/login-requests", json={"email": operator}, headers=JSON))
        deadline = time.monotonic() + 20
        text = None
        while text is None and time.monotonic() < deadline:
            found = mailpit.get("/api/v1/search", params={"query": f"to:{operator}"}).json()
            fresh = [m for m in found.get("messages", []) if m["ID"] not in seen]
            if fresh:
                text = mailpit.get(f"/api/v1/message/{fresh[0]['ID']}").json()["Text"]
            else:
                time.sleep(0.5)
        assert text, "no operator email in Mailpit"
        token = LINK.search(text).group(2)

        cookie = session_cookie(api.post("/v1/auth/verify", json={"token": token}, headers=JSON))
        events = api.get("/v1/operator/auth-events", headers={"Cookie": cookie})

        assert events.status_code == 200
        assert events.json()[0]["event_type"] == "login_succeeded"
        assert all(e["email_masked"] is None or "***" in e["email_masked"] for e in events.json())
        api.post("/v1/auth/logout", headers=JSON | {"Cookie": cookie})
