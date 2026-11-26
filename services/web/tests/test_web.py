"""The LM-011 web interface and its backend-for-frontend.

The Wallet, Catalog and Coordinator are replaced by ``httpx.MockTransport``
handlers, so every success and failure path is tested without the stack.
"""

from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from services.web.app.main import Upstream, Upstreams, app, get_upstreams

TRANSACTION = str(uuid4())
CORRELATION = str(uuid4())
WALLET = {
    "user_id": "user-alice",
    "available_points": 10000,
    "version": 3,
    "updated_at": "2026-11-25T10:00:00Z",
}
REWARDS = [
    {
        "reward_id": "reward-coffee",
        "name": "Coffee voucher",
        "point_cost": 100,
        "total_inventory": 100,
        "available_inventory": 97,
        "version": 3,
        "updated_at": "2026-11-25T10:00:00Z",
    }
]
REDEMPTION = {
    "transaction_id": TRANSACTION,
    "correlation_id": CORRELATION,
    "status": "REQUESTED",
    "coordination_mode": "saga",
    "created_at": "2026-11-25T10:00:00Z",
    "updated_at": "2026-11-25T10:00:00Z",
}
TIMELINE = [
    {
        "sequence": 1,
        "from_status": None,
        "to_status": "REQUESTED",
        "workflow_step": 0,
        "reason": "redemption created",
        "created_at": "2026-11-25T10:00:00Z",
    },
    {
        "sequence": 2,
        "from_status": "REQUESTED",
        "to_status": "DEDUCTING_POINTS",
        "workflow_step": 1,
        "reason": "wallet deduction requested",
        "created_at": "2026-11-25T10:00:01Z",
    },
]


class FakeServices:
    """Records requests and answers from per-service handlers."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.handlers = {
            "wallet": lambda request: httpx.Response(200, json=WALLET),
            "catalog": lambda request: httpx.Response(200, json=REWARDS),
            "coordinator": self._coordinator,
        }

    def _coordinator(self, request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json=REDEMPTION)
        if request.url.path.endswith("/transitions"):
            return httpx.Response(200, json=TIMELINE)
        return httpx.Response(200, json=REDEMPTION)

    def upstream(self, name: str, label: str) -> Upstream:
        def handle(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            return self.handlers[name](request)

        client = httpx.Client(base_url=f"http://{name}", transport=httpx.MockTransport(handle))
        return Upstream(label, client)

    def upstreams(self) -> Upstreams:
        return Upstreams(
            wallet=self.upstream("wallet", "Wallet"),
            catalog=self.upstream("catalog", "Catalog"),
            coordinator=self.upstream("coordinator", "Coordinator"),
        )


@pytest.fixture
def services():
    fake = FakeServices()
    app.dependency_overrides[get_upstreams] = fake.upstreams
    yield fake
    app.dependency_overrides.clear()


@pytest.fixture
def client(services):
    # Unexpected errors must become a response, not an exception in the test.
    return TestClient(app, raise_server_exceptions=False)


# --- User can view balance and available rewards ------------------------------------


def test_the_page_and_its_assets_are_served(client):
    page = client.get("/")
    script = client.get("/static/app.js")

    assert page.status_code == 200
    assert 'id="rewards"' in page.text
    assert 'value="two_phase_commit"' in page.text
    assert 'value="saga"' in page.text
    assert script.status_code == 200
    assert client.get("/health").json() == {"status": "ok", "service": "web"}


def test_balance_comes_from_the_wallet(client, services):
    response = client.get("/api/users/user-alice/wallet")

    assert response.status_code == 200
    assert response.json()["available_points"] == 10000
    assert services.requests[-1].url.path == "/v1/wallets/user-alice"


def test_rewards_come_from_the_catalog(client, services):
    response = client.get("/api/rewards")

    assert response.json() == REWARDS
    assert services.requests[-1].url.path == "/v1/rewards"


@pytest.mark.parametrize("user_id", ["..", "user%2F..", "user%20alice", "a" * 129])
def test_a_user_id_that_is_not_a_plain_segment_never_reaches_the_wallet(client, services, user_id):
    response = client.get(f"/api/users/{user_id}/wallet")

    # Rejected by routing (404) or by the id pattern (422); either way the
    # Wallet is never called with a path built from it.
    assert response.status_code in {404, 422}
    assert services.requests == []


# --- User can submit a redemption in either mode -----------------------------------------


@pytest.mark.parametrize("mode", ["two_phase_commit", "saga"])
def test_a_redemption_is_submitted_in_either_mode(client, services, mode):
    response = client.post(
        "/api/redemptions",
        json={
            "user_id": "user-alice",
            "reward_id": "reward-coffee",
            "quantity": 2,
            "coordination_mode": mode,
        },
        headers={"Idempotency-Key": "click-123"},
    )

    assert response.status_code == 202
    sent = services.requests[-1]
    assert (sent.method, sent.url.path) == ("POST", "/v1/redemptions")
    assert sent.headers["Idempotency-Key"] == "click-123"
    assert httpx.Response(200, content=sent.content).json() == {
        "user_id": "user-alice",
        "reward_id": "reward-coffee",
        "quantity": 2,
        "coordination_mode": mode,
    }


def test_a_missing_idempotency_key_gets_a_fresh_one(client, services):
    body = {
        "user_id": "user-alice",
        "reward_id": "reward-coffee",
        "quantity": 1,
        "coordination_mode": "saga",
    }

    client.post("/api/redemptions", json=body)
    client.post("/api/redemptions", json=body)

    keys = [request.headers["Idempotency-Key"] for request in services.requests]
    assert len(set(keys)) == 2


@pytest.mark.parametrize(
    "body",
    [
        {
            "user_id": "user-alice",
            "reward_id": "reward-coffee",
            "quantity": 1,
            "coordination_mode": "magic",
        },
        {
            "user_id": "user-alice",
            "reward_id": "reward-coffee",
            "quantity": 0,
            "coordination_mode": "saga",
        },
        {"user_id": "", "reward_id": "reward-coffee", "quantity": 1, "coordination_mode": "saga"},
    ],
)
def test_an_invalid_redemption_is_explained_and_never_sent(client, services, body):
    response = client.post("/api/redemptions", json=body)

    assert response.status_code == 422
    assert response.json()["title"] == "Invalid request"
    assert services.requests == []


# --- Transaction status and event timeline are visible ---------------------------------------


def test_status_and_timeline_are_returned_together(client, services):
    response = client.get(f"/api/redemptions/{TRANSACTION}")

    assert response.json() == {"redemption": REDEMPTION, "timeline": TIMELINE}
    assert [request.url.path for request in services.requests] == [
        f"/v1/redemptions/{TRANSACTION}",
        f"/internal/v1/redemptions/{TRANSACTION}/transitions",
    ]


def test_a_non_uuid_transaction_id_is_rejected_before_any_call(client, services):
    response = client.get("/api/redemptions/not-a-uuid")

    assert response.status_code == 422
    assert services.requests == []


# --- Errors are shown without exposing internal stack traces -----------------------------------


def test_a_service_problem_keeps_only_its_title_and_detail(client, services):
    services.handlers["coordinator"] = lambda request: httpx.Response(
        409,
        json={
            "type": "urn:ledgermesh:coordinator:idempotency-conflict",
            "title": "Idempotency conflict",
            "status": 409,
            "detail": "The idempotency key is already associated with a different request.",
            "transaction_id": TRANSACTION,
        },
    )

    response = client.post(
        "/api/redemptions",
        json={
            "user_id": "user-alice",
            "reward_id": "reward-coffee",
            "quantity": 1,
            "coordination_mode": "saga",
        },
    )

    assert response.status_code == 409
    assert response.json() == {
        "title": "Idempotency conflict",
        "detail": "The idempotency key is already associated with a different request.",
    }


def test_a_problem_without_title_or_detail_gets_a_safe_message(client, services):
    services.handlers["wallet"] = lambda request: httpx.Response(404, text="<html>nginx</html>")

    response = client.get("/api/users/user-zed/wallet")

    assert response.status_code == 404
    assert response.json() == {
        "title": "Request was rejected",
        "detail": "Wallet could not complete the request.",
    }


def test_a_server_error_hides_the_upstream_body(client, services):
    services.handlers["catalog"] = lambda request: httpx.Response(
        500, text='Traceback (most recent call last):\n  File "repository.py"\nKeyError: secret'
    )

    response = client.get("/api/rewards")

    assert response.status_code == 502
    assert response.json() == {
        "title": "Catalog is unavailable",
        "detail": "Please try again in a moment.",
    }
    assert "Traceback" not in response.text


def test_an_unreachable_service_is_reported_as_unavailable(client, services):
    def refuse(request):
        raise httpx.ConnectError("connection refused", request=request)

    services.handlers["coordinator"] = refuse

    response = client.get(f"/api/redemptions/{TRANSACTION}")

    assert response.status_code == 503
    assert response.json()["title"] == "Coordinator is unavailable"


def test_an_unexpected_error_never_reaches_the_browser_as_a_trace(client, services):
    def crash(request):
        raise RuntimeError("password=hunter2 at repository.py line 42")

    services.handlers["wallet"] = crash

    response = client.get("/api/users/user-alice/wallet")

    assert response.status_code == 500
    assert response.json() == {
        "title": "Something went wrong",
        "detail": "Please try again in a moment.",
    }
    assert "hunter2" not in response.text
    assert "Traceback" not in response.text
