"""Hosted smoke: redemptions end to end through the public URL (LM-050).

Each scenario uses its own wallet and reward created for this run by
infra/staging/fixtures.py (``smoke-<run_id>-<name>``), so the starting values
are known exactly and concurrent runs cannot disturb each other. Effects are
checked only through the public read-only routes.
"""

from __future__ import annotations

import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
import pytest

from tests.hosted._support import (
    FIXTURE_REWARDS,
    FIXTURE_WALLETS,
    SAGA_AUDIT_TRAILS,
    TERMINAL_STATUSES,
    Deadline,
    Diagnostics,
    Settings,
    requires_hosted,
    summarize_events,
    wait_for,
)

pytestmark = requires_hosted


class Gateway:
    """The public routes this suite uses, with bounded waits and diagnostics."""

    def __init__(
        self,
        client: httpx.Client,
        settings: Settings,
        deadline: Deadline,
        diagnostics: Diagnostics,
    ) -> None:
        self.client = client
        self.settings = settings
        self.deadline = deadline
        self.diagnostics = diagnostics

    def _budget(self) -> float:
        remaining = self.deadline.remaining()
        if remaining <= 0:
            pytest.fail(
                f"the hosted smoke run used its whole {self.settings.timeout_seconds:.0f}s budget"
            )
        return min(self.settings.wait_seconds, remaining)

    def redeem(
        self,
        scenario: str,
        user: str,
        reward: str,
        *,
        key: str,
        quantity: int = 1,
    ) -> httpx.Response:
        response = self.client.post(
            "/v1/redemptions",
            json={
                "user_id": user,
                "reward_id": reward,
                "quantity": quantity,
                "coordination_mode": "saga",
            },
            headers={"Idempotency-Key": key},
        )
        entry = self.diagnostics.redemption(scenario)
        entry.setdefault("idempotency_key", key)
        entry.setdefault("requests", []).append(
            {"quantity": quantity, "http_status": response.status_code}
        )
        if response.status_code == 202:
            self.diagnostics.observe(scenario, response.json())
        return response

    def redemption(self, scenario: str, transaction_id: str) -> dict[str, Any]:
        response = self.client.get(f"/v1/redemptions/{transaction_id}")
        assert response.status_code == 200, (
            f"GET /v1/redemptions/{transaction_id}: HTTP {response.status_code} "
            f"{response.text[:200]}"
        )
        body = response.json()
        self.diagnostics.observe(scenario, body)
        return body

    def wait_terminal(self, scenario: str, transaction_id: str) -> dict[str, Any]:
        return wait_for(
            lambda: self.redemption(scenario, transaction_id),
            lambda body: body["status"] in TERMINAL_STATUSES,
            what=f"redemption {transaction_id} ({scenario}) to reach a terminal status",
            timeout=self._budget(),
            describe=lambda body: body["status"],
        )

    def wallet_points(self, user: str) -> int:
        response = self.client.get(f"/wallet/v1/wallets/{user}")
        assert response.status_code == 200, f"wallet {user}: HTTP {response.status_code}"
        return response.json()["available_points"]

    def inventory(self, reward: str) -> int:
        response = self.client.get(f"/catalog/v1/rewards/{reward}")
        assert response.status_code == 200, f"reward {reward}: HTTP {response.status_code}"
        return response.json()["available_inventory"]

    def audit_events(self, transaction_id: str) -> list[dict[str, Any]]:
        response = self.client.get(f"/audit/v1/transactions/{transaction_id}/events")
        assert response.status_code == 200, f"audit {transaction_id}: HTTP {response.status_code}"
        return response.json()

    def wait_audit_trail(
        self, scenario: str, transaction_id: str, expected: list[str]
    ) -> list[dict[str, Any]]:
        """Audit events arrive asynchronously (outbox -> Kafka -> audit consumer)."""

        def statuses(events: list[dict[str, Any]]) -> list[str]:
            return [e["envelope"]["payload"].get("to_status") for e in events]

        events = wait_for(
            lambda: self.audit_events(transaction_id),
            lambda events: Counter(statuses(events)) >= Counter(expected),
            what=f"the audit trail {expected} of {transaction_id} ({scenario})",
            timeout=self._budget(),
            describe=lambda events: str(statuses(events)),
        )
        self.diagnostics.redemption(scenario)["audit_events"] = summarize_events(events)
        return events

    def assert_fresh(self, user: str, points: int, reward: str, stock: int) -> None:
        """This run's fixtures are untouched: the starting values are known exactly."""
        assert self.wallet_points(user) == points, f"{user} was not fresh; create new fixtures"
        assert self.inventory(reward) == stock, f"{reward} was not fresh; create new fixtures"

    def record_effects(self, scenario: str, **values: int) -> None:
        self.diagnostics.redemption(scenario).setdefault("effects", {}).update(values)


@pytest.fixture
def gateway(
    client: httpx.Client,
    fixtures_ready: Settings,
    deadline: Deadline,
    diagnostics: Diagnostics,
) -> Gateway:
    return Gateway(client, fixtures_ready, deadline, diagnostics)


def assert_audit_trail(events: list[dict[str, Any]], redemption: dict[str, Any], expected):
    """Each transition exactly once, in one correlation, in ledger order."""
    tx = redemption["transaction_id"]
    envelopes = [e["envelope"] for e in events]
    transitions = [env for env in envelopes if env["event_type"] == "coordinator.transitioned"]
    assert sorted(env["payload"]["to_status"] for env in transitions) == sorted(expected), (
        f"{tx}: audit transitions {[env['payload']['to_status'] for env in transitions]}"
    )
    event_ids = [env["event_id"] for env in envelopes]
    assert len(event_ids) == len(set(event_ids)), f"{tx}: an audit event was recorded twice"
    sequences = [e["ledger_sequence"] for e in events]
    assert sequences == sorted(set(sequences)), f"{tx}: ledger sequence not increasing"
    for env in envelopes:
        assert env["transaction_id"] == tx
        # The coordinator's correlation id survives Kafka and every service hop.
        assert env["correlation_id"] == redemption["correlation_id"], (
            f"{tx}: audit event {env['event_id']} has correlation {env['correlation_id']}, "
            f"expected {redemption['correlation_id']}"
        )


def test_saga_redemption_completes_exactly_once(gateway: Gateway, settings: Settings):
    scenario = "completed"
    user, reward = settings.fixture_id("completed"), settings.fixture_id("completed")
    points, (cost, stock) = FIXTURE_WALLETS["completed"], FIXTURE_REWARDS["completed"]
    key = settings.idempotency_key(scenario)
    gateway.assert_fresh(user, points, reward, stock)

    # Five identical requests at once, as a client retrying over a bad network.
    with ThreadPoolExecutor(max_workers=5) as pool:
        responses = list(
            pool.map(lambda _: gateway.redeem(scenario, user, reward, key=key), range(5))
        )
    assert [r.status_code for r in responses] == [202] * 5, [r.text[:200] for r in responses]
    bodies = [r.json() for r in responses]
    assert len({b["transaction_id"] for b in bodies}) == 1, "one key must mean one transaction"
    assert len({b["correlation_id"] for b in bodies}) == 1
    created = bodies[0]
    assert created["coordination_mode"] == "saga"
    # The result is not in the response: the request is only accepted, and the
    # outcome has to be polled for (202 + REQUESTED from the creating request).
    assert "REQUESTED" in {b["status"] for b in bodies}, [b["status"] for b in bodies]

    final = gateway.wait_terminal(scenario, created["transaction_id"])
    assert final["status"] == "COMPLETED", gateway.diagnostics.redemption(scenario)
    assert final["correlation_id"] == created["correlation_id"]

    # Replaying after completion returns the same transaction and changes nothing.
    replay = gateway.redeem(scenario, user, reward, key=key)
    assert replay.status_code == 202
    assert replay.json()["transaction_id"] == created["transaction_id"]
    assert replay.json()["status"] == "COMPLETED"

    events = gateway.wait_audit_trail(
        scenario, created["transaction_id"], SAGA_AUDIT_TRAILS["COMPLETED"]
    )
    assert_audit_trail(events, created, SAGA_AUDIT_TRAILS["COMPLETED"])

    # Exactly one deduction and one reservation, read after the audit trail
    # settled (so any duplicate effect would have had time to show).
    balance, inventory = gateway.wallet_points(user), gateway.inventory(reward)
    gateway.record_effects(scenario, wallet_points=balance, available_inventory=inventory)
    assert balance == points - cost, f"wallet {user}: {balance}"
    assert inventory == stock - 1, f"reward {reward}: {inventory}"


def test_reusing_a_key_for_a_different_request_is_rejected(gateway: Gateway, settings: Settings):
    scenario = "conflict"
    user, reward = settings.fixture_id("conflict"), settings.fixture_id("conflict")
    points, (cost, stock) = FIXTURE_WALLETS["conflict"], FIXTURE_REWARDS["conflict"]
    key = settings.idempotency_key(scenario)
    gateway.assert_fresh(user, points, reward, stock)

    first = gateway.redeem(scenario, user, reward, key=key)
    assert first.status_code == 202, first.text[:200]
    conflict = gateway.redeem(scenario, user, reward, key=key, quantity=2)
    assert conflict.status_code == 409, conflict.text[:200]
    problem = conflict.json()
    assert problem["type"] == "urn:ledgermesh:coordinator:idempotency-conflict"
    assert problem["transaction_id"] == first.json()["transaction_id"]

    final = gateway.wait_terminal(scenario, first.json()["transaction_id"])
    assert final["status"] == "COMPLETED", gateway.diagnostics.redemption(scenario)
    events = gateway.wait_audit_trail(
        scenario, final["transaction_id"], SAGA_AUDIT_TRAILS["COMPLETED"]
    )
    assert_audit_trail(events, final, SAGA_AUDIT_TRAILS["COMPLETED"])
    balance, inventory = gateway.wallet_points(user), gateway.inventory(reward)
    gateway.record_effects(scenario, wallet_points=balance, available_inventory=inventory)
    # Only the first request had any effect.
    assert balance == points - cost
    assert inventory == stock - 1


def test_insufficient_points_fails_without_effects(gateway: Gateway, settings: Settings):
    scenario = "insufficient"
    user, reward = settings.fixture_id("insufficient"), settings.fixture_id("insufficient")
    points, (_cost, stock) = FIXTURE_WALLETS["insufficient"], FIXTURE_REWARDS["insufficient"]
    gateway.assert_fresh(user, points, reward, stock)

    created = gateway.redeem(scenario, user, reward, key=settings.idempotency_key(scenario))
    assert created.status_code == 202, created.text[:200]
    final = gateway.wait_terminal(scenario, created.json()["transaction_id"])
    assert final["status"] == "FAILED", gateway.diagnostics.redemption(scenario)
    events = gateway.wait_audit_trail(
        scenario, final["transaction_id"], SAGA_AUDIT_TRAILS["FAILED"]
    )
    assert_audit_trail(events, final, SAGA_AUDIT_TRAILS["FAILED"])
    balance, inventory = gateway.wallet_points(user), gateway.inventory(reward)
    gateway.record_effects(scenario, wallet_points=balance, available_inventory=inventory)
    assert balance == points
    assert inventory == stock, "a failed redemption must not reserve inventory"


def test_exhausted_inventory_is_compensated(gateway: Gateway, settings: Settings):
    scenario = "compensated"
    user, reward = settings.fixture_id("compensated"), settings.fixture_id("sold-out")
    points, (_cost, stock) = FIXTURE_WALLETS["compensated"], FIXTURE_REWARDS["sold-out"]
    gateway.assert_fresh(user, points, reward, stock)

    created = gateway.redeem(scenario, user, reward, key=settings.idempotency_key(scenario))
    assert created.status_code == 202, created.text[:200]
    final = gateway.wait_terminal(scenario, created.json()["transaction_id"])
    assert final["status"] == "COMPENSATED", gateway.diagnostics.redemption(scenario)
    events = gateway.wait_audit_trail(
        scenario, final["transaction_id"], SAGA_AUDIT_TRAILS["COMPENSATED"]
    )
    assert_audit_trail(events, final, SAGA_AUDIT_TRAILS["COMPENSATED"])
    balance, inventory = gateway.wallet_points(user), gateway.inventory(reward)
    gateway.record_effects(scenario, wallet_points=balance, available_inventory=inventory)
    # Deducted, then refunded exactly once; nothing was reserved.
    assert balance == points
    assert inventory == stock


def test_unknown_redemption_is_a_clear_404(client: httpx.Client):
    response = client.get(f"/v1/redemptions/{uuid.uuid4()}")
    assert response.status_code == 404
    assert response.json()["type"] == "urn:ledgermesh:coordinator:not-found"


def test_two_phase_commit_redemption():
    pytest.skip(
        "2PC cannot be triggered through the public API: POST /v1/redemptions only records "
        "REQUESTED (services/coordinator/app/repository.py), the Saga starter picks up only "
        "coordination_mode 'saga' (services/coordinator/app/saga.py), and 2PC runs only via "
        "POST /internal/v1/redemptions/{id}/execute-2pc, which is 404 outside LEDGERMESH_ENV "
        "local/test (services/coordinator/app/main.py) and blocked by the gateway. A public "
        "two_phase_commit request would stay REQUESTED forever, so none is sent. 2PC is "
        "covered by make test-2pc-integration (tests/integration)."
    )


def test_running_revision(settings: Settings, client: httpx.Client):
    if settings.expected_revision is None:
        pytest.skip("no LEDGERMESH_HOSTED_EXPECTED_REVISION given")
    body = client.get("/health").json()
    assert set(body) == {"status", "service"}, body
    pytest.skip(
        f"expected revision {settings.expected_revision}: GET /health reports no revision "
        "(services/*/app/main.py), so it cannot be checked through the public URL. The "
        "deploy and smoke workflows check it on the VM with `deploy.sh status --expect`."
    )
