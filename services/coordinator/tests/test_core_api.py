from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from services.coordinator.app.errors import (
    IdempotencyConflict,
    InvalidTransition,
    RedemptionNotFound,
)
from services.coordinator.app.main import app, get_coordinator_service
from services.coordinator.app.models import CreateRedemptionRequest
from services.coordinator.app.repository import (
    TRANSITIONS,
    RedemptionRecord,
    TransitionRecord,
    request_hash,
)
from services.coordinator.app.service import CoordinatorService


class FakeCoordinatorRepository:
    def __init__(self) -> None:
        self.redemptions: dict[UUID, RedemptionRecord] = {}
        self.keys: dict[str, tuple[str, UUID]] = {}
        self.transitions: dict[UUID, list[TransitionRecord]] = {}

    def create(self, idempotency_key: str, request: CreateRedemptionRequest) -> RedemptionRecord:
        digest = request_hash(request)
        existing = self.keys.get(idempotency_key)
        if existing is not None:
            existing_hash, transaction_id = existing
            if existing_hash != digest:
                raise IdempotencyConflict("The key belongs to another request.", transaction_id)
            return replace(self.redemptions[transaction_id], duplicate=True)

        transaction_id = uuid4()
        now = datetime.now(UTC)
        record = RedemptionRecord(
            transaction_id,
            uuid4(),
            "REQUESTED",
            request.coordination_mode,
            now,
            now,
        )
        self.redemptions[transaction_id] = record
        self.keys[idempotency_key] = (digest, transaction_id)
        self.transitions[transaction_id] = [
            TransitionRecord(1, None, "REQUESTED", 0, "redemption created", now)
        ]
        return record

    def get(self, transaction_id: UUID) -> RedemptionRecord:
        try:
            return self.redemptions[transaction_id]
        except KeyError as error:
            raise RedemptionNotFound("Redemption not found.", transaction_id) from error

    def transition(
        self, transaction_id: UUID, to_status: str, reason: str | None = None
    ) -> RedemptionRecord:
        current = self.get(transaction_id)
        edge = f"{current.status}->{to_status}"
        if edge not in TRANSITIONS[current.coordination_mode]:
            raise InvalidTransition(f"Invalid transition {edge}.", transaction_id)
        now = datetime.now(UTC)
        step = len(self.transitions[transaction_id])
        updated = replace(current, status=to_status, updated_at=now)
        self.redemptions[transaction_id] = updated
        self.transitions[transaction_id].append(
            TransitionRecord(step + 1, current.status, to_status, step, reason, now)
        )
        return updated

    def list_transitions(self, transaction_id: UUID) -> list[TransitionRecord]:
        self.get(transaction_id)
        return self.transitions[transaction_id]


def client_and_repository() -> tuple[TestClient, FakeCoordinatorRepository]:
    repository = FakeCoordinatorRepository()
    app.dependency_overrides[get_coordinator_service] = lambda: CoordinatorService(repository)
    return TestClient(app), repository


def request_payload(mode: str = "saga") -> dict:
    return {
        "user_id": "user-alice",
        "reward_id": "reward-coffee",
        "quantity": 1,
        "coordination_mode": mode,
    }


def test_create_redemption_returns_requested_state():
    client, _repository = client_and_repository()

    response = client.post(
        "/v1/redemptions",
        headers={"Idempotency-Key": "request-1"},
        json=request_payload(),
    )

    assert response.status_code == 202
    assert response.json()["status"] == "REQUESTED"
    assert response.json()["coordination_mode"] == "saga"
    assert UUID(response.json()["transaction_id"])
    assert UUID(response.json()["correlation_id"])


def test_identical_idempotent_retry_returns_same_transaction():
    client, repository = client_and_repository()
    headers = {"Idempotency-Key": "same-key"}

    first = client.post("/v1/redemptions", headers=headers, json=request_payload())
    second = client.post("/v1/redemptions", headers=headers, json=request_payload())

    assert second.status_code == 202
    assert second.json() == first.json()
    assert len(repository.redemptions) == 1
    assert len(next(iter(repository.transitions.values()))) == 1


def test_idempotency_key_reuse_with_different_body_returns_conflict():
    client, _repository = client_and_repository()
    headers = {"Idempotency-Key": "same-key"}
    client.post("/v1/redemptions", headers=headers, json=request_payload())

    response = client.post(
        "/v1/redemptions",
        headers=headers,
        json=request_payload() | {"quantity": 2},
    )

    assert response.status_code == 409
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["type"] == "urn:ledgermesh:coordinator:idempotency-conflict"


def test_missing_or_invalid_creation_values_are_rejected():
    client, repository = client_and_repository()

    missing_header = client.post("/v1/redemptions", json=request_payload())
    invalid_quantity = client.post(
        "/v1/redemptions",
        headers={"Idempotency-Key": "bad"},
        json=request_payload() | {"quantity": 0},
    )

    assert missing_header.status_code == 422
    assert invalid_quantity.status_code == 422
    assert repository.redemptions == {}


def test_get_returns_current_mode_and_state():
    client, _repository = client_and_repository()
    created = client.post(
        "/v1/redemptions",
        headers={"Idempotency-Key": "request-1"},
        json=request_payload("two_phase_commit"),
    ).json()

    response = client.get(f"/v1/redemptions/{created['transaction_id']}")

    assert response.status_code == 200
    assert response.json() == created


def test_valid_transition_is_persisted_and_visible():
    client, _repository = client_and_repository()
    created = client.post(
        "/v1/redemptions",
        headers={"Idempotency-Key": "request-1"},
        json=request_payload(),
    ).json()
    transaction_id = created["transaction_id"]

    transitioned = client.post(
        f"/internal/v1/redemptions/{transaction_id}/transitions",
        json={"to_status": "DEDUCTING_POINTS", "reason": "Saga started"},
    )
    history = client.get(f"/internal/v1/redemptions/{transaction_id}/transitions")

    assert transitioned.status_code == 200
    assert transitioned.json()["status"] == "DEDUCTING_POINTS"
    assert [item["to_status"] for item in history.json()] == [
        "REQUESTED",
        "DEDUCTING_POINTS",
    ]


def test_invalid_transition_returns_conflict_without_changing_state():
    client, _repository = client_and_repository()
    created = client.post(
        "/v1/redemptions",
        headers={"Idempotency-Key": "request-1"},
        json=request_payload(),
    ).json()
    transaction_id = created["transaction_id"]

    response = client.post(
        f"/internal/v1/redemptions/{transaction_id}/transitions",
        json={"to_status": "COMPLETED"},
    )
    current = client.get(f"/v1/redemptions/{transaction_id}")

    assert response.status_code == 409
    assert response.json()["type"] == "urn:ledgermesh:coordinator:invalid-transition"
    assert current.json()["status"] == "REQUESTED"


def test_unknown_redemption_returns_not_found():
    client, _repository = client_and_repository()

    response = client.get(f"/v1/redemptions/{uuid4()}")

    assert response.status_code == 404


def teardown_function():
    app.dependency_overrides.clear()
