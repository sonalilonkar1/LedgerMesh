from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from services.catalog.app.errors import (
    InsufficientInventory,
    InvalidQuantity,
    InvalidReservationTransition,
    ReservationNotFound,
    RewardNotFound,
)
from services.catalog.app.main import app, get_catalog_service
from services.catalog.app.repository import ReservationRecord, RewardRecord
from services.catalog.app.service import CatalogService


class FakeCatalogRepository:
    def __init__(self) -> None:
        now = datetime.now(UTC)
        self.rewards = {
            "reward-coffee": RewardRecord("reward-coffee", "Coffee", 100, 2, 2, 0, now),
            "reward-sold-out": RewardRecord("reward-sold-out", "Poster", 300, 0, 0, 0, now),
        }
        self.reservations: dict[UUID, ReservationRecord] = {}

    def list_rewards(self, *, available_only: bool = True) -> list[RewardRecord]:
        values = sorted(self.rewards.values(), key=lambda reward: reward.reward_id)
        if available_only:
            values = [reward for reward in values if reward.available_inventory > 0]
        return values

    def get_reward(self, reward_id: str) -> RewardRecord:
        try:
            return self.rewards[reward_id]
        except KeyError as error:
            raise RewardNotFound(f"Reward {reward_id!r} does not exist.") from error

    def reserve(
        self, reward_id: str, transaction_id: UUID, quantity: int, event_id: UUID | None
    ) -> ReservationRecord:
        del event_id
        reward = self.get_reward(reward_id)
        if transaction_id in self.reservations:
            return replace(self.reservations[transaction_id], duplicate=True)
        if reward.available_inventory < quantity:
            raise InsufficientInventory("Not enough inventory.", transaction_id)
        reward = replace(
            reward,
            available_inventory=reward.available_inventory - quantity,
            version=reward.version + 1,
            updated_at=datetime.now(UTC),
        )
        self.rewards[reward_id] = reward
        result = ReservationRecord(
            transaction_id,
            reward_id,
            quantity,
            "reserved",
            reward.available_inventory,
            reward.version,
            datetime.now(UTC),
        )
        self.reservations[transaction_id] = result
        return result

    def release(
        self, reward_id: str, transaction_id: UUID, quantity: int, event_id: UUID | None
    ) -> ReservationRecord:
        del event_id
        reward = self.get_reward(reward_id)
        existing = self.reservations.get(transaction_id)
        if existing is None:
            result = ReservationRecord(
                transaction_id,
                reward_id,
                quantity,
                "tombstone",
                reward.available_inventory,
                reward.version,
                datetime.now(UTC),
                tombstone=True,
            )
            self.reservations[transaction_id] = result
            return result
        if existing.state in {"released", "tombstone"}:
            return replace(existing, duplicate=True, tombstone=existing.state == "tombstone")
        if existing.state == "fulfilled":
            raise InvalidReservationTransition("Already fulfilled.", transaction_id)
        reward = replace(
            reward,
            available_inventory=reward.available_inventory + existing.quantity,
            version=reward.version + 1,
            updated_at=datetime.now(UTC),
        )
        self.rewards[reward_id] = reward
        result = replace(
            existing,
            state="released",
            available_inventory=reward.available_inventory,
            version=reward.version,
            updated_at=datetime.now(UTC),
        )
        self.reservations[transaction_id] = result
        return result

    def fulfill(
        self, reward_id: str, transaction_id: UUID, event_id: UUID | None
    ) -> ReservationRecord:
        del event_id
        self.get_reward(reward_id)
        existing = self.reservations.get(transaction_id)
        if existing is None:
            raise ReservationNotFound("Reservation not found.", transaction_id)
        if existing.state == "fulfilled":
            return replace(existing, duplicate=True)
        if existing.state != "reserved":
            raise InvalidReservationTransition("Reservation is not active.", transaction_id)
        result = replace(existing, state="fulfilled", updated_at=datetime.now(UTC))
        self.reservations[transaction_id] = result
        return result


def client_and_repository() -> tuple[TestClient, FakeCatalogRepository]:
    repository = FakeCatalogRepository()
    app.dependency_overrides[get_catalog_service] = lambda: CatalogService(repository)
    return TestClient(app), repository


def test_catalog_lists_available_rewards_by_default():
    client, _repository = client_and_repository()

    response = client.get("/v1/rewards")

    assert response.status_code == 200
    assert [reward["reward_id"] for reward in response.json()] == ["reward-coffee"]


def test_catalog_can_include_sold_out_rewards():
    client, _repository = client_and_repository()

    response = client.get("/v1/rewards?available_only=false")

    assert response.status_code == 200
    assert {reward["reward_id"] for reward in response.json()} == {
        "reward-coffee",
        "reward-sold-out",
    }


def test_reward_query_returns_inventory_cost_and_version():
    client, _repository = client_and_repository()

    response = client.get("/v1/rewards/reward-coffee")

    assert response.status_code == 200
    assert response.json()["point_cost"] == 100
    assert response.json()["available_inventory"] == 2
    assert response.json()["version"] == 0


def test_reservation_and_release_are_linked_by_transaction_id():
    client, _repository = client_and_repository()
    transaction_id = str(uuid4())
    payload = {"transaction_id": transaction_id, "quantity": 1}

    reserved = client.post("/v1/rewards/reward-coffee/reservations", json=payload)
    released = client.post("/v1/rewards/reward-coffee/releases", json=payload)

    assert reserved.status_code == 200
    assert (reserved.json()["available_inventory"], reserved.json()["version"]) == (1, 1)
    assert released.status_code == 200
    assert released.json()["transaction_id"] == transaction_id
    assert released.json()["state"] == "released"
    assert (released.json()["available_inventory"], released.json()["version"]) == (2, 2)


def test_reservation_rejects_unavailable_inventory_as_problem_json():
    client, _repository = client_and_repository()

    response = client.post(
        "/v1/rewards/reward-sold-out/reservations",
        json={"transaction_id": str(uuid4()), "quantity": 1},
    )

    assert response.status_code == 409
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["type"] == "urn:ledgermesh:catalog:insufficient-inventory"


def test_invalid_quantity_is_rejected_for_http_and_non_http_callers():
    client, repository = client_and_repository()
    response = client.post(
        "/v1/rewards/reward-coffee/reservations",
        json={"transaction_id": str(uuid4()), "quantity": 0},
    )
    assert response.status_code == 422

    with pytest.raises(InvalidQuantity):
        CatalogService(repository).reserve("reward-coffee", uuid4(), 0)
    assert repository.rewards["reward-coffee"].available_inventory == 2


def test_retry_returns_duplicate_without_a_second_inventory_change():
    client, _repository = client_and_repository()
    payload = {"transaction_id": str(uuid4()), "quantity": 1}

    first = client.post("/v1/rewards/reward-coffee/reservations", json=payload)
    second = client.post("/v1/rewards/reward-coffee/reservations", json=payload)

    assert first.json()["outcome"] == "applied"
    assert second.json()["outcome"] == "duplicate"
    assert second.json()["available_inventory"] == 1
    assert second.json()["version"] == 1


def test_release_before_reserve_returns_a_tombstone():
    client, _repository = client_and_repository()
    payload = {"transaction_id": str(uuid4()), "quantity": 1}

    response = client.post("/v1/rewards/reward-coffee/releases", json=payload)

    assert response.status_code == 200
    assert response.json()["state"] == "tombstone"
    assert response.json()["outcome"] == "tombstone"
    assert response.json()["available_inventory"] == 2


def test_fulfilled_reservation_cannot_be_released():
    client, _repository = client_and_repository()
    transaction_id = str(uuid4())
    payload = {"transaction_id": transaction_id, "quantity": 1}
    client.post("/v1/rewards/reward-coffee/reservations", json=payload)
    client.post("/v1/rewards/reward-coffee/fulfillments", json={"transaction_id": transaction_id})

    response = client.post("/v1/rewards/reward-coffee/releases", json=payload)

    assert response.status_code == 409
    assert response.json()["type"] == "urn:ledgermesh:catalog:invalid-transition"


def teardown_function():
    app.dependency_overrides.clear()
