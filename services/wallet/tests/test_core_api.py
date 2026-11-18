from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from services.wallet.app.errors import (
    DeductionNotFound,
    InsufficientPoints,
    InvalidAmount,
    WalletNotFound,
)
from services.wallet.app.main import app, get_wallet_service
from services.wallet.app.repository import BalanceRecord, OperationRecord
from services.wallet.app.service import WalletService


class FakeWalletRepository:
    def __init__(self) -> None:
        self.balance = BalanceRecord("user-1", 100, 0, datetime.now(UTC))
        self.deductions: dict[UUID, OperationRecord] = {}
        self.refunds: dict[UUID, OperationRecord] = {}

    def get_balance(self, user_id: str) -> BalanceRecord:
        if user_id != self.balance.user_id:
            raise WalletNotFound(f"Wallet {user_id!r} does not exist.")
        return self.balance

    def deduct(
        self, user_id: str, transaction_id: UUID, points: int, event_id: UUID | None
    ) -> OperationRecord:
        del event_id
        self.get_balance(user_id)
        if transaction_id in self.deductions:
            return replace(self.deductions[transaction_id], duplicate=True)
        if points > self.balance.available_points:
            raise InsufficientPoints("Not enough points.", transaction_id)
        self.balance = replace(
            self.balance,
            available_points=self.balance.available_points - points,
            version=self.balance.version + 1,
            updated_at=datetime.now(UTC),
        )
        result = OperationRecord(
            **self.balance.__dict__,
            transaction_id=transaction_id,
            operation="deduct",
            points=points,
        )
        self.deductions[transaction_id] = result
        return result

    def refund(self, user_id: str, transaction_id: UUID, event_id: UUID | None) -> OperationRecord:
        del event_id
        self.get_balance(user_id)
        if transaction_id in self.refunds:
            return replace(self.refunds[transaction_id], duplicate=True)
        deduction = self.deductions.get(transaction_id)
        if deduction is None:
            raise DeductionNotFound("Original deduction not found.", transaction_id)
        self.balance = replace(
            self.balance,
            available_points=self.balance.available_points + deduction.points,
            version=self.balance.version + 1,
            updated_at=datetime.now(UTC),
        )
        result = OperationRecord(
            **self.balance.__dict__,
            transaction_id=transaction_id,
            operation="refund",
            points=deduction.points,
        )
        self.refunds[transaction_id] = result
        return result


def client_and_repository() -> tuple[TestClient, FakeWalletRepository]:
    repository = FakeWalletRepository()
    app.dependency_overrides[get_wallet_service] = lambda: WalletService(repository)
    return TestClient(app), repository


def test_balance_returns_current_points_and_version():
    client, _repository = client_and_repository()

    response = client.get("/v1/wallets/user-1")

    assert response.status_code == 200
    assert response.json()["available_points"] == 100
    assert response.json()["version"] == 0


def test_deduction_and_refund_are_linked_by_transaction_id():
    client, _repository = client_and_repository()
    transaction_id = str(uuid4())

    deducted = client.post(
        "/v1/wallets/user-1/deductions",
        json={"transaction_id": transaction_id, "points": 40},
    )
    refunded = client.post("/v1/wallets/user-1/refunds", json={"transaction_id": transaction_id})

    assert deducted.status_code == 200
    assert deducted.json()["available_points"] == 60
    assert deducted.json()["version"] == 1
    assert refunded.status_code == 200
    assert refunded.json()["transaction_id"] == transaction_id
    assert refunded.json()["points"] == 40
    assert refunded.json()["available_points"] == 100
    assert refunded.json()["version"] == 2


def test_deduction_rejects_insufficient_points_as_problem_json():
    client, _repository = client_and_repository()

    response = client.post(
        "/v1/wallets/user-1/deductions",
        json={"transaction_id": str(uuid4()), "points": 101},
    )

    assert response.status_code == 409
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["type"] == "urn:ledgermesh:wallet:insufficient-points"


def test_invalid_amounts_are_rejected_before_the_repository_is_called():
    client, repository = client_and_repository()

    for points in (0, -1):
        response = client.post(
            "/v1/wallets/user-1/deductions",
            json={"transaction_id": str(uuid4()), "points": points},
        )
        assert response.status_code == 422

    assert repository.balance.available_points == 100
    assert repository.deductions == {}


def test_service_rejects_invalid_amount_for_non_http_callers():
    repository = FakeWalletRepository()
    service = WalletService(repository)

    with pytest.raises(InvalidAmount):
        service.deduct("user-1", uuid4(), 0)

    assert repository.balance.available_points == 100


def test_retry_returns_the_stored_outcome_without_a_second_mutation():
    client, _repository = client_and_repository()
    payload = {"transaction_id": str(uuid4()), "points": 25}

    first = client.post("/v1/wallets/user-1/deductions", json=payload)
    second = client.post("/v1/wallets/user-1/deductions", json=payload)

    assert first.json()["outcome"] == "applied"
    assert second.json()["outcome"] == "duplicate"
    assert second.json()["available_points"] == 75
    assert second.json()["version"] == 1


def test_refund_requires_the_original_deduction():
    client, _repository = client_and_repository()

    response = client.post("/v1/wallets/user-1/refunds", json={"transaction_id": str(uuid4())})

    assert response.status_code == 404
    assert response.json()["type"] == "urn:ledgermesh:wallet:deduction-not-found"


def teardown_function():
    app.dependency_overrides.clear()
