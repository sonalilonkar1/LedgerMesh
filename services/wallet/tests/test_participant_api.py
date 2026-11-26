"""HTTP contract tests for Wallet's 2PC participant API."""

from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from services.wallet.app.errors import ParticipantConflict, ParticipantTransactionNotFound
from services.wallet.app.main import app, get_participant_service
from services.wallet.app.participant import ParticipantRecord
from services.wallet.app.participant_service import WalletParticipantService


class FakeParticipantRepository:
    def __init__(self) -> None:
        self.records: dict[UUID, ParticipantRecord] = {}
        self.requests: dict[UUID, tuple[UUID, str, int]] = {}

    def prepare(
        self,
        transaction_id: UUID,
        correlation_id: UUID,
        user_id: str,
        points: int,
        fencing_token: int,
    ) -> ParticipantRecord:
        request = (correlation_id, user_id, points, fencing_token)
        if transaction_id in self.records:
            if transaction_id in self.requests and self.requests[transaction_id] != request:
                raise ParticipantConflict("Different prepare request.", transaction_id)
            return self.records[transaction_id]
        self.requests[transaction_id] = request
        self.records[transaction_id] = ParticipantRecord(
            transaction_id, "PREPARED" if points <= 100 else "ABORTED", datetime.now(UTC)
        )
        return self.records[transaction_id]

    def commit(self, transaction_id: UUID) -> ParticipantRecord:
        record = self.get(transaction_id)
        if record.state == "ABORTED":
            raise ParticipantConflict("Already aborted.", transaction_id)
        self.records[transaction_id] = replace(
            record, state="COMMITTED", updated_at=datetime.now(UTC)
        )
        return self.records[transaction_id]

    def abort(self, transaction_id: UUID) -> ParticipantRecord:
        record = self.records.get(transaction_id)
        if record is not None and record.state == "COMMITTED":
            raise ParticipantConflict("Already committed.", transaction_id)
        if record is None:
            record = ParticipantRecord(transaction_id, "ABORTED", datetime.now(UTC))
        self.records[transaction_id] = replace(
            record, state="ABORTED", updated_at=datetime.now(UTC)
        )
        return self.records[transaction_id]

    def get(self, transaction_id: UUID) -> ParticipantRecord:
        if transaction_id not in self.records:
            raise ParticipantTransactionNotFound("Unknown transaction.", transaction_id)
        return self.records[transaction_id]


def client_and_repository() -> tuple[TestClient, FakeParticipantRepository]:
    repository = FakeParticipantRepository()
    app.dependency_overrides[get_participant_service] = lambda: WalletParticipantService(repository)
    return TestClient(app), repository


def payload(*, correlation_id: UUID | None = None, user_id: str = "user-1", points: int = 40):
    return {
        "correlation_id": str(correlation_id or uuid4()),
        "user_id": user_id,
        "points": points,
        "fencing_token": 0,
    }


def test_prepare_and_status_return_participant_state() -> None:
    client, _ = client_and_repository()
    transaction_id = uuid4()
    request = payload()

    prepared = client.post(f"/v1/transactions/{transaction_id}/prepare", json=request)
    status = client.get(f"/v1/transactions/{transaction_id}")

    assert prepared.status_code == 200
    assert prepared.json()["participant"] == "wallet"
    assert prepared.json()["state"] == "PREPARED"
    assert status.json() == prepared.json()


def test_prepare_retry_and_final_decisions_are_idempotent() -> None:
    client, _ = client_and_repository()
    transaction_id = uuid4()
    request = payload()

    first = client.post(f"/v1/transactions/{transaction_id}/prepare", json=request)
    second = client.post(f"/v1/transactions/{transaction_id}/prepare", json=request)
    committed = client.post(f"/v1/transactions/{transaction_id}/commit")
    repeated = client.post(f"/v1/transactions/{transaction_id}/commit")

    assert first.json() == second.json()
    assert committed.json()["state"] == repeated.json()["state"] == "COMMITTED"


def test_unknown_commit_is_404_and_unknown_abort_creates_tombstone() -> None:
    client, _ = client_and_repository()
    commit_id, abort_id = uuid4(), uuid4()

    unknown_commit = client.post(f"/v1/transactions/{commit_id}/commit")
    aborted = client.post(f"/v1/transactions/{abort_id}/abort")
    repeated = client.post(f"/v1/transactions/{abort_id}/abort")

    assert unknown_commit.status_code == 404
    assert unknown_commit.json()["type"] == "urn:ledgermesh:wallet:participant-not-found"
    assert aborted.json()["state"] == repeated.json()["state"] == "ABORTED"


def test_conflicting_prepare_is_409_problem_json() -> None:
    client, _ = client_and_repository()
    transaction_id = uuid4()
    correlation_id = uuid4()
    client.post(
        f"/v1/transactions/{transaction_id}/prepare",
        json=payload(correlation_id=correlation_id),
    )

    response = client.post(
        f"/v1/transactions/{transaction_id}/prepare",
        json=payload(correlation_id=correlation_id, points=41),
    )

    assert response.status_code == 409
    assert response.headers["content-type"].startswith("application/problem+json")


def teardown_function() -> None:
    app.dependency_overrides.clear()
