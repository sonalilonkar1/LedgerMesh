"""HTTP contract tests for Catalog's 2PC participant API."""

from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from services.catalog.app.errors import ParticipantConflict, ParticipantTransactionNotFound
from services.catalog.app.main import app, get_participant_service
from services.catalog.app.participant import ParticipantRecord
from services.catalog.app.participant_service import CatalogParticipantService


class FakeParticipantRepository:
    def __init__(self) -> None:
        self.records: dict[UUID, ParticipantRecord] = {}
        self.requests: dict[UUID, tuple] = {}

    def prepare(
        self,
        transaction_id,
        correlation_id,
        reward_id,
        quantity,
        expected_reward_version,
        expected_point_cost,
        fencing_token,
    ):
        request = (
            correlation_id,
            reward_id,
            quantity,
            expected_reward_version,
            expected_point_cost,
            fencing_token,
        )
        if transaction_id in self.records:
            if transaction_id in self.requests and self.requests[transaction_id] != request:
                raise ParticipantConflict("Different prepare request.", transaction_id)
            return self.records[transaction_id]
        self.requests[transaction_id] = request
        state = "PREPARED" if quantity <= 2 and expected_reward_version == 0 else "ABORTED"
        self.records[transaction_id] = ParticipantRecord(transaction_id, state, datetime.now(UTC))
        return self.records[transaction_id]

    def commit(self, transaction_id):
        record = self.get(transaction_id)
        if record.state == "ABORTED":
            raise ParticipantConflict("Already aborted.", transaction_id)
        self.records[transaction_id] = replace(
            record, state="COMMITTED", updated_at=datetime.now(UTC)
        )
        return self.records[transaction_id]

    def abort(self, transaction_id):
        record = self.records.get(transaction_id)
        if record is not None and record.state == "COMMITTED":
            raise ParticipantConflict("Already committed.", transaction_id)
        record = record or ParticipantRecord(transaction_id, "ABORTED", datetime.now(UTC))
        self.records[transaction_id] = replace(
            record, state="ABORTED", updated_at=datetime.now(UTC)
        )
        return self.records[transaction_id]

    def get(self, transaction_id):
        if transaction_id not in self.records:
            raise ParticipantTransactionNotFound("Unknown transaction.", transaction_id)
        return self.records[transaction_id]


def client_and_repository():
    repository = FakeParticipantRepository()
    app.dependency_overrides[get_participant_service] = lambda: CatalogParticipantService(
        repository
    )
    return TestClient(app), repository


def payload(*, correlation_id=None, quantity=1, version=0, cost=100):
    return {
        "correlation_id": str(correlation_id or uuid4()),
        "reward_id": "reward-coffee",
        "quantity": quantity,
        "expected_reward_version": version,
        "expected_point_cost": cost,
        "fencing_token": 0,
    }


def test_prepare_status_and_commit_are_idempotent():
    client, _ = client_and_repository()
    transaction_id, correlation_id = uuid4(), uuid4()
    request = payload(correlation_id=correlation_id)

    first = client.post(f"/v1/transactions/{transaction_id}/prepare", json=request)
    retry = client.post(f"/v1/transactions/{transaction_id}/prepare", json=request)
    committed = client.post(f"/v1/transactions/{transaction_id}/commit")
    repeated = client.post(f"/v1/transactions/{transaction_id}/commit")
    status = client.get(f"/v1/transactions/{transaction_id}")

    assert first.json() == retry.json()
    assert first.json()["participant"] == "catalog"
    assert committed.json()["state"] == repeated.json()["state"] == "COMMITTED"
    assert status.json()["state"] == "COMMITTED"


def test_unknown_commit_and_unknown_abort_have_defined_results():
    client, _ = client_and_repository()
    commit_id, abort_id = uuid4(), uuid4()

    unknown = client.post(f"/v1/transactions/{commit_id}/commit")
    aborted = client.post(f"/v1/transactions/{abort_id}/abort")
    repeated = client.post(f"/v1/transactions/{abort_id}/abort")

    assert unknown.status_code == 404
    assert aborted.json()["state"] == repeated.json()["state"] == "ABORTED"


def test_conflicting_prepare_returns_409_problem_json():
    client, _ = client_and_repository()
    transaction_id, correlation_id = uuid4(), uuid4()
    client.post(
        f"/v1/transactions/{transaction_id}/prepare",
        json=payload(correlation_id=correlation_id),
    )

    response = client.post(
        f"/v1/transactions/{transaction_id}/prepare",
        json=payload(correlation_id=correlation_id, quantity=2),
    )

    assert response.status_code == 409
    assert response.headers["content-type"].startswith("application/problem+json")


def test_stale_quote_is_a_durable_no_vote():
    client, _ = client_and_repository()
    response = client.post(f"/v1/transactions/{uuid4()}/prepare", json=payload(version=99))
    assert response.status_code == 200
    assert response.json()["state"] == "ABORTED"


def teardown_function():
    app.dependency_overrides.clear()
