from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from fastapi.testclient import TestClient
from ledgermesh_messaging import EventEnvelope

from services.audit_ledger.app.errors import AuditEventConflict
from services.audit_ledger.app.main import app, get_audit_ledger_service
from services.audit_ledger.app.repository import AuditRecord
from services.audit_ledger.app.service import AuditLedgerService


def envelope_payload(
    *,
    event_id: UUID | None = None,
    transaction_id: UUID | None = None,
    workflow_step: int = 1,
    occurred_at: datetime | None = None,
) -> dict:
    transaction_id = transaction_id or uuid4()
    return {
        "schema_version": 1,
        "event_id": str(event_id or uuid4()),
        "event_type": "redemption.state_changed",
        "transaction_id": str(transaction_id),
        "correlation_id": str(transaction_id),
        "causation_id": None,
        "aggregate_type": "redemption",
        "aggregate_id": str(transaction_id),
        "aggregate_version": workflow_step,
        "workflow_step": workflow_step,
        "fencing_token": None,
        "occurred_at": (occurred_at or datetime.now(UTC)).isoformat().replace("+00:00", "Z"),
        "payload": {"state": f"STEP_{workflow_step}"},
    }


class FakeAuditRepository:
    def __init__(self) -> None:
        self.records: dict[UUID, AuditRecord] = {}

    def append(self, envelope: EventEnvelope) -> AuditRecord:
        existing = self.records.get(envelope.event_id)
        document = envelope.to_json()
        if existing is not None:
            if existing.envelope != document:
                raise AuditEventConflict(
                    "The event ID is already stored with different content.", envelope.event_id
                )
            return replace(existing, duplicate=True)
        record = AuditRecord(
            ledger_sequence=len(self.records) + 1,
            recorded_at=datetime.now(UTC),
            envelope=document,
        )
        self.records[envelope.event_id] = record
        return record

    def list_by_transaction(self, transaction_id: UUID) -> list[AuditRecord]:
        return [
            record
            for record in self.records.values()
            if record.envelope["transaction_id"] == str(transaction_id)
        ]


def client_and_repository() -> tuple[TestClient, FakeAuditRepository]:
    repository = FakeAuditRepository()
    app.dependency_overrides[get_audit_ledger_service] = lambda: AuditLedgerService(repository)
    return TestClient(app), repository


def test_append_returns_ledger_sequence_and_full_envelope():
    client, _repository = client_and_repository()
    payload = envelope_payload()

    response = client.post("/v1/audit-events", json=payload)

    assert response.status_code == 200
    assert response.json()["outcome"] == "appended"
    assert response.json()["ledger_sequence"] == 1
    assert response.json()["envelope"] == payload


def test_identical_retry_returns_duplicate_without_second_record():
    client, repository = client_and_repository()
    payload = envelope_payload()

    first = client.post("/v1/audit-events", json=payload)
    second = client.post("/v1/audit-events", json=payload)

    assert first.json()["outcome"] == "appended"
    assert second.json()["outcome"] == "duplicate"
    assert second.json()["ledger_sequence"] == first.json()["ledger_sequence"]
    assert len(repository.records) == 1


def test_same_event_id_with_different_content_returns_conflict():
    client, _repository = client_and_repository()
    event_id = uuid4()
    original = envelope_payload(event_id=event_id)
    changed = original | {"payload": {"state": "DIFFERENT"}}
    client.post("/v1/audit-events", json=original)

    response = client.post("/v1/audit-events", json=changed)

    assert response.status_code == 409
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["type"] == "urn:ledgermesh:audit-ledger:event-conflict"
    assert response.json()["event_id"] == str(event_id)


def test_query_returns_only_transaction_events_in_ledger_order():
    client, _repository = client_and_repository()
    transaction_id = uuid4()
    later_time = datetime.now(UTC)
    first = envelope_payload(transaction_id=transaction_id, workflow_step=2, occurred_at=later_time)
    second = envelope_payload(
        transaction_id=transaction_id,
        workflow_step=1,
        occurred_at=later_time - timedelta(minutes=1),
    )
    client.post("/v1/audit-events", json=first)
    client.post("/v1/audit-events", json=envelope_payload())
    client.post("/v1/audit-events", json=second)

    response = client.get(f"/v1/transactions/{transaction_id}/events")

    assert response.status_code == 200
    assert [event["ledger_sequence"] for event in response.json()] == [1, 3]
    assert [event["envelope"]["workflow_step"] for event in response.json()] == [2, 1]


def test_all_wire_envelope_fields_are_required():
    client, _repository = client_and_repository()
    payload = envelope_payload()
    payload.pop("causation_id")

    response = client.post("/v1/audit-events", json=payload)

    assert response.status_code == 422


def test_audit_event_mutation_routes_do_not_exist():
    client, _repository = client_and_repository()
    event_id = uuid4()

    for method in ("PUT", "PATCH", "DELETE"):
        response = client.request(method, f"/v1/audit-events/{event_id}", json={})
        assert response.status_code == 404


def teardown_function():
    app.dependency_overrides.clear()
