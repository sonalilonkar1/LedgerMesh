"""PostgreSQL integration tests for the immutable Audit Ledger (LM-009)."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from ledgermesh_messaging import EventEnvelope
from sqlalchemy import Engine, delete, func, select, update
from sqlalchemy.exc import IntegrityError

from services.audit_ledger.app.db.schema import audit_events
from services.audit_ledger.app.errors import AuditEventConflict
from services.audit_ledger.app.repository import PostgresAuditRepository

from .conftest import upgrade

pytestmark = pytest.mark.database


@pytest.fixture
def repository(database_url: str, engine: Engine) -> PostgresAuditRepository:
    upgrade("audit_ledger", database_url)
    return PostgresAuditRepository(engine)


def envelope(
    *,
    event_id: UUID | None = None,
    transaction_id: UUID | None = None,
    workflow_step: int = 1,
    occurred_at: datetime | None = None,
    payload: dict | None = None,
) -> EventEnvelope:
    transaction_id = transaction_id or uuid4()
    return EventEnvelope(
        event_id=event_id or uuid4(),
        event_type="redemption.state_changed",
        transaction_id=transaction_id,
        correlation_id=transaction_id,
        aggregate_type="redemption",
        aggregate_id=str(transaction_id),
        aggregate_version=workflow_step,
        workflow_step=workflow_step,
        occurred_at=occurred_at or datetime.now(UTC),
        payload=payload or {"state": f"STEP_{workflow_step}"},
    )


def event_count(engine: Engine) -> int:
    with engine.connect() as connection:
        return connection.scalar(select(func.count()).select_from(audit_events))


def test_append_and_query_preserve_complete_envelope(repository):
    event = envelope()

    appended = repository.append(event)
    queried = repository.list_by_transaction(event.transaction_id)

    assert appended.duplicate is False
    assert appended.envelope == event.to_json()
    assert len(queried) == 1
    assert queried[0].envelope == event.to_json()


def test_identical_retry_returns_the_existing_record(repository):
    event = envelope()

    first = repository.append(event)
    duplicate = repository.append(event)

    assert duplicate.duplicate is True
    assert duplicate.ledger_sequence == first.ledger_sequence
    assert duplicate.recorded_at == first.recorded_at
    assert event_count(repository.engine) == 1


def test_reusing_event_id_with_different_content_is_rejected(repository):
    event_id = uuid4()
    original = envelope(event_id=event_id)
    changed = envelope(
        event_id=event_id,
        transaction_id=original.transaction_id,
        payload={"state": "DIFFERENT"},
    )
    repository.append(original)

    with pytest.raises(AuditEventConflict):
        repository.append(changed)

    assert event_count(repository.engine) == 1


def test_transaction_query_uses_ledger_recording_order(repository):
    transaction_id = uuid4()
    now = datetime.now(UTC)
    first = envelope(transaction_id=transaction_id, workflow_step=2, occurred_at=now)
    second = envelope(
        transaction_id=transaction_id,
        workflow_step=1,
        occurred_at=now - timedelta(minutes=1),
    )
    repository.append(first)
    repository.append(envelope())
    repository.append(second)

    results = repository.list_by_transaction(transaction_id)

    assert [result.ledger_sequence for result in results] == [1, 3]
    assert [result.envelope["workflow_step"] for result in results] == [2, 1]


def test_concurrent_identical_appends_create_one_record(repository):
    event = envelope()
    barrier = Barrier(10)

    def append_once(_):
        barrier.wait()
        return repository.append(event)

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(append_once, range(10)))

    assert sum(not result.duplicate for result in results) == 1
    assert sum(result.duplicate for result in results) == 9
    assert len({result.ledger_sequence for result in results}) == 1
    assert event_count(repository.engine) == 1


def test_caller_owned_transaction_can_roll_back_append(repository):
    with repository.engine.connect() as connection:
        transaction = connection.begin()
        repository.append(envelope(), connection=connection)
        transaction.rollback()

    assert event_count(repository.engine) == 0


@pytest.mark.parametrize("mutation", ["update", "delete"])
def test_database_rejects_changes_to_existing_events(repository, mutation):
    event = envelope()
    repository.append(event)
    statement = (
        update(audit_events)
        .where(audit_events.c.event_id == event.event_id)
        .values(event_type="redemption.changed")
        if mutation == "update"
        else delete(audit_events).where(audit_events.c.event_id == event.event_id)
    )

    with pytest.raises(IntegrityError), repository.engine.begin() as connection:
        connection.execute(statement)

    assert event_count(repository.engine) == 1
