import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from ledgermesh_messaging import EventEnvelope, PermanentError

from services.audit_ledger.app.event_worker import AuditEventHandler


class RecordingRepository:
    def __init__(self):
        self.events = []

    def append(self, envelope, *, connection=None):
        self.events.append(envelope)


class FakeSession:
    async def run_sync(self, function):
        return function(SimpleNamespace(connection=lambda: object()))


def envelope(event_type="coordinator.transitioned"):
    return EventEnvelope(
        event_type=event_type,
        transaction_id=uuid4(),
        correlation_id=uuid4(),
        aggregate_type="redemption",
        aggregate_id="redemption-1",
        workflow_step=1,
    )


def test_transition_event_is_appended():
    repository = RecordingRepository()
    assert asyncio.run(AuditEventHandler(repository)(FakeSession(), envelope())) == []
    assert len(repository.events) == 1


def test_unrelated_event_is_rejected():
    with pytest.raises(PermanentError):
        asyncio.run(
            AuditEventHandler(RecordingRepository())(FakeSession(), envelope("wallet.changed"))
        )
