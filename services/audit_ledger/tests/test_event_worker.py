import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from ledgermesh_messaging import (
    REDEMPTION_EVENTS,
    DeadLetterReason,
    EventEnvelope,
    FailureRouter,
    PermanentError,
    RetryPolicy,
    dead_letter_topic,
)

from services.audit_ledger.app.event_worker import CONSUMER_GROUP, AuditEventHandler


class RecordingRepository:
    def __init__(self):
        self.events = []

    def append(self, envelope, *, connection=None):
        self.events.append(envelope)


class FakeSession:
    async def run_sync(self, function):
        return function(SimpleNamespace(connection=lambda: object()))


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, **kwargs):
        self.sent.append((topic, kwargs))


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


def test_unsupported_event_is_dead_lettered_with_its_reason():
    producer = FakeProducer()
    record = SimpleNamespace(
        topic=REDEMPTION_EVENTS.name,
        partition=0,
        offset=7,
        value=b"event",
        key=b"transaction",
        headers=[],
    )
    router = FailureRouter(producer, consumer=CONSUMER_GROUP, policy=RetryPolicy())

    asyncio.run(
        router.dead_letter(record, PermanentError("unsupported"), DeadLetterReason.PERMANENT_ERROR)
    )

    [(topic, kwargs)] = producer.sent
    assert topic == dead_letter_topic(REDEMPTION_EVENTS).name
    assert (kwargs["value"], kwargs["key"]) == (b"event", b"transaction")
    headers = dict(kwargs["headers"])
    assert headers["lm-dlq-reason"] == b"permanent_error"
    assert headers["lm-dlq-consumer"] == b"audit-ledger"
