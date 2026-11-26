"""LM-030: exercise the real Audit Ledger handler's ordered retry path."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from ledgermesh_messaging import (
    REDEMPTION_EVENTS,
    DeadLetterReason,
    EventEnvelope,
    FailureRouter,
    InboxConsumer,
    RetryPolicy,
    dead_letter_topic,
    retry_topic,
)
from ledgermesh_messaging.retry import DLQ_ERROR, DLQ_REASON, headers_of
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.audit_ledger.app.db.schema import audit_events
from services.audit_ledger.app.event_worker import CONSUMER_GROUP, ordered_audit_handler
from services.audit_ledger.app.repository import PostgresAuditRepository
from tests.integration.conftest import ServiceDatabases

pytestmark = [pytest.mark.database, pytest.mark.integration]


class FakeProducer:
    def __init__(self) -> None:
        self.sent = []

    async def send_and_wait(self, topic, **kwargs) -> None:
        self.sent.append((topic, kwargs))


def transition(transaction_id, version: int, *, cause=None) -> EventEnvelope:
    return EventEnvelope(
        event_type="coordinator.transitioned",
        transaction_id=transaction_id,
        correlation_id=uuid4(),
        causation_id=cause,
        aggregate_type="redemption",
        aggregate_id=str(transaction_id),
        aggregate_version=version,
        workflow_step=version,
        payload={
            "coordination_mode": "saga",
            "to_status": "REQUESTED" if version == 0 else f"STEP_{version}",
        },
    )


def record(envelope: EventEnvelope, *, topic: str = REDEMPTION_EVENTS.name, headers=()):
    return SimpleNamespace(
        topic=topic,
        partition=0,
        offset=4,
        key=str(envelope.transaction_id).encode(),
        value=envelope.to_bytes(),
        headers=list(headers),
    )


def consumer_for(sessions, repository, producer, *, redeliveries: int = 2):
    policy = RetryPolicy(
        attempts=1,
        backoff_seconds=0,
        redeliveries=redeliveries,
        redelivery_delay_seconds=0,
        max_delay_seconds=0,
        handler_timeout_seconds=2,
    )
    router = FailureRouter(producer, consumer=CONSUMER_GROUP, policy=policy)
    return InboxConsumer(
        None,
        sessions,
        name=CONSUMER_GROUP,
        handler=ordered_audit_handler(repository),
        router=router,
    )


async def audit_count(engine) -> int:
    async with engine.connect() as connection:
        return await connection.scalar(select(func.count()).select_from(audit_events))


def run_audit_case(service_databases: ServiceDatabases, scenario) -> None:
    engine = create_async_engine(service_databases.urls["audit_ledger"])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    repository = PostgresAuditRepository(service_databases.engines["audit_ledger"])

    async def run():
        try:
            await scenario(engine, sessions, repository)
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_future_audit_event_waits_for_predecessor_then_is_applied(service_databases):
    async def scenario(engine, sessions, repository):
        producer = FakeProducer()
        consumer = consumer_for(sessions, repository, producer)
        transaction_id = uuid4()
        root = transition(transaction_id, 0)
        later = transition(transaction_id, 1, cause=uuid4())

        await consumer.handle(record(later))

        assert await audit_count(engine) == 0
        assert len(producer.sent) == 1
        retry_name, retry_message = producer.sent.pop()
        assert retry_name == retry_topic(REDEMPTION_EVENTS).name
        assert retry_message["value"] == later.to_bytes()

        await consumer.handle(record(root))
        await consumer.handle(record(later, topic=retry_name, headers=retry_message["headers"]))

        assert await audit_count(engine) == 2
        assert [
            item.envelope["event_id"] for item in repository.list_by_transaction(transaction_id)
        ] == [str(root.event_id), str(later.event_id)]

    run_audit_case(service_databases, scenario)


def test_stale_audit_event_does_not_append_or_reverse_history(service_databases):
    async def scenario(engine, sessions, repository):
        consumer = consumer_for(sessions, repository, FakeProducer())
        transaction_id = uuid4()
        root = transition(transaction_id, 0)
        later = transition(transaction_id, 1, cause=uuid4())

        await consumer.handle(record(root))
        await consumer.handle(record(later))
        stale = transition(transaction_id, 0, cause=uuid4())
        result = await consumer.handle(record(stale))

        assert result is not None and not result.duplicate
        assert await audit_count(engine) == 2
        assert [
            item.envelope["event_id"] for item in repository.list_by_transaction(transaction_id)
        ] == [str(root.event_id), str(later.event_id)]

    run_audit_case(service_databases, scenario)


def test_missing_audit_prerequisite_ends_in_explainable_dlq(service_databases):
    async def scenario(engine, sessions, repository):
        producer = FakeProducer()
        consumer = consumer_for(sessions, repository, producer, redeliveries=1)
        transaction_id = uuid4()
        future = transition(transaction_id, 2, cause=uuid4())

        await consumer.handle(record(future))
        retry_name, retry_message = producer.sent.pop()
        assert retry_name == retry_topic(REDEMPTION_EVENTS).name
        await consumer.handle(record(future, topic=retry_name, headers=retry_message["headers"]))

        dlq_name, dlq_message = producer.sent.pop()
        assert dlq_name == dead_letter_topic(REDEMPTION_EVENTS).name
        headers = headers_of(SimpleNamespace(headers=dlq_message["headers"]))
        assert headers[DLQ_REASON] == str(DeadLetterReason.RETRIES_EXHAUSTED)
        assert "arrived before version 0" in headers[DLQ_ERROR]
        assert await audit_count(engine) == 0

    run_audit_case(service_databases, scenario)
