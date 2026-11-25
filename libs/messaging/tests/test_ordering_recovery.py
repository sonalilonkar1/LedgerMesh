"""LM-030: future event deferral, causal recovery and stale-event safety."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from ledgermesh_messaging import (
    WALLET_RESULTS,
    DeadLetterReason,
    EventEnvelope,
    FailureRouter,
    InboxConsumer,
    RetryPolicy,
    dead_letter_topic,
    in_version_order,
    retry_topic,
)
from ledgermesh_messaging.retry import DLQ_ERROR, DLQ_REASON, headers_of

from .conftest import increment_and_reply, make_envelope, read_counter

pytestmark = pytest.mark.integration
CONSUMER = "lm030-audit-order"


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, **kwargs):
        self.sent.append((topic, kwargs))


def event(version: int) -> EventEnvelope:
    return make_envelope(
        event_type="wallet.points_deducted",
        aggregate_type="wallet",
        aggregate_id="user-lm030",
        aggregate_version=version,
        workflow_step=1,
        causation_id=uuid4(),
    )


def record(envelope: EventEnvelope, *, topic: str | None = None, headers=(), offset=11):
    return SimpleNamespace(
        topic=topic or WALLET_RESULTS.name,
        partition=0,
        offset=offset,
        key=b"user-lm030",
        value=envelope.to_bytes(),
        headers=list(headers),
    )


def make_consumer(session_factory, producer, *, attempts=1, redeliveries=1):
    policy = RetryPolicy(
        attempts=attempts,
        backoff_seconds=0,
        redeliveries=redeliveries,
        redelivery_delay_seconds=0,
        max_delay_seconds=0,
        handler_timeout_seconds=2,
    )
    router = FailureRouter(
        producer, consumer=CONSUMER, policy=policy, clock=lambda: 1_700_000_000
    )
    return InboxConsumer(
        None,
        session_factory,
        name=CONSUMER,
        handler=in_version_order(increment_and_reply, consumer=CONSUMER),
        router=router,
    )


async def _deliver(session_factory, envelope):
    from ledgermesh_messaging import process_once

    return await process_once(
        session_factory,
        CONSUMER,
        envelope,
        in_version_order(increment_and_reply, consumer=CONSUMER),
    )


async def test_future_event_is_retried_then_applied_after_prerequisite(session_factory):
    producer = FakeProducer()
    consumer = make_consumer(session_factory, producer)
    second = event(2)

    await consumer.handle(record(second))
    # Future events must not reach business logic before their predecessor.
    assert await read_counter(session_factory) == 0
    assert len(producer.sent) == 1
    topic, message = producer.sent[0]
    assert topic == retry_topic(WALLET_RESULTS).name
    assert message["value"] == second.to_bytes()
    assert EventEnvelope.from_bytes(message["value"]).event_id == second.event_id
    retried = record(second, topic=topic, headers=message["headers"])

    await _deliver(session_factory, event(1))
    await consumer.handle(retried)

    assert await read_counter(session_factory) == 2


async def test_stale_event_cannot_reverse_or_repeat_current_state(session_factory):
    await _deliver(session_factory, event(1))
    await _deliver(session_factory, event(2))
    stale = event(1)

    await _deliver(session_factory, stale)

    assert await read_counter(session_factory) == 2


async def test_missing_prerequisite_dead_letters_with_explainable_expected_version(
    session_factory,
):
    producer = FakeProducer()
    consumer = make_consumer(session_factory, producer, attempts=1, redeliveries=0)
    future = event(3)

    await consumer.handle(record(future))

    assert len(producer.sent) == 1
    topic, message = producer.sent[0]
    assert topic == dead_letter_topic(WALLET_RESULTS).name
    headers = headers_of(SimpleNamespace(headers=message["headers"]))
    assert headers[DLQ_REASON] == str(DeadLetterReason.RETRIES_EXHAUSTED)
    assert "arrived before version 1" in headers[DLQ_ERROR]
    assert await read_counter(session_factory) == 0
