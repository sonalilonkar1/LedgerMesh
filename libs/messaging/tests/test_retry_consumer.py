"""InboxConsumer with a FailureRouter: timeouts, retry topic, DLQ and due times (LM-024)."""

import asyncio
import time
from types import SimpleNamespace

import pytest

from ledgermesh_messaging import (
    FailureRouter,
    InboxConsumer,
    PermanentError,
    RetryPolicy,
    TransientError,
)
from ledgermesh_messaging.retry import (
    DLQ_REASON,
    NOT_BEFORE_MS,
    RETRY_COUNT,
    headers_of,
)

from .conftest import increment_and_reply, make_envelope, read_counter

pytestmark = pytest.mark.integration

NAME = "wallet-saga"


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, **kwargs):
        self.sent.append((topic, kwargs))


def record(envelope=None, topic="wallet.commands", headers=(), offset=0):
    return SimpleNamespace(
        topic=topic,
        partition=0,
        offset=offset,
        key=b"user-1",
        value=(envelope or make_envelope()).to_bytes(),
        headers=list(headers),
    )


def as_record(sent, offset=0):
    topic, kwargs = sent
    return SimpleNamespace(
        topic=topic,
        partition=0,
        offset=offset,
        key=kwargs["key"],
        value=kwargs["value"],
        headers=kwargs["headers"],
    )


def consumer(session_factory, handler, producer, **policy):
    defaults = {"attempts": 2, "backoff_seconds": 0, "redelivery_delay_seconds": 0}
    route = FailureRouter(producer, consumer=NAME, policy=RetryPolicy(**{**defaults, **policy}))
    return InboxConsumer(None, session_factory, name=NAME, handler=handler, router=route)


def failing(times, error=TransientError):
    """A handler that fails ``times`` times, then applies normally."""
    calls = []

    async def handler(session, envelope):
        calls.append(envelope.event_id)
        if len(calls) <= times:
            raise error(f"failure {len(calls)}")
        return await increment_and_reply(session, envelope)

    handler.calls = calls
    return handler


async def test_a_transient_failure_that_clears_within_the_attempts_applies_once(session_factory):
    producer = FakeProducer()
    handler = failing(1)

    result = await consumer(session_factory, handler, producer, attempts=2).handle(record())

    assert result is not None
    assert producer.sent == []
    assert await read_counter(session_factory) == 1


async def test_exhausted_attempts_move_to_the_retry_topic_and_apply_there(session_factory):
    producer = FakeProducer()
    handler = failing(2)
    main = consumer(session_factory, handler, producer, attempts=2)
    retry = consumer(session_factory, handler, producer, attempts=2)

    assert await main.handle(record()) is None
    [(topic, _kwargs)] = producer.sent
    assert topic == "wallet.commands.retry"
    assert await read_counter(session_factory) == 0

    result = await retry.handle(as_record(producer.sent[0]))

    assert result is not None
    assert await read_counter(session_factory) == 1
    assert len(producer.sent) == 1


async def test_a_message_that_keeps_failing_ends_on_the_dlq_with_its_reason(session_factory):
    producer = FakeProducer()
    handler = failing(100)
    workers = consumer(session_factory, handler, producer, attempts=1, redeliveries=2)
    envelope = make_envelope()

    await workers.handle(record(envelope))
    while producer.sent[-1][0].endswith(".retry"):
        await workers.handle(as_record(producer.sent[-1], offset=len(producer.sent)))

    assert [topic for topic, _ in producer.sent] == [
        "wallet.commands.retry",
        "wallet.commands.retry",
        "wallet.commands.dlq",
    ]
    parked = as_record(producer.sent[-1])
    assert headers_of(parked)[DLQ_REASON] == "retries_exhausted"
    assert headers_of(parked)[RETRY_COUNT] == "2"
    # The same event and transaction ids all the way through.
    assert set(handler.calls) == {envelope.event_id}
    assert await read_counter(session_factory) == 0


async def test_a_permanent_error_skips_the_retry_topic(session_factory):
    producer = FakeProducer()

    await consumer(session_factory, failing(1, PermanentError), producer).handle(record())

    assert [topic for topic, _ in producer.sent] == ["wallet.commands.dlq"]
    assert headers_of(as_record(producer.sent[0]))[DLQ_REASON] == "permanent_error"


async def test_an_invalid_envelope_is_dead_lettered_with_its_reason(session_factory):
    producer = FakeProducer()
    bad = SimpleNamespace(
        topic="wallet.commands", partition=0, offset=3, key=b"k", value=b"{", headers=[]
    )

    await consumer(session_factory, increment_and_reply, producer).handle(bad)

    assert [topic for topic, _ in producer.sent] == ["wallet.commands.dlq"]
    assert headers_of(as_record(producer.sent[0]))[DLQ_REASON] == "invalid_envelope"


async def test_a_handler_that_exceeds_the_timeout_is_retried_not_applied(session_factory):
    producer = FakeProducer()

    async def slow(session, envelope):
        await increment_and_reply(session, envelope)
        await asyncio.sleep(5)
        return []

    await consumer(session_factory, slow, producer, attempts=2, handler_timeout_seconds=0.1).handle(
        record()
    )

    assert [topic for topic, _ in producer.sent] == ["wallet.commands.retry"]
    assert "TimeoutError" in headers_of(as_record(producer.sent[0]))["lm-last-error"]
    # The timed-out attempt's write was rolled back.
    assert await read_counter(session_factory) == 0


async def test_the_retry_consumer_waits_until_a_message_is_due(session_factory):
    producer = FakeProducer()
    worker = consumer(session_factory, increment_and_reply, producer)
    due = time.time() + 0.5
    retried = record(headers=[(NOT_BEFORE_MS, str(int(due * 1000)).encode())])

    stop = asyncio.Event()
    started = time.monotonic()
    assert await worker._wait_until_due(retried, stop)

    assert time.monotonic() - started >= 0.4


async def test_stopping_while_waiting_does_not_process_the_message(session_factory):
    worker = consumer(session_factory, increment_and_reply, FakeProducer())
    later = record(headers=[(NOT_BEFORE_MS, str(int((time.time() + 60) * 1000)).encode())])
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.1, stop.set)

    assert await worker._wait_until_due(later, stop) is False
    assert await read_counter(session_factory) == 0
