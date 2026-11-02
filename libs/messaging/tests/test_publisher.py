import asyncio
import json

import pytest
from aiokafka.errors import KafkaConnectionError
from sqlalchemy import select

from ledgermesh_messaging import OutboxPublisher, OutgoingMessage, enqueue, outbox

from .conftest import make_envelope

pytestmark = pytest.mark.integration


class RecordingProducer:
    """Fake producer that records sends and can fail on chosen calls."""

    def __init__(self, fail_on: dict[int, BaseException] | None = None, delay: float = 0.0):
        self.sent: list[tuple[str, dict, bytes, dict]] = []
        self._fail_on = fail_on or {}
        self._delay = delay
        self._calls = 0

    async def send_and_wait(self, topic, value=None, key=None, partition=None, timestamp_ms=None, headers=None):
        self._calls += 1
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._calls in self._fail_on:
            raise self._fail_on[self._calls]
        self.sent.append((topic, json.loads(value), key, dict(headers or [])))


class SimulatedCrash(BaseException):
    """Not caught by the publisher, like a process dying mid-batch."""


async def stage(session_factory, count: int, key: str = "user-1") -> list:
    envelopes = [make_envelope(aggregate_version=i) for i in range(count)]
    async with session_factory() as session, session.begin():
        for envelope in envelopes:
            await enqueue(
                session,
                OutgoingMessage(
                    topic="wallet.results",
                    key=key,
                    envelope=envelope,
                    headers={"traceparent": "00-abc-def-01"},
                ),
            )
    return envelopes


async def unpublished(session_factory) -> int:
    async with session_factory() as session:
        rows = await session.execute(select(outbox.c.id).where(outbox.c.published_at.is_(None)))
        return len(rows.all())


async def test_publishes_in_insertion_order_and_marks_rows(session_factory):
    envelopes = await stage(session_factory, 5)
    producer = RecordingProducer()

    published = await OutboxPublisher(session_factory, producer, name="t").publish_batch()

    assert published == 5
    assert [sent[1]["event_id"] for sent in producer.sent] == [str(e.event_id) for e in envelopes]
    assert all(sent[2] == b"user-1" for sent in producer.sent)
    assert producer.sent[0][3] == {"traceparent": b"00-abc-def-01"}
    assert await unpublished(session_factory) == 0


async def test_kafka_failure_stops_batch_to_preserve_order(session_factory):
    envelopes = await stage(session_factory, 4)
    producer = RecordingProducer(fail_on={3: KafkaConnectionError("broker down")})
    publisher = OutboxPublisher(session_factory, producer, name="t")

    assert await publisher.publish_batch() == 2
    assert await unpublished(session_factory) == 2
    async with session_factory() as session:
        failed = (await session.execute(select(outbox).order_by(outbox.c.id).offset(2).limit(1))).one()
    assert failed.attempts == 1
    assert "broker down" in failed.last_error

    assert await publisher.publish_batch() == 2
    assert [sent[1]["event_id"] for sent in producer.sent] == [str(e.event_id) for e in envelopes]


async def test_crash_after_send_redelivers_same_event(session_factory):
    (envelope,) = await stage(session_factory, 1)
    crashing = RecordingProducer()

    async def send_then_crash(*args, **kwargs):
        await RecordingProducer.send_and_wait(crashing, *args, **kwargs)
        raise SimulatedCrash

    crashing.send_and_wait = send_then_crash
    with pytest.raises(SimulatedCrash):
        await OutboxPublisher(session_factory, crashing, name="t").publish_batch()
    assert await unpublished(session_factory) == 1

    healthy = RecordingProducer()
    await OutboxPublisher(session_factory, healthy, name="t").publish_batch()

    assert crashing.sent[0][1]["event_id"] == healthy.sent[0][1]["event_id"] == str(envelope.event_id)
    assert await unpublished(session_factory) == 0


async def test_only_one_publisher_runs_at_a_time(session_factory):
    await stage(session_factory, 10)
    slow = RecordingProducer(delay=0.02)
    other = RecordingProducer()

    first, second = await asyncio.gather(
        OutboxPublisher(session_factory, slow, name="wallet").publish_batch(),
        OutboxPublisher(session_factory, other, name="wallet").publish_batch(),
    )

    assert sorted([first, second]) == [0, 10]
    assert len(slow.sent) + len(other.sent) == 10


async def test_run_loop_drains_and_stops(session_factory):
    await stage(session_factory, 3)
    producer = RecordingProducer()
    stop = asyncio.Event()
    task = asyncio.create_task(
        OutboxPublisher(session_factory, producer, name="t", poll_interval=0.01).run(stop)
    )

    for _ in range(200):
        if len(producer.sent) == 3:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert len(producer.sent) == 3
