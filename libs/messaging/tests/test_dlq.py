"""ledgermesh-dlq reads a dead-letter topic up to its captured end (LM-024). No Docker needed.

The fake consumer's ``position()`` is a coroutine, like aiokafka's. The
original loop condition passed an async generator to ``any()`` and raised
``TypeError``; these tests would have failed on it.
"""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest

from ledgermesh_messaging import EventEnvelope
from ledgermesh_messaging.dlq import read_dead_letters
from ledgermesh_messaging.retry import DLQ_CONSUMER, DLQ_REASON

TOPIC = "wallet.commands.dlq"


def dead_letter(partition: int, offset: int, reason: str = "permanent_error"):
    envelope = EventEnvelope(
        event_type="wallet.deduct_points",
        transaction_id=uuid4(),
        correlation_id=uuid4(),
        aggregate_type="wallet",
        aggregate_id="user-1",
        workflow_step=1,
    )
    return SimpleNamespace(
        topic=TOPIC,
        partition=partition,
        offset=offset,
        key=b"user-1",
        value=envelope.to_bytes(),
        headers=[(DLQ_REASON, reason.encode()), (DLQ_CONSUMER, b"wallet-saga")],
    )


class FakeConsumer:
    """Serves records in small batches and tracks positions like aiokafka."""

    def __init__(self, partitions: dict[int, int], *, arriving_later: dict[int, int] | None = None):
        self.records = {
            partition: [dead_letter(partition, offset) for offset in range(count)]
            for partition, count in partitions.items()
        }
        self.arriving_later = arriving_later or {}
        self.positions = {}
        self.started = self.stopped = False
        self.getmany_calls = 0

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    def partitions_for_topic(self, topic):
        return set(self.records) if topic == TOPIC else None

    def assign(self, partitions):
        self.assigned = partitions

    async def seek_to_beginning(self, *partitions):
        for partition in partitions:
            self.positions[partition] = 0

    async def end_offsets(self, partitions):
        end = {tp: len(self.records[tp.partition]) for tp in partitions}
        # Dead letters that land after the end offsets were captured.
        for partition, count in self.arriving_later.items():
            start = len(self.records[partition])
            self.records[partition] += [
                dead_letter(partition, offset) for offset in range(start, start + count)
            ]
        return end

    async def position(self, partition):
        return self.positions[partition]

    async def getmany(self, *partitions, timeout_ms):
        self.getmany_calls += 1
        if self.getmany_calls > 50:
            raise AssertionError("read_dead_letters did not stop")
        batches = {}
        for tp in partitions:
            start = self.positions[tp]
            batch = self.records[tp.partition][start : start + 2]
            if batch:
                batches[tp] = batch
                self.positions[tp] = start + len(batch)
        return batches


def read(consumer, limit=None):
    return asyncio.run(read_dead_letters("unused:9092", TOPIC, limit, consumer=consumer))


def test_reads_every_message_up_to_the_end_offsets_in_several_batches():
    consumer = FakeConsumer({0: 5, 1: 3})

    entries = read(consumer)

    assert len(entries) == 8
    assert consumer.getmany_calls >= 3
    assert {entry["reason"] for entry in entries} == {"permanent_error"}
    assert all(entry["event"]["transaction_id"] for entry in entries)
    assert consumer.started and consumer.stopped


def test_messages_arriving_after_the_end_offsets_were_captured_are_left_for_later():
    consumer = FakeConsumer({0: 3}, arriving_later={0: 4})

    entries = read(consumer)

    assert [entry["dlq_offset"] for entry in entries] == [0, 1, 2]


def test_an_empty_topic_returns_nothing_without_polling():
    consumer = FakeConsumer({0: 0})

    assert read(consumer) == []
    assert consumer.getmany_calls == 0


def test_limit_keeps_the_newest_messages():
    consumer = FakeConsumer({0: 6})

    entries = read(consumer, limit=2)

    assert [entry["dlq_offset"] for entry in entries] == [4, 5]


def test_a_missing_topic_exits_and_still_stops_the_consumer():
    consumer = FakeConsumer({})

    with pytest.raises(SystemExit, match="does not exist"):
        read(consumer)

    assert consumer.stopped
