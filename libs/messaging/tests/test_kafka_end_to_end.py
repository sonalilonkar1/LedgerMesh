import asyncio
from uuid import uuid4

import pytest
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from ledgermesh_messaging import (
    EventEnvelope,
    InboxConsumer,
    OutboxPublisher,
    OutgoingMessage,
    create_consumer,
    create_producer,
    enqueue,
)

from .conftest import KAFKA_BOOTSTRAP, increment_and_reply, make_envelope, read_counter

pytestmark = [pytest.mark.integration, pytest.mark.kafka]


@pytest.fixture
async def topic():
    name = f"lm-test-{uuid4().hex[:8]}"
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])
        yield name
        await admin.delete_topics([name])
    finally:
        await admin.close()


@pytest.fixture
async def producer():
    producer = create_producer(KAFKA_BOOTSTRAP)
    await producer.start()
    yield producer
    await producer.stop()


async def stage(session_factory, topic: str, count: int, key: str) -> list[EventEnvelope]:
    envelopes = [make_envelope(aggregate_id=key, aggregate_version=i) for i in range(count)]
    async with session_factory() as session, session.begin():
        for envelope in envelopes:
            await enqueue(session, OutgoingMessage(topic=topic, key=key, envelope=envelope))
    return envelopes


async def next_record(consumer, timeout: float = 20.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        batch = await consumer.getmany(timeout_ms=500, max_records=1)
        for records in batch.values():
            return records[0]
    raise AssertionError("no record received")


async def test_outbox_to_inbox_preserves_per_key_order(session_factory, topic, producer):
    envelopes = await stage(session_factory, topic, 10, key="user-42")
    assert await OutboxPublisher(session_factory, producer, name="e2e").publish_batch() == 10

    seen: list[int] = []

    async def record_version(session, envelope):
        seen.append(envelope.aggregate_version)
        return []

    kafka_consumer = create_consumer(KAFKA_BOOTSTRAP, f"g-{uuid4().hex[:6]}", topic)
    await kafka_consumer.start()
    try:
        consumer = InboxConsumer(kafka_consumer, session_factory, name="e2e", handler=record_version)
        for _ in envelopes:
            record = await next_record(kafka_consumer)
            await consumer.handle(record)
            await consumer.commit(record)
    finally:
        await kafka_consumer.stop()

    assert seen == list(range(10))


async def test_redelivery_after_crash_before_offset_commit_is_deduplicated(
    session_factory, topic, producer
):
    await stage(session_factory, topic, 1, key="user-7")
    await OutboxPublisher(session_factory, producer, name="e2e").publish_batch()
    group = f"g-{uuid4().hex[:6]}"

    first = create_consumer(KAFKA_BOOTSTRAP, group, topic)
    await first.start()
    try:
        record = await next_record(first)
        result = await InboxConsumer(
            first, session_factory, name="wallet-saga", handler=increment_and_reply
        ).handle(record)
        assert not result.duplicate
        # Crash here: the database committed but the offset did not.
    finally:
        await first.stop()

    second = create_consumer(KAFKA_BOOTSTRAP, group, topic)
    await second.start()
    try:
        redelivered = await next_record(second)
        consumer = InboxConsumer(second, session_factory, name="wallet-saga", handler=increment_and_reply)
        result = await consumer.handle(redelivered)
        await consumer.commit(redelivered)
    finally:
        await second.stop()

    assert redelivered.offset == record.offset
    assert result.duplicate
    assert await read_counter(session_factory) == 1


async def test_invalid_message_goes_to_dead_letter_hook(session_factory, topic, producer):
    await producer.send_and_wait(topic, b"not an envelope", key=b"user-1")
    dead_letters = []

    async def capture(record, exc):
        dead_letters.append((record.value, type(exc).__name__))

    kafka_consumer = create_consumer(KAFKA_BOOTSTRAP, f"g-{uuid4().hex[:6]}", topic)
    await kafka_consumer.start()
    try:
        consumer = InboxConsumer(
            kafka_consumer, session_factory, name="e2e", handler=None, on_dead_letter=capture
        )
        record = await next_record(kafka_consumer)
        assert await consumer.handle(record) is None
    finally:
        await kafka_consumer.stop()

    assert dead_letters == [(b"not an envelope", "EnvelopeError")]
