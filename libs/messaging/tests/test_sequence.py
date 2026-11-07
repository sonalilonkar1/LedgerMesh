import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

from ledgermesh_messaging import InboxConsumer, inbox, process_once
from ledgermesh_messaging.schema import aggregate_versions
from ledgermesh_messaging.sequence import FutureEventError, in_version_order

from .conftest import increment_and_reply, make_envelope, read_counter

pytestmark = pytest.mark.integration

CONSUMER = "audit-ledger"

ordered = in_version_order(increment_and_reply, consumer=CONSUMER)


def wallet_event(version: int, aggregate_id: str = "user-1"):
    return make_envelope(
        event_type="PointsDeducted", aggregate_id=aggregate_id, aggregate_version=version
    )


async def deliver(session_factory, envelope, handler=ordered, consumer=CONSUMER):
    return await process_once(session_factory, consumer, envelope, handler)


async def latest_version(session_factory, aggregate_id="user-1", consumer=CONSUMER):
    async with session_factory() as session:
        return await session.scalar(
            select(aggregate_versions.c.version).where(
                aggregate_versions.c.consumer == consumer,
                aggregate_versions.c.aggregate_id == aggregate_id,
            )
        )


async def inbox_rows(session_factory):
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(inbox))


async def test_in_order_events_are_applied_and_the_latest_version_is_stored(session_factory):
    for version in (1, 2, 3):
        await deliver(session_factory, wallet_event(version))

    assert await read_counter(session_factory) == 3
    assert await latest_version(session_factory) == 3


async def test_older_version_is_ignored_without_running_the_handler(session_factory):
    await deliver(session_factory, wallet_event(1))
    await deliver(session_factory, wallet_event(2))

    result = await deliver(session_factory, wallet_event(1))

    assert not result.duplicate
    assert result.result_event_ids == ()
    assert await read_counter(session_factory) == 2
    assert await latest_version(session_factory) == 2


async def test_ignored_stale_event_is_recorded_so_redelivery_is_a_duplicate(session_factory):
    await deliver(session_factory, wallet_event(1))
    stale = wallet_event(1)

    await deliver(session_factory, stale)
    redelivered = await deliver(session_factory, stale)

    assert redelivered.duplicate
    assert await inbox_rows(session_factory) == 2


async def test_future_version_is_not_applied_early(session_factory):
    with pytest.raises(FutureEventError) as raised:
        await deliver(session_factory, wallet_event(2))

    assert raised.value.expected == 1
    assert await read_counter(session_factory) == 0
    assert await inbox_rows(session_factory) == 0
    assert await latest_version(session_factory) is None


async def test_future_version_succeeds_after_its_predecessor_arrives(session_factory):
    early = wallet_event(2)
    with pytest.raises(FutureEventError):
        await deliver(session_factory, early)

    await deliver(session_factory, wallet_event(1))
    result = await deliver(session_factory, early)

    assert not result.duplicate
    assert await read_counter(session_factory) == 2
    assert await latest_version(session_factory) == 2


async def test_versions_are_tracked_per_aggregate(session_factory):
    await deliver(session_factory, wallet_event(1, aggregate_id="user-1"))
    await deliver(session_factory, wallet_event(1, aggregate_id="user-2"))

    assert await read_counter(session_factory) == 2
    assert await latest_version(session_factory, "user-1") == 1
    assert await latest_version(session_factory, "user-2") == 1


async def test_versions_are_tracked_per_consumer(session_factory):
    other = in_version_order(increment_and_reply, consumer="coordinator-saga")
    event = wallet_event(1)

    await deliver(session_factory, event)
    await deliver(session_factory, event, handler=other, consumer="coordinator-saga")

    assert await read_counter(session_factory) == 2


async def test_concurrent_events_with_the_same_version_apply_once(session_factory):
    events = [wallet_event(1) for _ in range(10)]

    results = await asyncio.gather(*(deliver(session_factory, event) for event in events))

    assert sum(1 for result in results if result.result_event_ids) == 1
    assert await read_counter(session_factory) == 1
    assert await latest_version(session_factory) == 1


async def test_custom_first_version(session_factory):
    from_zero = in_version_order(increment_and_reply, consumer=CONSUMER, first_version=0)

    await deliver(session_factory, wallet_event(0), handler=from_zero)

    assert await read_counter(session_factory) == 1
    assert await latest_version(session_factory) == 0


async def test_event_without_a_version_is_rejected(session_factory):
    command = make_envelope(event_type="DeductPoints")

    with pytest.raises(ValueError, match="no aggregate_version"):
        await deliver(session_factory, command)

    assert await inbox_rows(session_factory) == 0


async def test_consumer_retries_a_future_event_then_dead_letters_it(session_factory):
    dead_letters = []

    async def capture(record, exc):
        dead_letters.append(exc)

    consumer = InboxConsumer(
        None,
        session_factory,
        name=CONSUMER,
        handler=ordered,
        on_dead_letter=capture,
        max_attempts=3,
        retry_backoff=0,
    )

    result = await consumer.handle(SimpleNamespace(value=wallet_event(5).to_bytes()))

    assert result is None
    assert [type(exc) for exc in dead_letters] == [FutureEventError]
    assert await read_counter(session_factory) == 0
