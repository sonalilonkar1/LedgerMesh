import asyncio

import pytest
from sqlalchemy import func, select, update

from ledgermesh_messaging import inbox, outbox, process_once

from .conftest import counters, increment_and_reply, make_envelope, read_counter

pytestmark = pytest.mark.integration

CONSUMER = "wallet-saga"


async def count_rows(session_factory, table) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(table))


async def test_business_update_inbox_and_outbox_commit_together(session_factory):
    envelope = make_envelope()

    result = await process_once(session_factory, CONSUMER, envelope, increment_and_reply)

    assert not result.duplicate
    assert len(result.result_event_ids) == 1
    assert await read_counter(session_factory) == 1
    assert await count_rows(session_factory, inbox) == 1
    async with session_factory() as session:
        row = (await session.execute(select(outbox))).one()
    assert row.event_id == result.result_event_ids[0]
    assert row.published_at is None
    assert row.envelope["causation_id"] == str(envelope.event_id)


async def test_handler_failure_rolls_back_everything(session_factory):
    async def fail_after_writing(session, envelope):
        await increment_and_reply(session, envelope)
        raise RuntimeError("crash inside handler")

    with pytest.raises(RuntimeError):
        await process_once(session_factory, CONSUMER, make_envelope(), fail_after_writing)

    assert await read_counter(session_factory) == 0
    assert await count_rows(session_factory, inbox) == 0
    assert await count_rows(session_factory, outbox) == 0


async def test_failed_attempt_can_be_retried(session_factory):
    envelope = make_envelope()
    calls = 0

    async def fail_first_time(session, env):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient")
        return await increment_and_reply(session, env)

    with pytest.raises(RuntimeError):
        await process_once(session_factory, CONSUMER, envelope, fail_first_time)
    result = await process_once(session_factory, CONSUMER, envelope, fail_first_time)

    assert not result.duplicate
    assert await read_counter(session_factory) == 1


async def test_sequential_duplicate_returns_stored_outcome(session_factory):
    envelope = make_envelope()
    first = await process_once(session_factory, CONSUMER, envelope, increment_and_reply)
    async with session_factory() as session, session.begin():
        await session.execute(update(outbox).values(published_at=func.now()))

    second = await process_once(session_factory, CONSUMER, envelope, increment_and_reply)

    assert second.duplicate
    assert second.result_event_ids == first.result_event_ids
    assert await read_counter(session_factory) == 1
    assert await count_rows(session_factory, outbox) == 1
    async with session_factory() as session:
        published_at = await session.scalar(select(outbox.c.published_at))
    assert published_at is None, "stored reply should be queued for publication again"


async def test_duplicate_without_republish_leaves_outbox_alone(session_factory):
    envelope = make_envelope()
    await process_once(session_factory, CONSUMER, envelope, increment_and_reply)
    async with session_factory() as session, session.begin():
        await session.execute(update(outbox).values(published_at=func.now()))

    await process_once(
        session_factory, CONSUMER, envelope, increment_and_reply, republish_on_duplicate=False
    )

    async with session_factory() as session:
        assert await session.scalar(select(outbox.c.published_at)) is not None


async def test_concurrent_duplicates_apply_exactly_once(session_factory):
    envelope = make_envelope()

    results = await asyncio.gather(
        *(process_once(session_factory, CONSUMER, envelope, increment_and_reply) for _ in range(25))
    )

    assert sum(not r.duplicate for r in results) == 1
    assert len({r.result_event_ids for r in results}) == 1
    assert await read_counter(session_factory) == 1
    assert await count_rows(session_factory, inbox) == 1
    assert await count_rows(session_factory, outbox) == 1


async def test_same_event_is_independent_per_consumer(session_factory):
    envelope = make_envelope()

    await process_once(session_factory, "wallet-saga", envelope, increment_and_reply)
    await process_once(session_factory, "audit-ledger", envelope, increment_and_reply)

    assert await read_counter(session_factory) == 2


async def test_distinct_events_all_apply(session_factory):
    envelopes = [make_envelope() for _ in range(20)]

    await asyncio.gather(
        *(process_once(session_factory, CONSUMER, e, increment_and_reply) for e in envelopes)
    )

    assert await read_counter(session_factory) == 20
    async with session_factory() as session:
        assert await session.scalar(select(counters.c.value)) == 20
