import asyncio

import pytest
from sqlalchemy import func, select, update

from ledgermesh_messaging import EventEnvelope, OutgoingMessage, enqueue, inbox, outbox, process_once

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


async def test_duplicate_returns_stored_outcome_without_requeueing(session_factory):
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
    assert published_at is not None, "replay is opt-in, so the stored reply stays published"


async def test_duplicate_with_replay_requeues_stored_reply(session_factory):
    envelope = make_envelope()
    await process_once(session_factory, CONSUMER, envelope, increment_and_reply)
    async with session_factory() as session, session.begin():
        await session.execute(update(outbox).values(published_at=func.now()))

    await process_once(
        session_factory, CONSUMER, envelope, increment_and_reply, republish_on_duplicate=True
    )

    async with session_factory() as session:
        assert await session.scalar(select(outbox.c.published_at)) is None
        assert await session.scalar(select(func.count()).select_from(outbox)) == 1


def forward_to(topic: str | None):
    """Handler that applies one business change and optionally emits the next step."""

    async def handler(session, envelope):
        await session.execute(
            update(counters).where(counters.c.name == "applied").values(value=counters.c.value + 1)
        )
        if topic is None:
            return []
        next_step = make_envelope(
            transaction_id=envelope.transaction_id,
            correlation_id=envelope.correlation_id,
            causation_id=envelope.event_id,
        )
        return [OutgoingMessage(topic=topic, key=str(envelope.transaction_id), envelope=next_step)]

    return handler


async def deliver_until_quiet(session_factory, routes, *, deliveries: int, max_rounds: int = 10) -> int:
    """Act as the message bus: deliver every unpublished outbox row ``deliveries``
    times to the consumer for its topic, until no unpublished rows remain.
    Returns the number of rounds needed."""
    for round_number in range(1, max_rounds + 1):
        async with session_factory() as session, session.begin():
            rows = (
                await session.execute(
                    select(outbox).where(outbox.c.published_at.is_(None)).order_by(outbox.c.id)
                )
            ).all()
            if not rows:
                return round_number - 1
            await session.execute(
                update(outbox)
                .where(outbox.c.id.in_([row.id for row in rows]))
                .values(published_at=func.now())
            )

        for row in rows:
            consumer, handler, replay = routes[row.topic]
            envelope = EventEnvelope.model_validate(row.envelope)
            for _ in range(deliveries):
                await process_once(
                    session_factory, consumer, envelope, handler, republish_on_duplicate=replay
                )
    raise AssertionError(f"outbox still had unpublished rows after {max_rounds} rounds")


@pytest.mark.parametrize("replay", [False, True])
async def test_duplicate_storm_across_services_terminates(session_factory, replay):
    """Wallet -> Coordinator -> Ledger, with every message delivered three times."""
    routes = {
        "wallet.commands": ("wallet-saga", forward_to("wallet.results"), replay),
        "wallet.results": ("coordinator", forward_to("ledger.events"), replay),
        "ledger.events": ("audit-ledger", forward_to(None), replay),
    }
    async with session_factory() as session, session.begin():
        await enqueue(
            session,
            OutgoingMessage(topic="wallet.commands", key="user-1", envelope=make_envelope()),
        )

    rounds = await deliver_until_quiet(session_factory, routes, deliveries=3)

    assert rounds == 3, "one round per workflow step, however many duplicates arrive"
    assert await read_counter(session_factory) == 3, "each service applied its step exactly once"
    assert await count_rows(session_factory, inbox) == 3
    assert await count_rows(session_factory, outbox) == 3


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
