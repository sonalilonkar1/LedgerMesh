"""InboxConsumer metrics: duplicates rejected, in-process retries, handling time (LM-034)."""

from types import SimpleNamespace

import pytest
from prometheus_client import REGISTRY

from ledgermesh_messaging import InboxConsumer, PermanentError, TransientError, record_transition

from .conftest import increment_and_reply, make_envelope

pytestmark = pytest.mark.integration


def value(name: str, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def record(envelope):
    return SimpleNamespace(
        topic="wallet.commands",
        partition=0,
        offset=0,
        key=b"k",
        value=envelope.to_bytes(),
        headers=[],
    )


def committed(state: str) -> float:
    return value("ledgermesh_transitions_total", mode="saga", state=state)


async def test_a_handler_failing_after_the_transition_write_counts_nothing(session_factory):
    # Like the Coordinator's result handler writing a transition, then failing
    # on a later statement: the inbox transaction rolls back, so nothing counts.
    async def transition_then_fail(session, envelope):
        await increment_and_reply(session, envelope)
        record_transition(mode="saga", from_state="RESERVING_INVENTORY", to_state="COMPLETED")
        raise PermanentError("a later write failed")

    consumer = InboxConsumer(
        None,
        session_factory,
        name="metrics-rollback",
        handler=transition_then_fail,
        on_dead_letter=_ignore,
    )
    before = committed("COMPLETED")
    outcomes = value("ledgermesh_redemptions_total", mode="saga", outcome="COMPLETED")

    await consumer.handle(record(make_envelope()))

    assert committed("COMPLETED") == before
    assert value("ledgermesh_redemptions_total", mode="saga", outcome="COMPLETED") == outcomes


async def test_a_transition_retried_after_a_rollback_is_counted_once(session_factory):
    calls = []

    async def transition_then_flake(session, envelope):
        calls.append(1)
        await increment_and_reply(session, envelope)
        record_transition(mode="saga", from_state="DEDUCTING_POINTS", to_state="POINTS_DEDUCTED")
        if len(calls) == 1:
            raise TransientError("first attempt rolls back")
        return []

    consumer = InboxConsumer(
        None,
        session_factory,
        name="metrics-retry-once",
        handler=transition_then_flake,
        max_attempts=2,
        retry_backoff=0,
    )
    before = committed("POINTS_DEDUCTED")

    await consumer.handle(record(make_envelope()))

    assert len(calls) == 2
    assert committed("POINTS_DEDUCTED") == before + 1


async def _ignore(record, error):
    pass


async def test_a_redelivered_message_counts_as_a_rejected_duplicate(session_factory):
    consumer = InboxConsumer(None, session_factory, name="metrics-dup", handler=increment_and_reply)
    message = record(make_envelope())
    before = value("ledgermesh_duplicates_rejected_total", consumer="metrics-dup")
    handled = value("ledgermesh_message_handling_seconds_count", consumer="metrics-dup")

    await consumer.handle(message)
    await consumer.handle(message)

    assert value("ledgermesh_duplicates_rejected_total", consumer="metrics-dup") == before + 1
    assert value("ledgermesh_message_handling_seconds_count", consumer="metrics-dup") == handled + 2


async def test_in_process_retries_are_counted(session_factory):
    calls = []

    async def flaky(session, envelope):
        calls.append(1)
        if len(calls) < 3:
            raise TransientError("busy")
        return await increment_and_reply(session, envelope)

    consumer = InboxConsumer(
        None, session_factory, name="metrics-retry", handler=flaky, max_attempts=3, retry_backoff=0
    )
    before = value("ledgermesh_retries_total", consumer="metrics-retry", tier="in_process")

    await consumer.handle(record(make_envelope()))

    assert value("ledgermesh_retries_total", consumer="metrics-retry", tier="in_process") == (
        before + 2
    )
