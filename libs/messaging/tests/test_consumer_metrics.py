"""InboxConsumer metrics: duplicates rejected, in-process retries, handling time (LM-034)."""

from types import SimpleNamespace

import pytest
from prometheus_client import REGISTRY

from ledgermesh_messaging import InboxConsumer, TransientError

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
