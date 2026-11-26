"""InboxConsumer binds a message's ids to every log while handling it (LM-032)."""

import json
import logging
from types import SimpleNamespace

import pytest

from ledgermesh_messaging import (
    FailureRouter,
    InboxConsumer,
    PermanentError,
    RetryPolicy,
    TransientError,
)
from ledgermesh_messaging.logs import JsonFormatter

from .conftest import increment_and_reply, make_envelope

pytestmark = pytest.mark.integration

handler_logger = logging.getLogger("test.handler")


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.setFormatter(JsonFormatter("wallet"))
        self.lines = []

    def emit(self, record):
        self.lines.append(json.loads(self.format(record)))


@pytest.fixture
def captured():
    capture = Capture()
    root = logging.getLogger()
    previous = root.level
    root.addHandler(capture)
    root.setLevel(logging.INFO)
    yield capture.lines
    root.removeHandler(capture)
    root.setLevel(previous)


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, **kwargs):
        self.sent.append((topic, kwargs))


def record(envelope):
    return SimpleNamespace(
        topic="wallet.commands", partition=0, offset=0, key=b"k", value=envelope.to_bytes(), headers=[]
    )


def consumer(session_factory, handler, **policy):
    router = FailureRouter(
        FakeProducer(),
        consumer="wallet-saga",
        policy=RetryPolicy(**{"attempts": 1, "backoff_seconds": 0, **policy}),
    )
    return InboxConsumer(None, session_factory, name="wallet-saga", handler=handler, router=router)


async def test_handler_logs_carry_the_message_ids(session_factory, captured):
    async def logging_handler(session, envelope):
        handler_logger.info("applying")
        return await increment_and_reply(session, envelope)

    message = make_envelope(workflow_step=1)

    await consumer(session_factory, logging_handler).handle(record(message))

    [line] = [line for line in captured if line["message"] == "applying"]
    assert line["transaction_id"] == str(message.transaction_id)
    assert line["event_id"] == str(message.event_id)
    assert line["correlation_id"] == str(message.correlation_id)
    assert line["event_type"] == message.event_type
    assert line["workflow_step"] == 1


async def test_retry_and_dead_letter_logs_carry_the_message_ids(session_factory, captured):
    async def always_busy(session, envelope):
        raise TransientError("busy")

    async def always_broken(session, envelope):
        raise PermanentError("broken")

    busy, broken = make_envelope(), make_envelope()

    await consumer(session_factory, always_busy).handle(record(busy))
    await consumer(session_factory, always_broken).handle(record(broken))

    retry = next(line for line in captured if "retrying via" in line["message"])
    parked = next(line for line in captured if "dead-lettering" in line["message"])
    assert retry["transaction_id"] == str(busy.transaction_id)
    assert parked["transaction_id"] == str(broken.transaction_id)


async def test_the_binding_ends_with_the_message(session_factory, captured):
    await consumer(session_factory, increment_and_reply).handle(record(make_envelope()))
    handler_logger.info("between messages")

    [line] = [line for line in captured if line["message"] == "between messages"]
    assert line["transaction_id"] is None
