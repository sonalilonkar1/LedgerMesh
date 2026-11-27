"""OpenTelemetry trace propagation through the outbox, Kafka headers and the inbox (LM-033)."""

from types import SimpleNamespace

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from ledgermesh_messaging import (
    DeadLetterReason,
    FailureRouter,
    InboxConsumer,
    OutgoingMessage,
    PermanentError,
    RetryPolicy,
    TransientError,
    configure_tracing,
    current_trace_context,
    enqueue,
    outbox,
    record_transition_on_span,
    trace_engine,
    trace_headers,
    use_trace_context,
)
from ledgermesh_messaging.retry import RETRY_COUNT as RETRY_COUNT_HEADER
from ledgermesh_messaging.retry import headers_of
from ledgermesh_messaging.tracing import (
    ATTEMPT,
    COMPENSATION,
    FROM_STATE,
    MODE,
    RETRY_COUNT,
    STATE,
    TRANSACTION_ID,
    tracer,
    with_trace_headers,
)

from .conftest import DATABASE_URL, increment_and_reply, make_envelope

EXPORTER = InMemorySpanExporter()
configure_tracing("messaging-tests", exporter=EXPORTER)


@pytest.fixture(autouse=True)
def spans():
    EXPORTER.clear()
    yield EXPORTER
    EXPORTER.clear()


def trace_id_of(traceparent: str) -> int:
    return int(traceparent.split("-")[1], 16)


def kafka_record(envelope, headers=None, topic="wallet.commands"):
    return SimpleNamespace(
        topic=topic,
        partition=0,
        offset=7,
        key=b"k",
        value=envelope.to_bytes(),
        headers=[(name, value.encode()) for name, value in (headers or {}).items()],
    )


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, **kwargs):
        self.sent.append((topic, kwargs))


# --- Propagation helpers (no Docker) -----------------------------------------------


def test_there_are_no_trace_headers_outside_a_span():
    assert trace_headers() == {}
    assert current_trace_context() is None


def test_trace_headers_carry_the_current_span():
    with tracer().start_as_current_span("work") as span:
        headers = trace_headers()

    assert trace_id_of(headers["traceparent"]) == span.get_span_context().trace_id


def test_headers_set_by_the_caller_win():
    with tracer().start_as_current_span("work"):
        headers = with_trace_headers({"traceparent": "explicit", "x-other": "1"})

    assert headers["traceparent"] == "explicit"
    assert headers["x-other"] == "1"


def test_a_stored_context_is_continued_later():
    with tracer().start_as_current_span("api request") as request:
        stored = current_trace_context()

    with use_trace_context(stored), tracer().start_as_current_span("worker") as worker:
        pass

    assert worker.get_span_context().trace_id == request.get_span_context().trace_id
    assert worker.parent.span_id == request.get_span_context().span_id


def test_no_stored_context_starts_a_new_trace():
    with use_trace_context(None), tracer().start_as_current_span("worker") as worker:
        pass

    assert worker.parent is None


def test_a_transition_marks_the_span_with_mode_and_state():
    with tracer().start_as_current_span("step") as span:
        record_transition_on_span(
            coordination_mode="saga",
            from_status="REQUESTED",
            to_status="DEDUCTING_POINTS",
            workflow_step=1,
            reason="points requested",
            transaction_id="tx-1",
        )

    assert span.attributes[MODE] == "saga"
    assert span.attributes[STATE] == "DEDUCTING_POINTS"
    assert span.attributes[FROM_STATE] == "REQUESTED"
    assert span.attributes[TRANSACTION_ID] == "tx-1"
    assert COMPENSATION not in span.attributes
    [event] = span.events
    assert event.name == "redemption.transition"
    assert event.attributes["reason"] == "points requested"
    assert event.attributes[COMPENSATION] is False


def test_entering_compensation_is_marked_on_the_span():
    with tracer().start_as_current_span("step") as span:
        record_transition_on_span(
            coordination_mode="saga", from_status="RESERVING_INVENTORY", to_status="COMPENSATING"
        )
        record_transition_on_span(
            coordination_mode="saga", from_status="COMPENSATING", to_status="COMPENSATED"
        )

    assert span.attributes[COMPENSATION] is True
    assert span.attributes[STATE] == "COMPENSATED"
    assert [event.attributes[STATE] for event in span.events] == ["COMPENSATING", "COMPENSATED"]


# --- Retry and dead-letter republishing keeps the trace (no Docker) -------------------


def router(producer, **policy):
    return FailureRouter(
        producer,
        consumer="wallet-saga",
        policy=RetryPolicy(**{"attempts": 1, "backoff_seconds": 0, **policy}),
    )


async def test_a_retried_message_keeps_its_trace_context():
    producer = FakeProducer()
    traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    incoming = kafka_record(make_envelope(), {"traceparent": traceparent, "tracestate": "a=1"})

    await router(producer).retry_or_dead_letter(incoming, TransientError())

    [(topic, sent)] = producer.sent
    headers = headers_of(SimpleNamespace(headers=sent["headers"]))
    assert topic == "wallet.commands.retry"
    assert headers["traceparent"] == traceparent
    assert headers["tracestate"] == "a=1"


async def test_a_dead_lettered_message_keeps_its_trace_context():
    producer = FakeProducer()
    traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    incoming = kafka_record(make_envelope(), {"traceparent": traceparent})

    await router(producer).dead_letter(incoming, PermanentError(), DeadLetterReason.PERMANENT_ERROR)

    [(topic, sent)] = producer.sent
    assert topic == "wallet.commands.dlq"
    assert headers_of(SimpleNamespace(headers=sent["headers"]))["traceparent"] == traceparent


# --- Outbox, consumer and database spans (PostgreSQL) ---------------------------------


@pytest.mark.integration
async def test_enqueue_stores_the_trace_context_in_the_outbox_headers(session_factory):
    message = OutgoingMessage(topic="wallet.results", key="k", envelope=make_envelope())
    with tracer().start_as_current_span("handler") as span:
        async with session_factory() as session, session.begin():
            await enqueue(session, message)

    async with session_factory() as session:
        headers = await session.scalar(select(outbox.c.headers))
    assert trace_id_of(headers["traceparent"]) == span.get_span_context().trace_id


@pytest.mark.integration
async def test_the_consumer_continues_the_producers_trace_and_passes_it_on(session_factory):
    with tracer().start_as_current_span("producer") as producer:
        headers = trace_headers()
    envelope = make_envelope(workflow_step=1, payload={"coordination_mode": "saga"})
    consumer = InboxConsumer(None, session_factory, name="wallet-saga", handler=increment_and_reply)

    await consumer.handle(kafka_record(envelope, headers))

    [span] = [s for s in EXPORTER.get_finished_spans() if s.kind == SpanKind.CONSUMER]
    assert span.name == "wallet.commands process"
    assert span.context.trace_id == producer.get_span_context().trace_id
    assert span.parent.span_id == producer.get_span_context().span_id
    assert span.attributes["messaging.system"] == "kafka"
    assert span.attributes["messaging.consumer.group.name"] == "wallet-saga"
    assert span.attributes["ledgermesh.event_id"] == str(envelope.event_id)
    assert span.attributes[TRANSACTION_ID] == str(envelope.transaction_id)
    assert span.attributes[MODE] == "saga"
    assert span.attributes[RETRY_COUNT] == 0
    assert span.attributes[ATTEMPT] == 1

    # The reply the handler enqueued carries the consumer span's context onward.
    async with session_factory() as session:
        reply_headers = await session.scalar(select(outbox.c.headers))
    assert trace_id_of(reply_headers["traceparent"]) == producer.get_span_context().trace_id
    assert reply_headers["traceparent"].split("-")[2] == format(span.context.span_id, "016x")


@pytest.mark.integration
async def test_retries_and_the_retry_count_are_recorded_on_the_span(session_factory):
    calls = []

    async def flaky(session, envelope):
        calls.append(envelope.event_id)
        if len(calls) < 3:
            raise TransientError("busy")
        return await increment_and_reply(session, envelope)

    consumer = InboxConsumer(
        None,
        session_factory,
        name="wallet-saga",
        handler=flaky,
        router=router(FakeProducer(), attempts=3),
    )
    redelivered = kafka_record(make_envelope(workflow_step=1), {RETRY_COUNT_HEADER: "2"})

    await consumer.handle(redelivered)

    [span] = [s for s in EXPORTER.get_finished_spans() if s.kind == SpanKind.CONSUMER]
    assert span.attributes[ATTEMPT] == 3
    assert span.attributes[RETRY_COUNT] == 2
    assert [event.name for event in span.events] == ["retry", "retry"]
    assert span.status.status_code != StatusCode.ERROR


@pytest.mark.integration
async def test_a_dead_lettered_message_marks_its_span_as_failed(session_factory):
    async def reject(session, envelope):
        raise PermanentError("bad command")

    consumer = InboxConsumer(
        None, session_factory, name="wallet-saga", handler=reject, router=router(FakeProducer())
    )

    await consumer.handle(kafka_record(make_envelope(workflow_step=1)))

    [span] = [s for s in EXPORTER.get_finished_spans() if s.kind == SpanKind.CONSUMER]
    assert span.status.status_code == StatusCode.ERROR
    assert "dead_letter" in [event.name for event in span.events]


@pytest.mark.integration
async def test_sql_statements_are_child_spans_without_their_values():
    engine = trace_engine(create_async_engine(DATABASE_URL))
    try:
        sessions = async_sessionmaker(engine)
        with tracer().start_as_current_span("handler") as parent:
            async with sessions() as session:
                await session.execute(text("SELECT :secret_value"), {"secret_value": "s3cret"})
    finally:
        await engine.dispose()

    [span] = [s for s in EXPORTER.get_finished_spans() if s.kind == SpanKind.CLIENT]
    assert span.name.startswith("SELECT ")
    assert span.parent.span_id == parent.get_span_context().span_id
    assert span.attributes["db.system.name"] == "postgresql"
    assert "s3cret" not in str(span.attributes)


def test_tracing_is_a_no_op_when_disabled(monkeypatch):
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    engine = SimpleNamespace(url=None)

    assert configure_tracing("disabled") is None
    assert trace_engine(engine) is engine
    assert isinstance(trace.get_tracer_provider(), object)
