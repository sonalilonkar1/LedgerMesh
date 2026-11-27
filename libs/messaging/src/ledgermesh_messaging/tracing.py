"""OpenTelemetry tracing shared by every LedgerMesh service (LM-033).

One redemption is one trace, across REST calls, Kafka events and database
work:

- **REST**: ``instrument_fastapi`` continues the caller's trace from the W3C
  ``traceparent`` header, and ``instrument_httpx`` sends it on outgoing calls
  (the Coordinator's 2PC participant and quote clients).
- **Kafka**: ``enqueue`` copies the current trace context into the outbox
  row's headers, the publisher sends them as Kafka headers, and
  ``InboxConsumer`` continues the trace from them. Retry and dead-letter
  republishing keeps them.
- **Database**: ``trace_engine`` adds a span per SQL statement, using
  SQLAlchemy's cursor events.
- **Across a database hop** (the Coordinator API writes a Saga redemption, its
  worker starts it later), ``current_trace_context`` and
  ``use_trace_context`` carry the context through a stored column.

Spans carry LedgerMesh attributes (``ledgermesh.*``): mode, state, retry
count and compensation. ``configure_tracing`` exports over OTLP/HTTP when
``OTEL_EXPORTER_OTLP_ENDPOINT`` is set (Jaeger accepts OTLP); without it,
spans are still created, so context still propagates, but nothing is
exported. ``OTEL_SDK_DISABLED=true`` turns tracing off.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import propagate, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter
from opentelemetry.trace import SpanKind, Status, StatusCode
from sqlalchemy import event

TRACER_NAME = "ledgermesh"

# W3C trace context headers (and baggage), as the global propagator writes them.
TRACE_HEADERS = ("traceparent", "tracestate", "baggage")

# LedgerMesh span attributes.
SERVICE = "ledgermesh.service"
MODE = "ledgermesh.mode"
STATE = "ledgermesh.state"
FROM_STATE = "ledgermesh.from_state"
WORKFLOW_STEP = "ledgermesh.workflow_step"
TRANSACTION_ID = "ledgermesh.transaction_id"
CORRELATION_ID = "ledgermesh.correlation_id"
EVENT_ID = "ledgermesh.event_id"
EVENT_TYPE = "ledgermesh.event_type"
RETRY_COUNT = "ledgermesh.retry_count"
ATTEMPT = "ledgermesh.attempt"
COMPENSATION = "ledgermesh.compensation"

# States in which a redemption is undoing work it already did.
COMPENSATION_STATES = frozenset({"COMPENSATING", "COMPENSATED"})

DISABLED_ENV = "OTEL_SDK_DISABLED"
ENDPOINT_ENVS = ("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
UNTRACED_PATHS = "health,metrics"

_service_name: str | None = None


def tracer() -> trace.Tracer:
    return trace.get_tracer(TRACER_NAME)


def tracing_disabled() -> bool:
    return os.environ.get(DISABLED_ENV, "").strip().lower() == "true"


def configure_tracing(
    service: str, *, exporter: SpanExporter | None = None
) -> TracerProvider | None:
    """Install this process's tracer provider, tagged with ``service``.

    Safe to call more than once: the first call installs the provider (a
    process has one), later calls only add ``exporter`` if given. Tests pass an
    in-memory exporter; services rely on ``OTEL_EXPORTER_OTLP_ENDPOINT``.
    """
    global _service_name
    if tracing_disabled():
        return None
    _service_name = _service_name or service
    provider = trace.get_tracer_provider()
    if not isinstance(provider, TracerProvider):
        name = os.environ.get("OTEL_SERVICE_NAME") or service
        provider = TracerProvider(resource=Resource.create({"service.name": name}))
        if any(os.environ.get(env) for env in ENDPOINT_ENVS):
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)
    if exporter is not None:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider


# --- Propagation ------------------------------------------------------------------


def trace_headers() -> dict[str, str]:
    """The current trace context as W3C headers; empty when there is no span."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


def with_trace_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    """``headers`` plus the current trace context; headers already set win."""
    return {**trace_headers(), **(headers or {})}


def trace_headers_from(headers: Mapping[str, str] | None) -> dict[str, str]:
    """Only the trace context headers in ``headers``, to forward them unchanged."""
    return {name: value for name, value in (headers or {}).items() if name in TRACE_HEADERS}


def current_trace_context() -> dict[str, str] | None:
    """The current trace context to store with a record; ``None`` if untraced."""
    return trace_headers() or None


@contextmanager
def use_trace_context(headers: Mapping[str, str] | None) -> Iterator[None]:
    """Make spans started inside continue the trace in ``headers``.

    With no headers (an untraced or older record) this does nothing, so new
    spans start their own trace.
    """
    if not headers:
        yield
        return
    token = otel_context.attach(propagate.extract(dict(headers)))
    try:
        yield
    finally:
        otel_context.detach(token)


# --- LedgerMesh attributes ---------------------------------------------------------


def _clean(attributes: Mapping[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for name, value in attributes.items():
        if value is None:
            continue
        clean[name] = value if isinstance(value, bool | int | float) else str(value)
    return clean


def envelope_attributes(envelope: Any) -> dict[str, Any]:
    """Span attributes identifying an event and its redemption."""
    payload = getattr(envelope, "payload", None) or {}
    return _clean(
        {
            EVENT_ID: getattr(envelope, "event_id", None),
            EVENT_TYPE: getattr(envelope, "event_type", None),
            TRANSACTION_ID: getattr(envelope, "transaction_id", None),
            CORRELATION_ID: getattr(envelope, "correlation_id", None),
            WORKFLOW_STEP: getattr(envelope, "workflow_step", None),
            MODE: payload.get("coordination_mode") if isinstance(payload, Mapping) else None,
        }
    )


def record_transition_on_span(
    *,
    coordination_mode: str,
    from_status: str | None,
    to_status: str,
    workflow_step: int | None = None,
    reason: str | None = None,
    transaction_id: Any = None,
) -> None:
    """Mark the current span with a redemption state change.

    The span gets the latest mode and state as attributes, plus one
    ``redemption.transition`` event per change, so a span covering several
    transitions keeps all of them. Entering compensation sets
    ``ledgermesh.compensation``.
    """
    span = trace.get_current_span()
    if not span.is_recording():
        return
    compensating = to_status in COMPENSATION_STATES
    attributes = _clean(
        {
            MODE: coordination_mode,
            STATE: to_status,
            FROM_STATE: from_status,
            WORKFLOW_STEP: workflow_step,
            TRANSACTION_ID: transaction_id,
        }
    )
    span.set_attributes(attributes)
    if compensating:
        span.set_attribute(COMPENSATION, True)
    span.add_event(
        "redemption.transition",
        {**attributes, **_clean({"reason": reason, COMPENSATION: compensating})},
    )


# --- Instrumentation ----------------------------------------------------------------


def instrument_fastapi(app: Any) -> None:
    """Server spans for every request, continuing the caller's ``traceparent``.

    ``/health`` and ``/metrics`` are not traced, so probes and scrapes don't
    flood the trace store. The per-message ASGI ``http send`` and ``http
    receive`` spans are left out: one span per request is enough.
    """
    if tracing_disabled():
        return
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(
        app, excluded_urls=UNTRACED_PATHS, exclude_spans=["receive", "send"]
    )


def instrument_httpx() -> None:
    """Client spans for outgoing httpx calls, sending ``traceparent``."""
    if tracing_disabled():
        return
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    instrumentor = HTTPXClientInstrumentor()
    if not instrumentor.is_instrumented_by_opentelemetry:
        instrumentor.instrument()


def trace_engine(engine: Any) -> Any:
    """Add a client span per SQL statement on ``engine`` (sync or async); returns it.

    Spans are named ``<OPERATION> <database>`` (``UPDATE wallet``) and carry
    the statement text. Statements are parameterised, so values (balances,
    user ids) are not in the span.
    """
    sync_engine = getattr(engine, "sync_engine", engine)
    if tracing_disabled() or getattr(sync_engine, "_ledgermesh_traced", False):
        return engine
    database = sync_engine.url.database or "db"

    def start(conn, cursor, statement, parameters, context, executemany) -> None:
        words = statement.split(None, 1)
        operation = words[0].upper() if words else "SQL"
        context._ledgermesh_span = tracer().start_span(
            f"{operation} {database}",
            kind=SpanKind.CLIENT,
            attributes={
                "db.system.name": sync_engine.dialect.name,
                "db.namespace": database,
                "db.operation.name": operation,
                "db.query.text": statement,
            },
        )

    def end(conn, cursor, statement, parameters, context, executemany) -> None:
        if span := getattr(context, "_ledgermesh_span", None):
            span.end()

    def failed(exception_context) -> None:
        context = exception_context.execution_context
        if span := getattr(context, "_ledgermesh_span", None):
            span.record_exception(exception_context.original_exception)
            span.set_status(Status(StatusCode.ERROR))
            span.end()

    event.listen(sync_engine, "before_cursor_execute", start)
    event.listen(sync_engine, "after_cursor_execute", end)
    event.listen(sync_engine, "handle_error", failed)
    sync_engine._ledgermesh_traced = True
    return engine
