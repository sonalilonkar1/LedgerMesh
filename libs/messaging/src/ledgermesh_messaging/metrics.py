"""Prometheus metrics shared by every LedgerMesh process (LM-034).

Every metric is declared here, so the names and labels are the same in every
service and the Grafana dashboard can rely on them:

| Metric | Labels | Recorded by |
|---|---|---|
| ``ledgermesh_redemptions_total`` | ``mode``, ``outcome`` | Coordinator, on a terminal state |
| ``ledgermesh_redemption_duration_seconds`` | ``mode``, ``outcome`` | Coordinator: request -> terminal state |
| ``ledgermesh_transitions_total`` | ``mode``, ``state`` | Coordinator, every state change |
| ``ledgermesh_compensations_total`` | ``mode``, ``attempt`` | Coordinator, entering ``COMPENSATING`` |
| ``ledgermesh_2pc_prepared_hold_seconds`` | ``service``, ``decision`` | 2PC participants: prepare -> decision |
| ``ledgermesh_retries_total`` | ``consumer``, ``tier`` | ``InboxConsumer`` / ``FailureRouter`` |
| ``ledgermesh_dead_letters_total`` | ``consumer``, ``reason`` | ``FailureRouter`` |
| ``ledgermesh_duplicates_rejected_total`` | ``consumer`` | ``InboxConsumer`` (inbox hit) |
| ``ledgermesh_message_handling_seconds`` | ``consumer`` | ``InboxConsumer`` |
| ``ledgermesh_http_request_duration_seconds`` | ``service``, ``method``, ``route``, ``status`` | ``MetricsMiddleware`` |
| ``ledgermesh_invariant_violations`` | ``invariant`` | the invariant exporter |

``mode`` is the redemption's ``coordination_mode`` (``two_phase_commit`` or
``saga``), which is what lets the dashboard put the two side by side.
"""

from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    start_http_server,
)

logger = logging.getLogger(__name__)

# Covers a sub-10 ms local call up to a two-minute stuck workflow.
LATENCY_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
)  # fmt: skip

TERMINAL_STATES = frozenset({"COMMITTED", "ABORTED", "COMPLETED", "FAILED", "COMPENSATED"})
SUCCESS_STATES = frozenset({"COMMITTED", "COMPLETED"})

REDEMPTIONS = Counter(
    "ledgermesh_redemptions",
    "Redemptions that reached a terminal state.",
    ["mode", "outcome"],
)
REDEMPTION_SECONDS = Histogram(
    "ledgermesh_redemption_duration_seconds",
    "Time from the redemption request to its terminal state.",
    ["mode", "outcome"],
    buckets=LATENCY_BUCKETS,
)
TRANSITIONS = Counter(
    "ledgermesh_transitions",
    "Coordinator state changes.",
    ["mode", "state"],
)
COMPENSATIONS = Counter(
    "ledgermesh_compensations",
    "Saga compensation steps started (attempt=first) or retried (attempt=retry).",
    ["mode", "attempt"],
)
PREPARED_HOLD_SECONDS = Histogram(
    "ledgermesh_2pc_prepared_hold_seconds",
    "How long a prepared 2PC transaction held its locks before the decision arrived.",
    ["service", "decision"],
    buckets=LATENCY_BUCKETS,
)
RETRIES = Counter(
    "ledgermesh_retries",
    "Message retries: tier=in_process (same delivery) or retry_topic (republished).",
    ["consumer", "tier"],
)
DEAD_LETTERS = Counter(
    "ledgermesh_dead_letters",
    "Messages parked on a dead-letter topic, by reason.",
    ["consumer", "reason"],
)
DUPLICATES = Counter(
    "ledgermesh_duplicates_rejected",
    "Redelivered messages the inbox recognised and did not apply again.",
    ["consumer"],
)
HANDLING_SECONDS = Histogram(
    "ledgermesh_message_handling_seconds",
    "Time to handle one consumed message, retries included.",
    ["consumer"],
    buckets=LATENCY_BUCKETS,
)
HTTP_SECONDS = Histogram(
    "ledgermesh_http_request_duration_seconds",
    "HTTP request latency.",
    ["service", "method", "route", "status"],
    buckets=LATENCY_BUCKETS,
)
INVARIANT_VIOLATIONS = Gauge(
    "ledgermesh_invariant_violations",
    "Violations found by the most recent invariant check.",
    ["invariant"],
)

METRICS_PORT_ENV = "LEDGERMESH_METRICS_PORT"


# --- Publishing only what committed ------------------------------------------
#
# A transition is written inside a database transaction that may still roll
# back (a later statement fails, the handler raises, the inbox insert loses a
# race) and be retried. Counting it immediately would count transitions that
# never became durable, and count retries twice. So inside ``metrics_on_commit``
# the recordings are only collected, and published when the block exits
# normally, which every caller arranges to be after its transaction commits.
# On an exception they are discarded.

_PENDING: ContextVar[list[Callable[[], None]] | None] = ContextVar(
    "ledgermesh_pending_metrics", default=None
)


@contextmanager
def metrics_on_commit() -> Iterator[None]:
    """Publish metrics recorded inside the block only if it completes normally.

    Wrap the code that owns a database transaction, so that the block ends
    after the commit: ``with metrics_on_commit(), engine.begin() as c: ...``.
    Nested blocks hand their metrics to the outermost one, so nothing is
    published before the outermost transaction has committed.
    """
    outer = _PENDING.get()
    pending: list[Callable[[], None]] = []
    token = _PENDING.set(pending)
    try:
        yield
    except BaseException:
        _PENDING.reset(token)
        raise
    _PENDING.reset(token)
    if outer is not None:
        outer.extend(pending)
        return
    for publish in pending:
        publish()


def _publish_or_defer(publish: Callable[[], None]) -> None:
    pending = _PENDING.get()
    if pending is None:
        publish()  # no transaction is being tracked: the caller has committed
    else:
        pending.append(publish)


# --- Recording helpers -------------------------------------------------------


def record_transition(
    *,
    mode: str,
    from_state: str | None,
    to_state: str,
    created_at: Any | None = None,
    now: float | None = None,
) -> None:
    """Count one Coordinator state change; on a terminal state, record the outcome.

    ``created_at`` is the redemption's creation time (a datetime or epoch
    seconds), used for the end-to-end duration. Inside ``metrics_on_commit``
    nothing is published until the enclosing transaction has committed.
    """
    elapsed: float | None = None
    if to_state in TERMINAL_STATES and created_at is not None:
        started = created_at.timestamp() if hasattr(created_at, "timestamp") else created_at
        elapsed = max((time.time() if now is None else now) - float(started), 0.0)

    def publish() -> None:
        TRANSITIONS.labels(mode=mode, state=to_state).inc()
        if to_state == "COMPENSATING":
            attempt = "retry" if from_state == "COMPENSATING" else "first"
            COMPENSATIONS.labels(mode=mode, attempt=attempt).inc()
        if to_state in TERMINAL_STATES:
            REDEMPTIONS.labels(mode=mode, outcome=to_state).inc()
            if elapsed is not None:
                REDEMPTION_SECONDS.labels(mode=mode, outcome=to_state).observe(elapsed)

    _publish_or_defer(publish)


def record_prepared_hold(
    *, service: str, decision: str, prepared_at: Any, now: float | None = None
) -> None:
    """Record how long a prepared transaction blocked before ``decision`` arrived."""
    started = prepared_at.timestamp() if hasattr(prepared_at, "timestamp") else prepared_at
    elapsed = (time.time() if now is None else now) - float(started)
    PREPARED_HOLD_SECONDS.labels(service=service, decision=decision).observe(max(elapsed, 0.0))


def record_invariant_violations(counts: Mapping[str, int]) -> None:
    """Publish the latest invariant check, one gauge per invariant (0 when clean)."""
    for invariant, count in counts.items():
        INVARIANT_VIOLATIONS.labels(invariant=invariant).set(count)


# --- Exposition --------------------------------------------------------------


def render_metrics() -> tuple[bytes, str]:
    """The Prometheus text exposition and its content type, for a ``/metrics`` route."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


def start_metrics_server(default_port: int) -> int | None:
    """Serve ``/metrics`` from a worker process on ``LEDGERMESH_METRICS_PORT``.

    ``0`` disables it. Returns the port, or ``None`` if disabled.
    """
    port = int(os.environ.get(METRICS_PORT_ENV, default_port))
    if port == 0:
        return None
    start_http_server(port)
    logger.info("serving Prometheus metrics on :%d", port)
    return port


class MetricsMiddleware:
    """ASGI middleware recording ``ledgermesh_http_request_duration_seconds``.

    The ``route`` label is the matched route template when the framework
    provides one (``/v1/transactions/{transaction_id}/prepare``); otherwise ids
    in the path are replaced by ``{id}``, so the label stays low-cardinality.
    """

    _ID = re.compile(r"/(?:[0-9a-fA-F-]{36}|\d+)(?=/|$)")

    def __init__(self, app: Any, *, service: str) -> None:
        self.app = app
        self.service = service

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or scope.get("path") == "/metrics":
            await self.app(scope, receive, send)
            return
        status = {"code": 500}

        async def capture(message: dict) -> None:
            if message.get("type") == "http.response.start":
                status["code"] = message.get("status", 500)
            await send(message)

        started = time.perf_counter()
        try:
            await self.app(scope, receive, capture)
        finally:
            HTTP_SECONDS.labels(
                service=self.service,
                method=scope.get("method", "GET"),
                route=self._route(scope),
                status=str(status["code"]),
            ).observe(time.perf_counter() - started)

    def _route(self, scope: dict) -> str:
        route = scope.get("route")
        if template := getattr(route, "path", None):
            return template
        return self._ID.sub("/{id}", scope.get("path", ""))
