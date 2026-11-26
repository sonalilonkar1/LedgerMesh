"""Structured JSON logs with transaction correlation (LM-032).

Every service calls ``configure_logging(service)`` once at startup. From then
on each log record is one JSON object per line with the same field names in
every service, so one transaction can be followed across all of them by
filtering on ``transaction_id`` (or ``correlation_id``):

    {"timestamp": "...", "level": "INFO", "service": "wallet",
     "logger": "services.wallet.app.saga", "message": "...",
     "transaction_id": "...", "event_id": "...", "correlation_id": "...",
     "causation_id": "...", "event_type": "...", "workflow_step": 1,
     "state": "points_deducted", "fault_id": null}

The correlation fields come from a context variable rather than from each
call site. ``InboxConsumer`` and ``OutboxPublisher`` bind a message's ids
while handling it, and ``CorrelationMiddleware`` binds the transaction and
correlation ids of an HTTP request, so every log inside that scope carries
them, including logs from code that knows nothing about transactions.

Sensitive values never reach the output. Fields whose name looks like a secret
are replaced by ``[REDACTED]``, and credentials inside URLs are masked.

Failure injection (LM-035/036) marks the logs of an injected fault with its
identifier: ``LEDGERMESH_FAULT_ID`` tags every log of a process run under that
fault, and ``fault(...)`` / ``log_fault_injected(...)`` tag a fault injected at
one point in the code.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import traceback
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any, TextIO
from uuid import UUID

# The shared vocabulary. Every service uses exactly these names.
CORRELATION_FIELDS = (
    "transaction_id",
    "event_id",
    "correlation_id",
    "causation_id",
    "event_type",
    "workflow_step",
    "state",
    "fault_id",
)
BASE_FIELDS = ("timestamp", "level", "service", "logger", "message")

FAULT_ENV = "LEDGERMESH_FAULT_ID"
LEVEL_ENV = "LEDGERMESH_LOG_LEVEL"
REDACTED = "[REDACTED]"

# Any extra field whose name contains one of these is never written out.
_SENSITIVE_KEY = re.compile(
    r"pass(word)?|secret|token|authorization|api[_-]?key|credential|cookie|"
    r"database_url|dsn|private[_-]?key",
    re.IGNORECASE,
)
# scheme://user:password@host -> scheme://user:***@host
_URL_CREDENTIALS = re.compile(r"(\b[a-z][a-z0-9+.-]*://[^\s:/@]+:)[^\s@/]+(@)", re.IGNORECASE)

# Attributes every LogRecord has; anything else on a record came from ``extra=``.
_RECORD_ATTRIBUTES = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
    "taskName",
}

_context: ContextVar[Mapping[str, Any]] = ContextVar("ledgermesh_log_context", default={})


def _clean(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    return value


def current_context() -> dict[str, Any]:
    """The correlation fields bound in this task or thread."""
    return dict(_context.get())


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Bind fields to every log emitted inside the ``with`` block.

    Nested blocks add to, and may override, the outer fields; ``None`` values
    are ignored so a partial binding never erases a known id.
    """
    merged = {**_context.get(), **{k: _clean(v) for k, v in fields.items() if v is not None}}
    token = _context.set(merged)
    try:
        yield
    finally:
        _context.reset(token)


def envelope_fields(envelope: Any) -> dict[str, Any]:
    """The correlation fields of an ``EventEnvelope`` (or its JSON dict)."""
    get = (
        envelope.get
        if isinstance(envelope, Mapping)
        else lambda name: getattr(envelope, name, None)
    )
    return {
        "transaction_id": _clean(get("transaction_id")),
        "event_id": _clean(get("event_id")),
        "correlation_id": _clean(get("correlation_id")),
        "causation_id": _clean(get("causation_id")),
        "event_type": get("event_type"),
        "workflow_step": get("workflow_step"),
    }


def mask_credentials(text: str) -> str:
    return _URL_CREDENTIALS.sub(r"\1***\2", text)


# Names that match the pattern but are not secrets (ADR-001 fencing tokens).
_NOT_SENSITIVE = frozenset({"fencing_token"})


def _redact(key: str, value: Any) -> Any:
    if key not in _NOT_SENSITIVE and _SENSITIVE_KEY.search(key):
        return REDACTED
    if isinstance(value, str):
        return mask_credentials(value)
    if isinstance(value, Mapping):
        return {str(k): _redact(str(k), v) for k, v in value.items()}
    return _clean(value)


class JsonFormatter(logging.Formatter):
    """One JSON object per record, with the shared field names."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        context = current_context()
        entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "service": context.pop("service", self.service),
            "logger": record.name,
            "message": mask_credentials(record.getMessage()),
        }
        for name in CORRELATION_FIELDS:
            entry[name] = None
        for name, value in context.items():
            entry[name] = _redact(name, value)
        # Fields passed with ``extra=`` win over the bound context, except that
        # an explicit ``None`` never erases an id the context already knows.
        for name, value in record.__dict__.items():
            if name in _RECORD_ATTRIBUTES or name.startswith("_"):
                continue
            if value is None and entry.get(name) is not None:
                continue
            entry[name] = _redact(name, value)
        if record.exc_info:
            entry["exception"] = mask_credentials(
                "".join(traceback.format_exception(*record.exc_info)).rstrip()
            )
        return json.dumps(entry, default=str)


def configure_logging(
    service: str, *, level: str | int | None = None, stream: TextIO | None = None
) -> None:
    """Send every log of this process to one JSON handler, tagged with ``service``.

    Safe to call more than once. Uvicorn's own loggers are routed through the
    same handler, so access and error logs use the same format. If
    ``LEDGERMESH_FAULT_ID`` is set, every log carries it as ``fault_id``.
    """
    level = level or os.environ.get(LEVEL_ENV, "INFO")
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter(service))

    root = logging.getLogger()
    for existing in list(root.handlers):
        if getattr(existing, "_ledgermesh", False):
            root.removeHandler(existing)
    handler._ledgermesh = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(level)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        named = logging.getLogger(name)
        named.handlers.clear()
        named.propagate = True

    if fault_id := os.environ.get(FAULT_ENV):
        _context.set({**_context.get(), "fault_id": fault_id})


@contextmanager
def fault(fault_id: str, **fields: Any) -> Iterator[None]:
    """Tag every log inside the block with an injected fault's identifier."""
    with log_context(fault_id=fault_id, **fields):
        yield


def log_fault_injected(logger: logging.Logger, fault_id: str, point: str, **fields: Any) -> None:
    """Record that a fault was injected, so its effects can be found by ``fault_id``."""
    logger.warning("fault injected at %s", point, extra={"fault_id": fault_id, **fields})


class CorrelationMiddleware:
    """ASGI middleware that binds a request's transaction and correlation ids.

    ``transaction_id`` comes from ``/transactions/{id}`` or ``/redemptions/{id}``
    in the path, ``correlation_id`` from the ``X-Correlation-Id`` header (the
    Coordinator sends it on every 2PC call). Plain ASGI, so it adds no
    dependency to the library.
    """

    _PATH_ID = re.compile(r"/(?:transactions|redemptions)/([0-9a-fA-F-]{36})(?:/|$)")

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        fields: dict[str, Any] = {}
        if match := self._PATH_ID.search(scope.get("path", "")):
            fields["transaction_id"] = match.group(1).lower()
        for name, value in scope.get("headers") or ():
            if name.lower() == b"x-correlation-id":
                fields["correlation_id"] = value.decode("latin-1")
            elif name.lower() == b"x-transaction-id" and "transaction_id" not in fields:
                fields["transaction_id"] = value.decode("latin-1")
        with log_context(**fields):
            await self.app(scope, receive, send)


CORRELATION_HEADER = "X-Correlation-Id"
TRANSACTION_HEADER = "X-Transaction-Id"
