"""Retry topics, handler timeouts and dead-letter routing (LM-024).

A message that fails is handled in three tiers:

1. **In-process attempts.** ``InboxConsumer`` retries a transient failure
   (``TransientError``, a database ``OperationalError``, or a handler that
   exceeds ``handler_timeout_seconds``) ``attempts`` times with exponential
   backoff. This absorbs short blips without leaving the partition.
2. **Retry topic.** If those attempts run out, ``FailureRouter`` republishes the
   message's **original bytes and key** to ``<topic>.retry`` with a retry count
   and a not-before time. The value is not touched, so the ``event_id`` and
   ``transaction_id`` are preserved and the inbox still recognises a duplicate.
   The main partition moves on instead of blocking behind one message. A
   separate consumer group reads the retry topic and waits until each message
   is due.
3. **Dead-letter topic.** After ``redeliveries`` trips through the retry topic,
   or at once for an invalid envelope or a ``PermanentError``, the message goes
   to ``<topic>.dlq`` with headers recording why. ``ledgermesh-dlq`` lists them.

All limits are configurable, in code or through ``LM_RETRY_*`` variables
(``RetryPolicy.from_env``).
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

from aiokafka import AIOKafkaConsumer

from .envelope import EnvelopeError, EventEnvelope
from .kafka import create_consumer
from .topics import MAIN_TOPICS, TopicSpec, dead_letter_topic, retry_topic
from .tracing import trace_headers_from

logger = logging.getLogger(__name__)

# Header names. Every value is a UTF-8 string.
RETRY_COUNT = "lm-retry-count"
NOT_BEFORE_MS = "lm-not-before-ms"
LAST_ERROR = "lm-last-error"
ORIGINAL_TOPIC = "lm-original-topic"
ORIGINAL_PARTITION = "lm-original-partition"
ORIGINAL_OFFSET = "lm-original-offset"
DLQ_REASON = "lm-dlq-reason"
DLQ_ERROR = "lm-dlq-error"
DLQ_CONSUMER = "lm-dlq-consumer"
DLQ_FAILED_AT = "lm-dlq-failed-at"

ERROR_TEXT_LIMIT = 1000
ENV_PREFIX = "LM_RETRY_"


class DeadLetterReason(StrEnum):
    INVALID_ENVELOPE = "invalid_envelope"
    PERMANENT_ERROR = "permanent_error"
    RETRIES_EXHAUSTED = "retries_exhausted"


class RetryConfigError(ValueError):
    """A retry setting is missing a valid value."""


@dataclass(frozen=True)
class RetryPolicy:
    """How hard a consumer tries before a message is dead-lettered.

    ``max_delay_seconds`` caps every wait. Keep it well below the Kafka
    consumer's ``max.poll.interval.ms`` (300 s by default), because the retry
    consumer waits for a message's not-before time between polls.
    """

    attempts: int = 3
    backoff_seconds: float = 0.2
    redeliveries: int = 5
    redelivery_delay_seconds: float = 5.0
    max_delay_seconds: float = 60.0
    handler_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise RetryConfigError(f"attempts must be at least 1, got {self.attempts}")
        if self.redeliveries < 0:
            raise RetryConfigError(f"redeliveries must not be negative, got {self.redeliveries}")
        for name in ("backoff_seconds", "redelivery_delay_seconds", "max_delay_seconds"):
            if getattr(self, name) < 0:
                raise RetryConfigError(f"{name} must not be negative, got {getattr(self, name)}")
        if self.handler_timeout_seconds <= 0:
            raise RetryConfigError(
                f"handler_timeout_seconds must be positive, got {self.handler_timeout_seconds}"
            )

    @classmethod
    def from_env(
        cls, environ: Mapping[str, str] | None = None, prefix: str = ENV_PREFIX
    ) -> RetryPolicy:
        """Read overrides such as ``LM_RETRY_ATTEMPTS`` or ``LM_RETRY_REDELIVERIES``.

        Each field maps to ``<prefix><FIELD NAME IN UPPER CASE>``; unset or
        empty variables keep the default. An unparsable value raises
        ``RetryConfigError`` at startup rather than being ignored.
        """
        environ = os.environ if environ is None else environ
        values: dict[str, Any] = {}
        for field in fields(cls):
            variable = prefix + field.name.upper()
            raw = environ.get(variable, "").strip()
            if not raw:
                continue
            kind = int if field.type in (int, "int") else float
            try:
                values[field.name] = kind(raw)
            except ValueError as error:
                raise RetryConfigError(
                    f"{variable}={raw!r} is not a valid {kind.__name__}"
                ) from error
        return cls(**values)

    def backoff(self, attempt: int) -> float:
        """Wait before in-process attempt ``attempt + 1``."""
        return min(self.backoff_seconds * 2 ** (attempt - 1), self.max_delay_seconds)

    def redelivery_delay(self, redelivery: int) -> float:
        """Wait before retry-topic redelivery number ``redelivery`` (1-based)."""
        return min(self.redelivery_delay_seconds * 2 ** (redelivery - 1), self.max_delay_seconds)


class Producer(Protocol):
    async def send_and_wait(self, topic: str, **kwargs: Any) -> Any: ...


def headers_of(record: Any) -> dict[str, str]:
    """A Kafka record's headers as text; undecodable values are skipped."""
    decoded: dict[str, str] = {}
    for name, value in getattr(record, "headers", None) or ():
        try:
            decoded[name] = value.decode() if isinstance(value, bytes) else str(value)
        except UnicodeDecodeError:
            continue
    return decoded


def _encode(headers: Mapping[str, str]) -> list[tuple[str, bytes]]:
    return [(name, value.encode()) for name, value in headers.items()]


def _int(value: str | None, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


def retry_count(record: Any) -> int:
    """How many times ``record`` has already been through the retry topic."""
    return max(_int(headers_of(record).get(RETRY_COUNT)), 0)


def not_before(record: Any) -> float | None:
    """The epoch second before which a retried record must not be processed."""
    raw = headers_of(record).get(NOT_BEFORE_MS)
    return None if raw is None else _int(raw) / 1000


_SOURCE_BY_TOPIC: dict[str, TopicSpec] = {}
for _spec in MAIN_TOPICS:
    _SOURCE_BY_TOPIC[_spec.name] = _spec
    _SOURCE_BY_TOPIC[retry_topic(_spec).name] = _spec


def source_topic(record: Any) -> TopicSpec:
    """The main topic a record (from it, or from its retry topic) belongs to."""
    name = headers_of(record).get(ORIGINAL_TOPIC) or record.topic
    spec = _SOURCE_BY_TOPIC.get(name) or _SOURCE_BY_TOPIC.get(record.topic)
    if spec is None:
        raise ValueError(f"{record.topic!r} is not a LedgerMesh main or retry topic")
    return spec


def _describe_error(error: BaseException) -> str:
    return repr(error)[:ERROR_TEXT_LIMIT]


def _identity(record: Any) -> str:
    """event and transaction ids for logs, when the value is still a valid envelope."""
    try:
        envelope = EventEnvelope.from_bytes(record.value)
    except EnvelopeError:
        return "event_id=? transaction_id=?"
    return f"event_id={envelope.event_id} transaction_id={envelope.transaction_id}"


class FailureRouter:
    """Sends failed records to the retry topic or the dead-letter topic.

    Shared by a worker's main and retry consumers, so a record keeps its retry
    count and original position however many times it is redelivered.
    """

    def __init__(
        self,
        producer: Producer,
        *,
        consumer: str,
        policy: RetryPolicy,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.producer = producer
        self.consumer = consumer
        self.policy = policy
        self._clock = clock

    def _origin(self, record: Any) -> dict[str, str]:
        existing = headers_of(record)
        source = source_topic(record)
        if ORIGINAL_TOPIC in existing:
            return {
                ORIGINAL_TOPIC: existing[ORIGINAL_TOPIC],
                ORIGINAL_PARTITION: existing.get(ORIGINAL_PARTITION, ""),
                ORIGINAL_OFFSET: existing.get(ORIGINAL_OFFSET, ""),
            }
        return {
            ORIGINAL_TOPIC: source.name,
            ORIGINAL_PARTITION: str(record.partition),
            ORIGINAL_OFFSET: str(record.offset),
        }

    async def retry_or_dead_letter(self, record: Any, error: BaseException) -> None:
        """Republish to the retry topic, or dead-letter once redeliveries run out."""
        count = retry_count(record)
        if count >= self.policy.redeliveries:
            await self.dead_letter(record, error, DeadLetterReason.RETRIES_EXHAUSTED)
            return
        redelivery = count + 1
        delay = self.policy.redelivery_delay(redelivery)
        source = source_topic(record)
        headers = {
            **trace_headers_from(headers_of(record)),
            **self._origin(record),
            RETRY_COUNT: str(redelivery),
            NOT_BEFORE_MS: str(int((self._clock() + delay) * 1000)),
            LAST_ERROR: _describe_error(error),
        }
        target = retry_topic(source).name
        logger.warning(
            "retrying via %s in %.1fs (redelivery %d/%d) %s: %r",
            target,
            delay,
            redelivery,
            self.policy.redeliveries,
            _identity(record),
            error,
        )
        await self.producer.send_and_wait(
            target, value=record.value, key=record.key, headers=_encode(headers)
        )

    async def dead_letter(
        self, record: Any, error: BaseException, reason: DeadLetterReason
    ) -> None:
        """Park ``record`` on its source topic's DLQ with the reason in its headers."""
        source = source_topic(record)
        headers = {
            **trace_headers_from(headers_of(record)),
            **self._origin(record),
            RETRY_COUNT: str(retry_count(record)),
            DLQ_REASON: str(reason),
            DLQ_ERROR: _describe_error(error),
            DLQ_CONSUMER: self.consumer,
            DLQ_FAILED_AT: datetime.fromtimestamp(self._clock(), UTC).isoformat(),
        }
        if last := headers_of(record).get(LAST_ERROR):
            headers[LAST_ERROR] = last
        target = dead_letter_topic(source).name
        logger.error(
            "dead-lettering to %s reason=%s consumer=%s retries=%s %s: %r",
            target,
            reason,
            self.consumer,
            headers[RETRY_COUNT],
            _identity(record),
            error,
        )
        await self.producer.send_and_wait(
            target, value=record.value, key=record.key, headers=_encode(headers)
        )


def create_retry_consumer(
    bootstrap_servers: str, *specs: TopicSpec, **overrides: Any
) -> AIOKafkaConsumer:
    """A consumer for the retry topics of ``specs``, in their ``.retry`` group."""
    if not specs:
        raise ValueError("at least one topic is required")
    retry_specs: Sequence[TopicSpec] = [retry_topic(spec) for spec in specs]
    groups = {spec.consumer_group for spec in retry_specs}
    if len(groups) != 1:
        raise ValueError(f"topics belong to different consumer groups: {sorted(groups)}")
    return create_consumer(
        bootstrap_servers, groups.pop(), *(spec.name for spec in retry_specs), **overrides
    )
