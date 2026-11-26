"""Consuming Kafka messages through the inbox."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .envelope import EnvelopeError, EventEnvelope
from .inbox import Handler, ProcessResult, process_once
from .logs import envelope_fields, log_context
from .retry import DeadLetterReason, FailureRouter, not_before

logger = logging.getLogger(__name__)


class PermanentError(Exception):
    """Raised by a handler when retrying the message can never succeed."""


class TransientError(Exception):
    """Raised by a handler when the message may succeed if retried."""


DeadLetterHook = Callable[[ConsumerRecord, Exception], Awaitable[None]]


async def _raise(record: ConsumerRecord, exc: Exception) -> None:
    raise exc


class InboxConsumer:
    """Processes records one at a time and commits offsets after the database.

    Offsets are committed only after ``process_once`` has committed, so a crash
    in between causes redelivery, which the inbox turns into a duplicate.

    Without a ``router``, invalid envelopes, ``PermanentError`` and exhausted
    transient retries go to ``on_dead_letter``. The default re-raises, which
    stops the consumer rather than silently skipping a message.

    With a ``router`` (LM-024), its ``RetryPolicy`` sets the in-process attempts,
    their backoff and a per-attempt handler timeout. A handler that times out is
    treated as transient. Invalid envelopes and ``PermanentError`` are
    dead-lettered with their reason; exhausted transient failures go to the
    retry topic, and to the DLQ once its redeliveries run out. A record read
    from a retry topic is processed only after its not-before time.
    """

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        name: str,
        handler: Handler,
        on_dead_letter: DeadLetterHook = _raise,
        max_attempts: int = 5,
        retry_backoff: float = 0.2,
        router: FailureRouter | None = None,
    ) -> None:
        self._consumer = consumer
        self._session_factory = session_factory
        self._name = name
        self._handler = handler
        self._on_dead_letter = on_dead_letter
        self._router = router
        if router is None:
            self._max_attempts = max_attempts
            self._timeout: float | None = None
            self._backoff: Callable[[int], float] = lambda attempt: (
                retry_backoff * 2 ** (attempt - 1)
            )
        else:
            self._max_attempts = router.policy.attempts
            self._timeout = router.policy.handler_timeout_seconds
            self._backoff = router.policy.backoff

    async def _process(self, envelope: EventEnvelope) -> ProcessResult:
        work = process_once(self._session_factory, self._name, envelope, self._handler)
        if self._timeout is None:
            return await work
        # A timeout cancels the attempt, which rolls its transaction back.
        return await asyncio.wait_for(work, timeout=self._timeout)

    async def _reject(
        self, record: ConsumerRecord, exc: Exception, reason: DeadLetterReason
    ) -> None:
        if self._router is None:
            await self._on_dead_letter(record, exc)
        else:
            await self._router.dead_letter(record, exc, reason)

    async def _give_up(self, record: ConsumerRecord, exc: Exception) -> None:
        if self._router is None:
            await self._on_dead_letter(record, exc)
        else:
            await self._router.retry_or_dead_letter(record, exc)

    async def handle(self, record: ConsumerRecord) -> ProcessResult | None:
        """Process one record without committing its offset."""
        try:
            envelope = EventEnvelope.from_bytes(record.value)
        except EnvelopeError as exc:
            await self._reject(record, exc, DeadLetterReason.INVALID_ENVELOPE)
            return None

        # Every log while handling this message (handler, retries, dead
        # letters) carries its transaction, event and correlation ids (LM-032).
        with log_context(**envelope_fields(envelope)):
            return await self._handle_envelope(record, envelope)

    async def _handle_envelope(
        self, record: ConsumerRecord, envelope: EventEnvelope
    ) -> ProcessResult | None:
        for attempt in range(1, self._max_attempts + 1):
            try:
                return await self._process(envelope)
            except PermanentError as exc:
                await self._reject(record, exc, DeadLetterReason.PERMANENT_ERROR)
                return None
            except (TransientError, OperationalError, TimeoutError) as exc:
                if attempt == self._max_attempts:
                    await self._give_up(record, exc)
                    return None
                logger.warning(
                    "retrying event %s (attempt %d/%d): %r",
                    envelope.event_id,
                    attempt,
                    self._max_attempts,
                    exc,
                )
                await asyncio.sleep(self._backoff(attempt))
        return None

    async def commit(self, record: ConsumerRecord) -> None:
        await self._consumer.commit(
            {TopicPartition(record.topic, record.partition): record.offset + 1}
        )

    async def _wait_until_due(self, record: ConsumerRecord, stop: asyncio.Event) -> bool:
        """Wait for a retried record's not-before time; False if stopping first."""
        due = not_before(record)
        delay = 0.0 if due is None else due - time.time()
        if delay > 0:
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)
        return not stop.is_set()

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            batches: dict[Any, list[ConsumerRecord]] = await self._consumer.getmany(timeout_ms=200)
            for records in batches.values():
                for record in records:
                    # Stopping mid-wait leaves the offset uncommitted, so the
                    # record is read again after a restart.
                    if not await self._wait_until_due(record, stop):
                        return
                    await self.handle(record)
                    await self.commit(record)
