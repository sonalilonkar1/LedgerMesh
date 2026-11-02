"""Consuming Kafka messages through the inbox."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .envelope import EnvelopeError, EventEnvelope
from .inbox import Handler, ProcessResult, process_once

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

    Invalid envelopes, ``PermanentError`` and exhausted transient retries go to
    ``on_dead_letter``. The default re-raises, which stops the consumer rather
    than silently skipping a message; LM-024 supplies a dead-letter topic hook.
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
    ) -> None:
        self._consumer = consumer
        self._session_factory = session_factory
        self._name = name
        self._handler = handler
        self._on_dead_letter = on_dead_letter
        self._max_attempts = max_attempts
        self._retry_backoff = retry_backoff

    async def handle(self, record: ConsumerRecord) -> ProcessResult | None:
        """Process one record without committing its offset."""
        try:
            envelope = EventEnvelope.from_bytes(record.value)
        except EnvelopeError as exc:
            await self._on_dead_letter(record, exc)
            return None

        for attempt in range(1, self._max_attempts + 1):
            try:
                return await process_once(self._session_factory, self._name, envelope, self._handler)
            except PermanentError as exc:
                await self._on_dead_letter(record, exc)
                return None
            except (TransientError, OperationalError) as exc:
                if attempt == self._max_attempts:
                    await self._on_dead_letter(record, exc)
                    return None
                logger.warning(
                    "retrying event %s (attempt %d/%d): %r",
                    envelope.event_id,
                    attempt,
                    self._max_attempts,
                    exc,
                )
                await asyncio.sleep(self._retry_backoff * 2 ** (attempt - 1))
        return None

    async def commit(self, record: ConsumerRecord) -> None:
        await self._consumer.commit({TopicPartition(record.topic, record.partition): record.offset + 1})

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            batches: dict[Any, list[ConsumerRecord]] = await self._consumer.getmany(timeout_ms=200)
            for records in batches.values():
                for record in records:
                    await self.handle(record)
                    await self.commit(record)
