"""Relaying outbox rows to Kafka."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any, Protocol

from aiokafka.errors import KafkaError
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .schema import outbox

logger = logging.getLogger(__name__)


class Producer(Protocol):
    async def send_and_wait(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> Any: ...


def _advisory_lock_key(name: str) -> int:
    return int.from_bytes(hashlib.blake2b(name.encode(), digest_size=8).digest(), "big", signed=True)


class OutboxPublisher:
    """Publishes unpublished outbox rows in insertion order.

    Delivery is at-least-once: if the process dies after Kafka acknowledges a
    send but before the row is marked published, the row is sent again on the
    next batch with the same event id, and the receiving inbox discards it.

    A PostgreSQL advisory lock allows only one active publisher per ``name``,
    so running several replicas of a service cannot reorder its messages.
    A send failure stops the batch at that row for the same reason.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        producer: Producer,
        *,
        name: str,
        batch_size: int = 100,
        poll_interval: float = 0.2,
        max_backoff: float = 5.0,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._lock_key = _advisory_lock_key(name)
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._max_backoff = max_backoff

    async def publish_batch(self) -> int:
        """Publish up to ``batch_size`` rows and return how many succeeded."""
        async with self._session_factory() as session, session.begin():
            acquired = await session.scalar(select(func.pg_try_advisory_xact_lock(self._lock_key)))
            if not acquired:
                return 0

            rows = (
                await session.execute(
                    select(outbox)
                    .where(outbox.c.published_at.is_(None))
                    .order_by(outbox.c.id)
                    .limit(self._batch_size)
                )
            ).all()

            published = 0
            for row in rows:
                try:
                    await self._producer.send_and_wait(
                        row.topic,
                        value=json.dumps(row.envelope).encode(),
                        key=row.message_key.encode(),
                        headers=[(name, value.encode()) for name, value in row.headers.items()],
                    )
                except KafkaError as exc:
                    await session.execute(
                        update(outbox)
                        .where(outbox.c.id == row.id)
                        .values(attempts=outbox.c.attempts + 1, last_error=repr(exc)[:2000])
                    )
                    logger.warning("outbox publish failed for event %s: %r", row.event_id, exc)
                    break

                await session.execute(
                    update(outbox)
                    .where(outbox.c.id == row.id)
                    .values(published_at=func.now(), attempts=outbox.c.attempts + 1)
                )
                published += 1
            return published

    async def run(self, stop: asyncio.Event) -> None:
        backoff = self._poll_interval
        while not stop.is_set():
            try:
                published = await self.publish_batch()
                backoff = self._poll_interval
            except Exception:
                logger.exception("outbox publisher batch failed")
                published = 0
                backoff = min(backoff * 2, self._max_backoff)

            if published < self._batch_size:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=backoff)
                except TimeoutError:
                    pass
