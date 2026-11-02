"""Relaying outbox rows to Kafka."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
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


@dataclass(frozen=True)
class BatchResult:
    published: int
    failed: bool = False


class OutboxPublisher:
    """Publishes committed, unpublished outbox rows in ascending id order.

    Delivery is at-least-once: if the process dies after Kafka acknowledges a
    send but before the row is marked published, the row is sent again on the
    next batch with the same event id, and the receiving inbox discards it.

    Ordering: a PostgreSQL advisory lock allows only one active publisher per
    ``name``, and a send failure stops the batch at that row, so the publisher
    never reorders the rows it can see. Rows written in one transaction, or by
    transactions that commit one after another, therefore keep their order.
    It does not give causal order across concurrent transactions: ids are
    assigned at insert time, so a transaction holding a lower id can commit
    after a higher id has already been sent. Consumers that need strict
    per-aggregate or per-workflow order must check versions and causation
    (LM-026).

    ``run`` doubles its delay after each failed batch, up to ``max_backoff``,
    and returns to ``poll_interval`` after the next successful batch.
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

    async def publish_batch(self) -> BatchResult:
        """Publish up to ``batch_size`` rows.

        A Kafka error stops the batch and is reported as ``failed`` so the
        caller can back off; rows sent before the error stay marked published.
        """
        async with self._session_factory() as session, session.begin():
            acquired = await session.scalar(select(func.pg_try_advisory_xact_lock(self._lock_key)))
            if not acquired:
                return BatchResult(published=0)

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
                    return BatchResult(published=published, failed=True)

                await session.execute(
                    update(outbox)
                    .where(outbox.c.id == row.id)
                    .values(published_at=func.now(), attempts=outbox.c.attempts + 1)
                )
                published += 1
            return BatchResult(published=published)

    async def run(self, stop: asyncio.Event) -> None:
        backoff = self._poll_interval
        while not stop.is_set():
            try:
                result = await self.publish_batch()
            except Exception:
                logger.exception("outbox publisher batch failed")
                result = BatchResult(published=0, failed=True)

            if result.failed:
                backoff = min(backoff * 2, self._max_backoff)
                await self._pause(stop, backoff)
            else:
                backoff = self._poll_interval
                if result.published < self._batch_size:
                    await self._pause(stop, self._poll_interval)

    async def _pause(self, stop: asyncio.Event, delay: float) -> None:
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            pass
