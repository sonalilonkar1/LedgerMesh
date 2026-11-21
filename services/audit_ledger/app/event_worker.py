"""Persist Coordinator transition events from Kafka in the Audit Ledger."""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from ledgermesh_messaging import (
    REDEMPTION_EVENTS,
    InboxConsumer,
    PermanentError,
    create_consumer,
    create_producer,
    dead_letter_topic,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from services.audit_ledger.app.repository import PostgresAuditRepository, database_url, get_engine

logger = logging.getLogger(__name__)
DEAD_LETTER_TOPIC = dead_letter_topic(REDEMPTION_EVENTS).name


class AuditEventHandler:
    def __init__(self, repository: PostgresAuditRepository) -> None:
        self.repository = repository

    async def __call__(self, session: AsyncSession, envelope):
        if envelope.event_type != "coordinator.transitioned":
            raise PermanentError(f"unsupported audit event {envelope.event_type}")
        await session.run_sync(
            lambda sync: self.repository.append(envelope, connection=sync.connection())
        )
        return []


async def publish_dead_letter(producer, record, error: Exception) -> None:
    logger.error("dead-lettering audit event at offset %s: %r", record.offset, error)
    await producer.send_and_wait(
        DEAD_LETTER_TOPIC,
        value=record.value,
        key=record.key,
        headers=[("error", repr(error)[:1000].encode())],
    )


async def run(stop: asyncio.Event) -> None:
    engine = create_async_engine(database_url(), pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    consumer = create_consumer(
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        REDEMPTION_EVENTS.consumer_group,
        REDEMPTION_EVENTS.name,
    )
    producer = create_producer(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    attempted = []
    try:
        for client in (producer, consumer):
            attempted.append(client)
            await client.start()

        async def dead_letter(record, error: Exception) -> None:
            await publish_dead_letter(producer, record, error)

        inbox = InboxConsumer(
            consumer,
            sessions,
            name=REDEMPTION_EVENTS.consumer_group,
            handler=AuditEventHandler(PostgresAuditRepository(get_engine())),
            on_dead_letter=dead_letter,
        )
        await inbox.run(stop)
    finally:
        for client in reversed(attempted):
            try:
                await client.stop()
            except Exception:
                logger.exception("failed to stop %s during shutdown", type(client).__name__)
        await engine.dispose()


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    async def serve() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        await run(stop)

    asyncio.run(serve())


if __name__ == "__main__":
    main()
