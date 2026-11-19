"""Persist Coordinator transition events from Kafka in the Audit Ledger."""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from ledgermesh_messaging import REDEMPTION_EVENTS, InboxConsumer, PermanentError, create_consumer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from services.audit_ledger.app.repository import PostgresAuditRepository, database_url, get_engine

logger = logging.getLogger(__name__)


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


async def run(stop: asyncio.Event) -> None:
    engine = create_async_engine(database_url(), pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    consumer = create_consumer(
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        REDEMPTION_EVENTS.consumer_group,
        REDEMPTION_EVENTS.name,
    )
    try:
        await consumer.start()
        inbox = InboxConsumer(
            consumer,
            sessions,
            name=REDEMPTION_EVENTS.consumer_group,
            handler=AuditEventHandler(PostgresAuditRepository(get_engine())),
        )
        await inbox.run(stop)
    finally:
        try:
            await consumer.stop()
        finally:
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
