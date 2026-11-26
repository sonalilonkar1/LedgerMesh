"""Persist Coordinator transition events from Kafka in the Audit Ledger.

Failures (LM-024): transient errors are retried in process, then through
``redemption.events.retry`` (read by a second consumer), then parked on
``redemption.events.dlq`` with the reason. Limits come from ``LM_RETRY_*``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from ledgermesh_messaging import (
    REDEMPTION_EVENTS,
    FailureRouter,
    InboxConsumer,
    PermanentError,
    RetryPolicy,
    configure_logging,
    create_consumer,
    create_producer,
    create_retry_consumer,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from services.audit_ledger.app.repository import PostgresAuditRepository, database_url, get_engine

logger = logging.getLogger(__name__)
CONSUMER_GROUP = REDEMPTION_EVENTS.consumer_group


class AuditEventHandler:
    def __init__(self, repository: PostgresAuditRepository) -> None:
        self.repository = repository

    async def __call__(self, session: AsyncSession, envelope):
        if envelope.event_type != "coordinator.transitioned":
            raise PermanentError(f"unsupported audit event {envelope.event_type}")
        await session.run_sync(
            lambda sync: self.repository.append(envelope, connection=sync.connection())
        )
        # The event's ids are already bound by InboxConsumer (LM-032).
        logger.info(
            "audit event recorded",
            extra={"state": envelope.payload.get("to_status")},
        )
        return []


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


async def run(stop: asyncio.Event) -> None:
    policy = RetryPolicy.from_env()  # fails fast on an invalid LM_RETRY_* value
    engine = create_async_engine(database_url(), pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    consumer = create_consumer(bootstrap_servers(), CONSUMER_GROUP, REDEMPTION_EVENTS.name)
    retry_consumer = create_retry_consumer(bootstrap_servers(), REDEMPTION_EVENTS)
    producer = create_producer(bootstrap_servers())
    attempted = []
    try:
        for client in (producer, consumer, retry_consumer):
            attempted.append(client)
            await client.start()

        router = FailureRouter(producer, consumer=CONSUMER_GROUP, policy=policy)
        handler = AuditEventHandler(PostgresAuditRepository(get_engine()))
        # Both consumers share the inbox name, so a retried event is still a duplicate.
        inbox, retries = (
            InboxConsumer(source, sessions, name=CONSUMER_GROUP, handler=handler, router=router)
            for source in (consumer, retry_consumer)
        )
        await asyncio.gather(inbox.run(stop), retries.run(stop))
    finally:
        for client in reversed(attempted):
            try:
                await client.stop()
            except Exception:
                logger.exception("failed to stop %s during shutdown", type(client).__name__)
        await engine.dispose()


def main() -> None:
    configure_logging("audit-ledger")

    async def serve() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        await run(stop)

    asyncio.run(serve())


if __name__ == "__main__":
    main()
