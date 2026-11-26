"""Persist Coordinator events from Kafka in the Audit Ledger.

Failures are retried in process, then through the retry topic, then sent to the
dead-letter topic. Retry limits come from ``LM_RETRY_*``.
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
    create_consumer,
    create_producer,
    create_retry_consumer,
    in_version_order,
    start_metrics_server,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from services.audit_ledger.app.repository import PostgresAuditRepository, database_url, get_engine

logger = logging.getLogger(__name__)
CONSUMER_GROUP = REDEMPTION_EVENTS.consumer_group
SUPPORTED_EVENT_TYPES = {
    "coordinator.transitioned",
    "redemption.committed",
    "redemption.aborted",
}


class AuditEventHandler:
    def __init__(self, repository: PostgresAuditRepository) -> None:
        self.repository = repository

    async def __call__(self, session: AsyncSession, envelope):
        if envelope.event_type not in SUPPORTED_EVENT_TYPES:
            raise PermanentError(f"unsupported audit event {envelope.event_type}")
        await session.run_sync(
            lambda sync: self.repository.append(envelope, connection=sync.connection())
        )
        return []


def ordered_audit_handler(repository: PostgresAuditRepository):
    """Sequence Saga transition events while accepting standalone 2PC outcomes."""
    append = AuditEventHandler(repository)
    ordered_saga = in_version_order(
        append,
        consumer=CONSUMER_GROUP,
        first_version=0,
        root_step=0,
    )

    async def handle(session, envelope):
        # Saga emits every transition, so version gaps must wait for missing
        # events. 2PC currently emits only its terminal outcome; ordering it
        # against absent intermediate events would strand it on the retry topic.
        if envelope.payload.get("coordination_mode") == "two_phase_commit":
            return await append(session, envelope)
        return await ordered_saga(session, envelope)

    return handle


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


async def run(stop: asyncio.Event) -> None:
    policy = RetryPolicy.from_env()
    engine = create_async_engine(database_url(), pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    consumer = create_consumer(bootstrap_servers(), CONSUMER_GROUP, REDEMPTION_EVENTS.name)
    retry_consumer = create_retry_consumer(bootstrap_servers(), REDEMPTION_EVENTS)
    producer = create_producer(bootstrap_servers())
    attempted = []
    metrics = start_metrics_server("audit-ledger", 9104)
    try:
        for client in (producer, consumer, retry_consumer):
            attempted.append(client)
            await client.start()

        router = FailureRouter(producer, consumer=CONSUMER_GROUP, policy=policy)
        handler = ordered_audit_handler(PostgresAuditRepository(get_engine()))
        # Both consumers share the inbox name; a retried message is a duplicate.
        main_inbox, retry_inbox = (
            InboxConsumer(source, sessions, name=CONSUMER_GROUP, handler=handler, router=router)
            for source in (consumer, retry_consumer)
        )
        await asyncio.gather(main_inbox.run(stop), retry_inbox.run(stop))
    finally:
        for client in reversed(attempted):
            try:
                await client.stop()
            except Exception:
                logger.exception("failed to stop %s during shutdown", type(client).__name__)
        metrics.close()
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
