"""Run the Catalog Saga inbox consumer and outbox publisher (LM-021).

Failures (LM-024): transient errors are retried in process, then through
``catalog.commands.retry`` (read by a second consumer), then parked on
``catalog.commands.dlq`` with the reason. Limits come from ``LM_RETRY_*``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from ledgermesh_messaging import (
    CATALOG_COMMANDS,
    FailureRouter,
    InboxConsumer,
    OutboxPublisher,
    RetryPolicy,
    create_consumer,
    create_producer,
    create_retry_consumer,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.catalog.app.repository import PostgresCatalogRepository, database_url, get_engine
from services.catalog.app.saga import CatalogSagaHandler

logger = logging.getLogger(__name__)
CONSUMER_GROUP = CATALOG_COMMANDS.consumer_group


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


async def run(stop: asyncio.Event) -> None:
    policy = RetryPolicy.from_env()  # fails fast on an invalid LM_RETRY_* value
    engine = create_async_engine(database_url(), pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    producer = create_producer(bootstrap_servers())
    consumer = create_consumer(bootstrap_servers(), CONSUMER_GROUP, CATALOG_COMMANDS.name)
    retry_consumer = create_retry_consumer(bootstrap_servers(), CATALOG_COMMANDS)
    attempted = []
    try:
        for client in (producer, consumer, retry_consumer):
            # Track before awaiting start(), so partial startup is also cleaned up.
            attempted.append(client)
            await client.start()

        router = FailureRouter(producer, consumer=CONSUMER_GROUP, policy=policy)
        handler = CatalogSagaHandler(PostgresCatalogRepository(get_engine()))
        # Both consumers share the inbox name, so a retried command is still a duplicate.
        main_consumer, retries = (
            InboxConsumer(source, sessions, name=CONSUMER_GROUP, handler=handler, router=router)
            for source in (consumer, retry_consumer)
        )
        publisher = OutboxPublisher(sessions, producer, name="catalog")
        await asyncio.gather(main_consumer.run(stop), retries.run(stop), publisher.run(stop))
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
