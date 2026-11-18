"""Run the Catalog Saga inbox consumer and outbox publisher (LM-021)."""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from ledgermesh_messaging import (
    CATALOG_COMMANDS,
    InboxConsumer,
    OutboxPublisher,
    create_consumer,
    create_producer,
    dead_letter_topic,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.catalog.app.repository import PostgresCatalogRepository, database_url, get_engine
from services.catalog.app.saga import CatalogSagaHandler

logger = logging.getLogger(__name__)
CONSUMER_GROUP = CATALOG_COMMANDS.consumer_group
DEAD_LETTER_TOPIC = dead_letter_topic(CATALOG_COMMANDS).name


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


async def run(stop: asyncio.Event) -> None:
    engine = create_async_engine(database_url(), pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    producer = create_producer(bootstrap_servers())
    consumer = create_consumer(bootstrap_servers(), CONSUMER_GROUP, CATALOG_COMMANDS.name)
    producer_started = False
    consumer_started = False

    try:
        await producer.start()
        producer_started = True
        await consumer.start()
        consumer_started = True

        async def dead_letter(record, error: Exception) -> None:
            logger.error("dead-lettering Catalog command at offset %s: %r", record.offset, error)
            await producer.send_and_wait(
                DEAD_LETTER_TOPIC,
                value=record.value,
                key=record.key,
                headers=[("error", repr(error)[:1000].encode())],
            )

        handler = CatalogSagaHandler(PostgresCatalogRepository(get_engine()))
        inbox_consumer = InboxConsumer(
            consumer,
            sessions,
            name=CONSUMER_GROUP,
            handler=handler,
            on_dead_letter=dead_letter,
        )
        publisher = OutboxPublisher(sessions, producer, name="catalog")
        await asyncio.gather(inbox_consumer.run(stop), publisher.run(stop))
    finally:
        if consumer_started:
            await consumer.stop()
        if producer_started:
            await producer.stop()
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
