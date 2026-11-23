"""Run the Coordinator Saga starter, result consumer, and outbox publisher."""

from __future__ import annotations

import asyncio
import logging
import os
import signal

import httpx
from ledgermesh_messaging import (
    CATALOG_RESULTS,
    WALLET_RESULTS,
    InboxConsumer,
    OutboxPublisher,
    create_consumer,
    create_producer,
    dead_letter_topic,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.coordinator.app.repository import database_url
from services.coordinator.app.saga import (
    CoordinatorSagaMachine,
    CoordinatorSagaResultHandler,
    HttpCatalogQuoteClient,
    SagaStarter,
)

logger = logging.getLogger(__name__)
CONSUMER_GROUP = "coordinator-saga"
RESULT_DLQ_TOPICS = {
    WALLET_RESULTS.name: dead_letter_topic(WALLET_RESULTS).name,
    CATALOG_RESULTS.name: dead_letter_topic(CATALOG_RESULTS).name,
}


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


async def publish_dead_letter(producer, record, error: Exception) -> None:
    """Publish an invalid participant result to the matching result DLQ."""
    topic = RESULT_DLQ_TOPICS.get(record.topic)
    if topic is None:
        raise ValueError(f"no Coordinator result DLQ is configured for {record.topic!r}")
    logger.error(
        "dead-lettering Coordinator result from %s at offset %s: %r",
        record.topic,
        record.offset,
        error,
    )
    await producer.send_and_wait(
        topic,
        value=record.value,
        key=record.key,
        headers=[("error", repr(error)[:1000].encode())],
    )


async def run(stop: asyncio.Event) -> None:
    engine = create_async_engine(database_url(), pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    producer = create_producer(bootstrap_servers())
    consumer = create_consumer(
        bootstrap_servers(), CONSUMER_GROUP, WALLET_RESULTS.name, CATALOG_RESULTS.name
    )
    attempted = []
    try:
        for client in (producer, consumer):
            attempted.append(client)
            await client.start()

        machine = CoordinatorSagaMachine()
        handler = CoordinatorSagaResultHandler(machine)

        async def dead_letter(record, error: Exception) -> None:
            await publish_dead_letter(producer, record, error)

        inbox = InboxConsumer(
            consumer,
            sessions,
            name=CONSUMER_GROUP,
            handler=handler,
            on_dead_letter=dead_letter,
        )
        publisher = OutboxPublisher(sessions, producer, name="coordinator")
        async with httpx.AsyncClient(timeout=5.0) as client:
            quote_client = HttpCatalogQuoteClient(
                os.environ.get("CATALOG_URL", "http://localhost:8002"), client
            )
            starter = SagaStarter(sessions, quote_client, machine)
            await asyncio.gather(inbox.run(stop), publisher.run(stop), starter.run(stop))
    finally:
        for client in reversed(attempted):
            try:
                await client.stop()
            except Exception:
                logger.exception("failed to stop %s", type(client).__name__)
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
