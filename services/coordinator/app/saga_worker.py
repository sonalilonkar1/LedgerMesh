"""Run the Coordinator Saga starter, result consumer, and outbox publisher.

Failures (LM-024): each participant result that keeps failing transiently is
retried in process, then through its ``.retry`` topic (read by a second
consumer), then parked on its own ``.dlq`` with the reason. An invalid result
goes straight to its DLQ. Limits come from ``LM_RETRY_*``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

import httpx
from ledgermesh_messaging import (
    CATALOG_RESULTS,
    WALLET_RESULTS,
    FailureRouter,
    InboxConsumer,
    OutboxPublisher,
    RetryPolicy,
    configure_logging,
    create_consumer,
    create_producer,
    create_retry_consumer,
    start_metrics_server,
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
RESULT_TOPICS = (WALLET_RESULTS, CATALOG_RESULTS)


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


async def run(stop: asyncio.Event) -> None:
    policy = RetryPolicy.from_env()  # fails fast on an invalid LM_RETRY_* value
    engine = create_async_engine(database_url(), pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    producer = create_producer(bootstrap_servers())
    consumer = create_consumer(
        bootstrap_servers(), CONSUMER_GROUP, *(topic.name for topic in RESULT_TOPICS)
    )
    retry_consumer = create_retry_consumer(bootstrap_servers(), *RESULT_TOPICS)
    attempted = []
    metrics = start_metrics_server("coordinator", 9103)
    try:
        for client in (producer, consumer, retry_consumer):
            attempted.append(client)
            await client.start()

        machine = CoordinatorSagaMachine()
        handler = CoordinatorSagaResultHandler(machine)
        router = FailureRouter(producer, consumer=CONSUMER_GROUP, policy=policy)
        # Both consumers share the inbox name, so a retried result is still a duplicate.
        inbox, retries = (
            InboxConsumer(source, sessions, name=CONSUMER_GROUP, handler=handler, router=router)
            for source in (consumer, retry_consumer)
        )
        publisher = OutboxPublisher(sessions, producer, name="coordinator")
        async with httpx.AsyncClient(timeout=5.0) as client:
            quote_client = HttpCatalogQuoteClient(
                os.environ.get("CATALOG_URL", "http://localhost:8002"), client
            )
            starter = SagaStarter(sessions, quote_client, machine)
            await asyncio.gather(
                inbox.run(stop), retries.run(stop), publisher.run(stop), starter.run(stop)
            )
    finally:
        for client in reversed(attempted):
            try:
                await client.stop()
            except Exception:
                logger.exception("failed to stop %s", type(client).__name__)
        metrics.close()
        await engine.dispose()


def main() -> None:
    configure_logging("coordinator")

    async def serve() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        await run(stop)

    asyncio.run(serve())


if __name__ == "__main__":
    main()
