"""Run the Wallet Saga command consumer and its outbox publisher (LM-020).

From the repository root, with the local stack running and topics created
(``uv run ledgermesh-topics``):

    WALLET_DATABASE_URL=postgresql+psycopg://wallet:ledgermesh_dev@localhost:54322/wallet \\
      uv run python -m services.wallet.app.saga_worker

Restart safety: ``InboxConsumer`` commits a Kafka offset only after the command's
database transaction (inbox row, Wallet change and reply) has committed. A crash
in between redelivers the command, and the inbox reports it as a duplicate
instead of applying it again. Replies left in the outbox are published by
``OutboxPublisher`` after the restart.

Failures (LM-024): transient errors are retried in process, then through
``wallet.commands.retry`` (read by a second consumer), then parked on
``wallet.commands.dlq`` with the reason. Limits come from ``LM_RETRY_*``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from ledgermesh_messaging import (
    WALLET_COMMANDS,
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

from services.wallet.app.repository import PostgresWalletRepository, database_url, get_engine
from services.wallet.app.saga import WalletSagaHandler

logger = logging.getLogger(__name__)

CONSUMER_GROUP = WALLET_COMMANDS.consumer_group


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


async def run(stop: asyncio.Event) -> None:
    policy = RetryPolicy.from_env()  # fails fast on an invalid LM_RETRY_* value
    engine = create_async_engine(database_url(), pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    producer = create_producer(bootstrap_servers())
    consumer = create_consumer(bootstrap_servers(), CONSUMER_GROUP, WALLET_COMMANDS.name)
    retry_consumer = create_retry_consumer(bootstrap_servers(), WALLET_COMMANDS)
    # A client is tracked *before* its start() is awaited, so one whose start()
    # fails part-way is still stopped. Each stop is isolated, so a failure to stop
    # one client does not skip the others or the engine.
    attempted = []
    try:
        for client in (producer, consumer, retry_consumer):
            attempted.append(client)
            await client.start()
        await _serve(stop, sessions, producer, consumer, retry_consumer, policy)
    finally:
        for client in reversed(attempted):
            try:
                await client.stop()
            except Exception:
                logger.exception("failed to stop %s during shutdown", type(client).__name__)
        await engine.dispose()


async def _serve(
    stop: asyncio.Event, sessions, producer, consumer, retry_consumer, policy: RetryPolicy
) -> None:
    router = FailureRouter(producer, consumer=CONSUMER_GROUP, policy=policy)
    handler = WalletSagaHandler(PostgresWalletRepository(get_engine()))
    # Both consumers share the inbox name, so a retried command is still a duplicate.
    main_consumer, retries = (
        InboxConsumer(source, sessions, name=CONSUMER_GROUP, handler=handler, router=router)
        for source in (consumer, retry_consumer)
    )
    publisher = OutboxPublisher(sessions, producer, name="wallet")
    await asyncio.gather(main_consumer.run(stop), retries.run(stop), publisher.run(stop))


def main() -> None:
    configure_logging("wallet")
    start_metrics_server(9101)  # LM-034; LEDGERMESH_METRICS_PORT overrides, 0 disables

    async def serve() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        await run(stop)

    asyncio.run(serve())


if __name__ == "__main__":
    main()
