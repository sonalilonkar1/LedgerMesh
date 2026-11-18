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
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from ledgermesh_messaging import (
    WALLET_COMMANDS,
    InboxConsumer,
    OutboxPublisher,
    create_consumer,
    create_producer,
    dead_letter_topic,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.wallet.app.repository import PostgresWalletRepository, database_url, get_engine
from services.wallet.app.saga import WalletSagaHandler

logger = logging.getLogger(__name__)

CONSUMER_GROUP = WALLET_COMMANDS.consumer_group
DEAD_LETTER_TOPIC = dead_letter_topic(WALLET_COMMANDS).name


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


async def run(stop: asyncio.Event) -> None:
    engine = create_async_engine(database_url(), pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    producer = create_producer(bootstrap_servers())
    consumer = create_consumer(bootstrap_servers(), CONSUMER_GROUP, WALLET_COMMANDS.name)
    await producer.start()
    await consumer.start()

    async def dead_letter(record, error: Exception) -> None:
        # Retry topics and alerting arrive with LM-024; until then a command that
        # can never succeed is parked on the DLQ instead of blocking its partition.
        logger.error("dead-lettering Wallet command at offset %s: %r", record.offset, error)
        await producer.send_and_wait(
            DEAD_LETTER_TOPIC,
            value=record.value,
            key=record.key,
            headers=[("error", repr(error)[:1000].encode())],
        )

    handler = WalletSagaHandler(PostgresWalletRepository(get_engine()))
    inbox_consumer = InboxConsumer(
        consumer, sessions, name=CONSUMER_GROUP, handler=handler, on_dead_letter=dead_letter
    )
    publisher = OutboxPublisher(sessions, producer, name="wallet")
    try:
        await asyncio.gather(inbox_consumer.run(stop), publisher.run(stop))
    finally:
        await consumer.stop()
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
