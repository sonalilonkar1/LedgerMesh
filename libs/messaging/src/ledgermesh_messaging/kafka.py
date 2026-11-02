"""Kafka client settings shared by every LedgerMesh service."""

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


def create_producer(bootstrap_servers: str, **overrides) -> AIOKafkaProducer:
    # Idempotence plus acks=all stops broker-side duplicates and reordering
    # caused by the producer's own retries.
    return AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        enable_idempotence=True,
        acks="all",
        **overrides,
    )


def create_consumer(bootstrap_servers: str, group_id: str, *topics: str, **overrides) -> AIOKafkaConsumer:
    # Offsets are committed by InboxConsumer after the database transaction.
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        **overrides,
    )
