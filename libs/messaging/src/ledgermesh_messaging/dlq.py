"""Inspect dead-lettered messages and why they failed (LM-024).

    uv run ledgermesh-dlq wallet.commands.dlq
    uv run ledgermesh-dlq wallet.commands.dlq --limit 20 --json

Reads a dead-letter topic from the beginning without joining a consumer group,
so it never moves anyone's offsets, and prints one line per message: the
reason, the error, the consumer that gave up, the original topic position,
how many retries it had, and the event and transaction ids when the message
is still a valid envelope.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition

from .envelope import EnvelopeError, EventEnvelope
from .retry import (
    DLQ_CONSUMER,
    DLQ_ERROR,
    DLQ_FAILED_AT,
    DLQ_REASON,
    LAST_ERROR,
    ORIGINAL_OFFSET,
    ORIGINAL_PARTITION,
    ORIGINAL_TOPIC,
    RETRY_COUNT,
    headers_of,
)


def describe(record: Any) -> dict[str, Any]:
    """Everything an operator needs to know about one dead-lettered record."""
    headers = headers_of(record)
    entry: dict[str, Any] = {
        "dlq_offset": record.offset,
        "reason": headers.get(DLQ_REASON, "unknown"),
        "error": headers.get(DLQ_ERROR) or headers.get("error", ""),
        "consumer": headers.get(DLQ_CONSUMER, ""),
        "failed_at": headers.get(DLQ_FAILED_AT, ""),
        "retries": int(headers.get(RETRY_COUNT, "0") or 0),
        "original": {
            "topic": headers.get(ORIGINAL_TOPIC, ""),
            "partition": headers.get(ORIGINAL_PARTITION, ""),
            "offset": headers.get(ORIGINAL_OFFSET, ""),
        },
        "last_retry_error": headers.get(LAST_ERROR, ""),
    }
    try:
        envelope = EventEnvelope.from_bytes(record.value)
    except EnvelopeError:
        entry["event"] = None
    else:
        entry["event"] = {
            "event_id": str(envelope.event_id),
            "event_type": envelope.event_type,
            "transaction_id": str(envelope.transaction_id),
        }
    return entry


def format_line(entry: dict[str, Any]) -> str:
    event = entry["event"]
    ids = (
        f"{event['event_type']} event={event['event_id']} transaction={event['transaction_id']}"
        if event
        else "invalid envelope"
    )
    original = entry["original"]
    return (
        f"#{entry['dlq_offset']} {entry['reason']} by {entry['consumer'] or '?'} "
        f"at {entry['failed_at'] or '?'} after {entry['retries']} retries | {ids} | "
        f"from {original['topic'] or '?'}[{original['partition']}]@{original['offset']} | "
        f"{entry['error']}"
    )


async def read_dead_letters(
    bootstrap_servers: str,
    topic: str,
    limit: int | None = None,
    *,
    consumer: Any | None = None,
) -> list[dict[str, Any]]:
    """Read ``topic`` from the beginning up to the end offsets seen at the start.

    Messages dead-lettered while this runs are left for the next run, so the
    read always finishes. ``consumer`` replaces the Kafka consumer in tests.
    """
    if consumer is None:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers, group_id=None, enable_auto_commit=False
        )
    await consumer.start()
    try:
        partitions = consumer.partitions_for_topic(topic) or set()
        if not partitions:
            raise SystemExit(f"topic {topic!r} does not exist or has no partitions")
        assigned = [TopicPartition(topic, partition) for partition in sorted(partitions)]
        consumer.assign(assigned)
        await consumer.seek_to_beginning(*assigned)
        end = await consumer.end_offsets(assigned)
        entries: list[dict[str, Any]] = []
        while await _behind(consumer, assigned, end):
            batches = await consumer.getmany(*assigned, timeout_ms=1000)
            for partition, records in batches.items():
                entries.extend(
                    describe(record) for record in records if record.offset < end[partition]
                )
        entries.sort(key=lambda entry: entry["dlq_offset"])
        return entries[-limit:] if limit else entries
    finally:
        await consumer.stop()


async def _behind(consumer: Any, assigned: list[TopicPartition], end: dict) -> bool:
    """True while any partition's position is short of its captured end offset.

    Positions are awaited one by one first: ``any()`` cannot iterate an
    async generator, so the comparison must run on plain values.
    """
    positions = [await consumer.position(partition) for partition in assigned]
    return any(
        position < end[partition] for position, partition in zip(positions, assigned, strict=True)
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="List dead-lettered messages and why they failed.")
    parser.add_argument("topic", help="a dead-letter topic, e.g. wallet.commands.dlq")
    parser.add_argument(
        "--bootstrap-servers", default=os.environ.get("LM_KAFKA_BOOTSTRAP", "localhost:29092")
    )
    parser.add_argument("--limit", type=int, default=None, help="show only the newest N")
    parser.add_argument("--json", action="store_true", help="one JSON object per line")
    args = parser.parse_args(argv)

    entries = asyncio.run(read_dead_letters(args.bootstrap_servers, args.topic, args.limit))
    for entry in entries:
        print(json.dumps(entry, sort_keys=True) if args.json else format_line(entry))
    if not entries:
        print(f"{args.topic}: no dead-lettered messages")


if __name__ == "__main__":
    main()
