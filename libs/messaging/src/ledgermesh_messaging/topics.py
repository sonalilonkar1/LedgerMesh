"""Kafka topics, message keys and consumer groups for the Saga workflow (LM-018).

Every topic is declared here, so services import names instead of typing them
and ``ensure_topics`` creates the same set in every environment. Producers
build messages with ``TopicSpec.message`` so a command can never be sent with
the wrong key, which would break per-user or per-reward ordering.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from enum import StrEnum

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.errors import TopicAlreadyExistsError, for_code

from .envelope import EventEnvelope
from .outbox import OutgoingMessage

DAY_MS = 24 * 60 * 60 * 1000


class KeyBy(StrEnum):
    # Commands are ordered per wallet or reward, results per transaction.
    AGGREGATE = "aggregate_id"
    TRANSACTION = "transaction_id"


class TopicKeyError(ValueError):
    """Raised when an envelope does not belong on the topic it is sent to."""


class TopicMismatchError(RuntimeError):
    """Raised when an existing topic has a different partition count or replication factor."""


@dataclass(frozen=True)
class TopicSpec:
    name: str
    key_by: KeyBy
    producer: str
    consumer_group: str | None
    aggregate_type: str | None = None
    partitions: int = 6
    retention_ms: int = 7 * DAY_MS

    def key_for(self, envelope: EventEnvelope) -> str:
        if self.key_by is KeyBy.TRANSACTION:
            return str(envelope.transaction_id)
        if envelope.aggregate_type != self.aggregate_type:
            raise TopicKeyError(
                f"{self.name} carries {self.aggregate_type} commands, "
                f"got aggregate_type={envelope.aggregate_type!r}"
            )
        return envelope.aggregate_id

    def message(self, envelope: EventEnvelope, headers: Mapping[str, str] | None = None) -> OutgoingMessage:
        return OutgoingMessage(
            topic=self.name, key=self.key_for(envelope), envelope=envelope, headers=headers or {}
        )

    def configs(self, replication_factor: int) -> dict[str, str]:
        return {
            "cleanup.policy": "delete",
            "retention.ms": str(self.retention_ms),
            "min.insync.replicas": str(min(2, replication_factor)),
        }


WALLET_COMMANDS = TopicSpec(
    "wallet.commands",
    KeyBy.AGGREGATE,
    producer="coordinator",
    consumer_group="wallet-saga",
    aggregate_type="wallet",
)
WALLET_RESULTS = TopicSpec(
    "wallet.results", KeyBy.TRANSACTION, producer="wallet", consumer_group="coordinator-saga"
)
CATALOG_COMMANDS = TopicSpec(
    "catalog.commands",
    KeyBy.AGGREGATE,
    producer="coordinator",
    consumer_group="catalog-saga",
    aggregate_type="reward",
)
CATALOG_RESULTS = TopicSpec(
    "catalog.results", KeyBy.TRANSACTION, producer="catalog", consumer_group="coordinator-saga"
)
REDEMPTION_EVENTS = TopicSpec(
    "redemption.events", KeyBy.TRANSACTION, producer="coordinator", consumer_group="audit-ledger"
)

MAIN_TOPICS = (WALLET_COMMANDS, WALLET_RESULTS, CATALOG_COMMANDS, CATALOG_RESULTS, REDEMPTION_EVENTS)


def retry_topic(spec: TopicSpec) -> TopicSpec:
    # Same key and partition count as the source, for stable partition affinity
    # within the retry topic. It is an independent Kafka topic, so it does not
    # preserve ordering relative to the main topic: a retried message can be
    # processed after newer messages for its key on the main topic. LM-024 and
    # LM-026 handle delayed and out-of-order retries explicitly, using
    # transaction_id, workflow_step, causation_id and inbox idempotency. A
    # separate group lets the delayed retry consumer run without stalling the
    # main consumer.
    return replace(spec, name=f"{spec.name}.retry", consumer_group=f"{spec.consumer_group}.retry")


def dead_letter_topic(spec: TopicSpec) -> TopicSpec:
    # Read by operators and LM-024 tooling, not by a service, and kept longer.
    return replace(
        spec, name=f"{spec.name}.dlq", consumer_group=None, partitions=1, retention_ms=30 * DAY_MS
    )


ALL_TOPICS = tuple(
    topic for spec in MAIN_TOPICS for topic in (spec, retry_topic(spec), dead_letter_topic(spec))
)


def consumer_groups(specs: Iterable[TopicSpec] = ALL_TOPICS) -> dict[str, tuple[str, ...]]:
    """Map each consumer group to the topics it owns."""
    groups: dict[str, list[str]] = {}
    for spec in specs:
        if spec.consumer_group is not None:
            groups.setdefault(spec.consumer_group, []).append(spec.name)
    return {group: tuple(names) for group, names in groups.items()}


@dataclass(frozen=True)
class BootstrapReport:
    created: tuple[str, ...]
    existing: tuple[str, ...]


async def ensure_topics(
    admin: AIOKafkaAdminClient,
    specs: Iterable[TopicSpec] = ALL_TOPICS,
    *,
    replication_factor: int = 1,
) -> BootstrapReport:
    """Create missing topics and reapply their mutable configs to existing ones.

    Safe to run any number of times. An existing topic with a different
    partition count or replication factor is reported, not changed, and no
    configs are applied: adding partitions moves keys to other partitions and
    would break per-key ordering for messages in flight, and
    ``min.insync.replicas`` derived from the wrong replication factor could make
    ``acks=all`` producers fail.
    """
    if replication_factor < 1:
        raise ValueError(f"replication_factor must be at least 1, got {replication_factor}")
    specs = list(specs)
    # list_topics never auto-creates, unlike a metadata request for one name.
    present = set(await admin.list_topics())
    missing = [spec for spec in specs if spec.name not in present]

    created: list[str] = []
    if missing:
        new_topics = [
            NewTopic(spec.name, spec.partitions, replication_factor, topic_configs=spec.configs(replication_factor))
            for spec in missing
        ]
        response = await admin.create_topics(new_topics)
        for topic, code, *rest in response.topic_errors:
            if code == 0:
                created.append(topic)
            elif code != TopicAlreadyExistsError.errno:
                # Another bootstrap may have created it first; anything else is fatal.
                raise for_code(code)(f"could not create {topic}: {rest[0] if rest else ''}")

    existing = [spec for spec in specs if spec.name not in created]
    if existing:
        # Checked before any config is applied, so a mismatch changes nothing.
        await _check_layout(admin, existing, replication_factor)
        await _apply_configs(admin, existing, replication_factor)

    return BootstrapReport(created=tuple(created), existing=tuple(spec.name for spec in existing))


@dataclass(frozen=True)
class TopicLayout:
    partitions: int
    replication_factor: int


async def topic_layouts(
    admin: AIOKafkaAdminClient, names: list[str], *, timeout: float = 10.0
) -> dict[str, TopicLayout]:
    """Return each topic's partition count and replication factor.

    Just after a topic is created the broker can list it with no partitions
    until their leaders are elected, so this polls until every topic has some.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        described = await admin.describe_topics(names)
        layouts = {
            topic["topic"]: TopicLayout(
                partitions=len(topic["partitions"]),
                replication_factor=min(len(p["replicas"]) for p in topic["partitions"]),
            )
            for topic in described
            if topic.get("error_code", 0) == 0 and topic["partitions"]
        }
        if len(layouts) == len(names) or loop.time() >= deadline:
            return layouts
        await asyncio.sleep(0.2)


async def partition_counts(
    admin: AIOKafkaAdminClient, names: list[str], *, timeout: float = 10.0
) -> dict[str, int]:
    layouts = await topic_layouts(admin, names, timeout=timeout)
    return {name: layout.partitions for name, layout in layouts.items()}


async def _check_layout(
    admin: AIOKafkaAdminClient, specs: list[TopicSpec], replication_factor: int
) -> None:
    actual = await topic_layouts(admin, [spec.name for spec in specs])
    wrong = []
    for spec in specs:
        layout = actual.get(spec.name)
        if layout is None:
            wrong.append(f"{spec.name} reported no partitions")
            continue
        if layout.partitions != spec.partitions:
            wrong.append(
                f"{spec.name} has {layout.partitions} partitions, expected {spec.partitions}"
            )
        if layout.replication_factor != replication_factor:
            # Changing it needs a partition reassignment, which this tool does not do.
            wrong.append(
                f"{spec.name} has replication factor {layout.replication_factor}, "
                f"expected {replication_factor}"
            )
    if wrong:
        raise TopicMismatchError("; ".join(wrong))


async def _apply_configs(admin: AIOKafkaAdminClient, specs: list[TopicSpec], replication_factor: int) -> None:
    resources = [
        ConfigResource(ConfigResourceType.TOPIC, spec.name, configs=spec.configs(replication_factor))
        for spec in specs
    ]
    responses = await admin.alter_configs(resources)
    for response in responses:
        for code, message, _type, name in response.resources:
            if code:
                raise for_code(code)(f"could not update {name}: {message}")


async def _bootstrap(bootstrap_servers: str, replication_factor: int) -> BootstrapReport:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        return await ensure_topics(admin, replication_factor=replication_factor)
    finally:
        await admin.close()


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {number}")
    return number


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update the LedgerMesh Kafka topics.")
    # 29092 is the host port of the Kafka broker in infra/compose.yaml (LM-003).
    parser.add_argument(
        "--bootstrap-servers", default=os.environ.get("LM_KAFKA_BOOTSTRAP", "localhost:29092")
    )
    parser.add_argument("--replication-factor", type=_positive_int, default=1)
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()

    report = asyncio.run(_bootstrap(args.bootstrap_servers, args.replication_factor))
    for name in report.created:
        print(f"created  {name}")
    for name in report.existing:
        print(f"ok       {name}")


if __name__ == "__main__":
    main()
