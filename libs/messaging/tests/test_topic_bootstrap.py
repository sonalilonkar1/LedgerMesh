import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType

from ledgermesh_messaging import ALL_TOPICS, TopicMismatchError, ensure_topics
from ledgermesh_messaging.topics import partition_counts

from .conftest import KAFKA_BOOTSTRAP

pytestmark = [pytest.mark.integration, pytest.mark.kafka]


@pytest.fixture
async def admin():
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    await admin.start()
    yield admin
    await admin.close()


@pytest.fixture
async def specs(admin):
    # A unique prefix keeps each test's topics apart on the shared broker.
    prefix = f"lm-test-{uuid4().hex[:8]}."
    specs = [replace(spec, name=prefix + spec.name) for spec in ALL_TOPICS]
    yield specs
    present = set(await admin.list_topics())
    await admin.delete_topics([spec.name for spec in specs if spec.name in present])


async def counts_for(admin, specs):
    return await partition_counts(admin, [spec.name for spec in specs])


async def topic_configs(admin, name, keys):
    [response] = await admin.describe_configs(
        [ConfigResource(ConfigResourceType.TOPIC, name, configs=dict.fromkeys(keys))]
    )
    [(code, _message, _type, _name, entries)] = response.resources
    if code:
        return {key: None for key in keys}
    values = {entry[0]: entry[1] for entry in entries}
    return {key: values.get(key) for key in keys}


async def wait_for_configs(admin, name, expected, timeout=10.0):
    # Config changes reach the broker shortly after the controller accepts them,
    # and until then the broker can answer with an error or no entries at all.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while (actual := await topic_configs(admin, name, expected)) != expected:
        if loop.time() >= deadline:
            break
        await asyncio.sleep(0.2)
    return actual


async def test_first_run_creates_every_topic_with_its_partitions(admin, specs):
    report = await ensure_topics(admin, specs)

    assert set(report.created) == {spec.name for spec in specs}
    assert report.existing == ()
    assert await counts_for(admin, specs) == {spec.name: spec.partitions for spec in specs}


async def test_created_topics_have_their_declared_layout_and_configs(admin, specs):
    await ensure_topics(admin, specs)

    counts = await counts_for(admin, specs)
    for name, count in counts.items():
        if name.endswith(".dlq"):
            assert count == 1, name
        elif name.endswith(".retry"):
            assert count == counts[name.removesuffix(".retry")], name

    for spec in specs:
        expected = spec.configs(1)
        assert await wait_for_configs(admin, spec.name, expected) == expected, spec.name


async def test_second_run_is_a_no_op(admin, specs):
    await ensure_topics(admin, specs)
    report = await ensure_topics(admin, specs)

    assert report.created == ()
    assert set(report.existing) == {spec.name for spec in specs}


async def test_rerun_restores_retention_drift(admin, specs):
    await ensure_topics(admin, specs)
    commands = specs[0]
    await admin.alter_configs(
        [ConfigResource(ConfigResourceType.TOPIC, commands.name, configs={"retention.ms": "1000"})]
    )
    drifted = {"retention.ms": "1000"}
    assert await wait_for_configs(admin, commands.name, drifted) == drifted

    await ensure_topics(admin, specs)

    expected = commands.configs(1)
    assert await wait_for_configs(admin, commands.name, expected) == expected


async def test_existing_topic_with_wrong_partition_count_is_reported_not_changed(admin, specs):
    commands = specs[0]
    await admin.create_topics([NewTopic(commands.name, 2, 1)])

    with pytest.raises(TopicMismatchError, match=commands.name):
        await ensure_topics(admin, specs)

    assert await counts_for(admin, [commands]) == {commands.name: 2}


async def test_existing_topics_with_a_different_replication_factor_are_reported_not_changed(
    admin, specs
):
    # The test broker has one node, so the topics exist with replication factor 1.
    await ensure_topics(admin, specs)
    commands = specs[0]

    with pytest.raises(TopicMismatchError, match="replication factor 1, expected 2"):
        await ensure_topics(admin, specs, replication_factor=2)

    # min.insync.replicas would be 2 for replication factor 2; it must stay 1.
    unchanged = {"min.insync.replicas": "1"}
    assert await wait_for_configs(admin, commands.name, unchanged) == unchanged
