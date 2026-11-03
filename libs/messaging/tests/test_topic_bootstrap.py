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


async def retention(admin, name):
    [response] = await admin.describe_configs(
        [ConfigResource(ConfigResourceType.TOPIC, name, configs={"retention.ms": None})]
    )
    [(_code, _message, _type, _name, entries)] = response.resources
    return {entry[0]: entry[1] for entry in entries}["retention.ms"]


async def wait_for_retention(admin, name, expected, timeout=10.0):
    # Config changes reach the broker shortly after the controller accepts them.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while (value := await retention(admin, name)) != expected and loop.time() < deadline:
        await asyncio.sleep(0.2)
    return value


async def test_first_run_creates_every_topic_with_its_partitions(admin, specs):
    report = await ensure_topics(admin, specs)

    assert set(report.created) == {spec.name for spec in specs}
    assert report.existing == ()
    assert await counts_for(admin, specs) == {spec.name: spec.partitions for spec in specs}


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
    assert await wait_for_retention(admin, commands.name, "1000") == "1000"

    await ensure_topics(admin, specs)

    expected = str(commands.retention_ms)
    assert await wait_for_retention(admin, commands.name, expected) == expected


async def test_existing_topic_with_wrong_partition_count_is_reported_not_changed(admin, specs):
    commands = specs[0]
    await admin.create_topics([NewTopic(commands.name, 2, 1)])

    with pytest.raises(TopicMismatchError, match=commands.name):
        await ensure_topics(admin, specs)

    assert await counts_for(admin, [commands]) == {commands.name: 2}
