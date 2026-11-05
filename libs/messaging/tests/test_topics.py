from uuid import uuid4

import pytest

from ledgermesh_messaging import (
    ALL_TOPICS,
    CATALOG_COMMANDS,
    CATALOG_RESULTS,
    REDEMPTION_EVENTS,
    WALLET_COMMANDS,
    WALLET_RESULTS,
    KeyBy,
    TopicKeyError,
    consumer_groups,
    dead_letter_topic,
    retry_topic,
)

from .conftest import make_envelope


def test_wallet_commands_are_keyed_by_user_id():
    envelope = make_envelope(aggregate_type="wallet", aggregate_id="user-42")
    message = WALLET_COMMANDS.message(envelope)
    assert (message.topic, message.key) == ("wallet.commands", "user-42")


def test_catalog_commands_are_keyed_by_reward_id():
    envelope = make_envelope(aggregate_type="reward", aggregate_id="reward-7")
    message = CATALOG_COMMANDS.message(envelope)
    assert (message.topic, message.key) == ("catalog.commands", "reward-7")


@pytest.mark.parametrize("spec", [WALLET_RESULTS, CATALOG_RESULTS, REDEMPTION_EVENTS])
def test_results_and_events_are_keyed_by_transaction_id(spec):
    transaction_id = uuid4()
    envelope = make_envelope(transaction_id=transaction_id, aggregate_id="user-1")
    assert spec.message(envelope).key == str(transaction_id)


def test_command_for_the_wrong_aggregate_is_rejected():
    # A reward command on the wallet topic would be ordered by the wrong key.
    with pytest.raises(TopicKeyError):
        WALLET_COMMANDS.message(make_envelope(aggregate_type="reward", aggregate_id="reward-7"))


def test_headers_pass_through():
    message = WALLET_COMMANDS.message(make_envelope(), headers={"traceparent": "00-abc"})
    assert message.headers == {"traceparent": "00-abc"}


def test_every_main_topic_has_retry_and_dead_letter_topics():
    names = {spec.name for spec in ALL_TOPICS}
    for base in ("wallet.commands", "wallet.results", "catalog.commands", "catalog.results", "redemption.events"):
        assert {base, f"{base}.retry", f"{base}.dlq"} <= names
    assert len(names) == len(ALL_TOPICS) == 15


def test_retry_topic_keeps_key_and_partitions_of_its_source():
    retry = retry_topic(WALLET_COMMANDS)
    assert retry.key_by is KeyBy.AGGREGATE
    assert retry.partitions == WALLET_COMMANDS.partitions
    assert retry.consumer_group == "wallet-saga.retry"


def test_dead_letter_topic_has_no_consumer_group_and_longer_retention():
    dlq = dead_letter_topic(WALLET_COMMANDS)
    assert dlq.consumer_group is None
    assert dlq.retention_ms > WALLET_COMMANDS.retention_ms


def test_consumer_group_ownership():
    assert consumer_groups() == {
        "wallet-saga": ("wallet.commands",),
        "wallet-saga.retry": ("wallet.commands.retry",),
        "coordinator-saga": ("wallet.results", "catalog.results"),
        "coordinator-saga.retry": ("wallet.results.retry", "catalog.results.retry"),
        "catalog-saga": ("catalog.commands",),
        "catalog-saga.retry": ("catalog.commands.retry",),
        "audit-ledger": ("redemption.events",),
        "audit-ledger.retry": ("redemption.events.retry",),
    }


def test_min_insync_replicas_never_exceeds_replication_factor():
    assert WALLET_COMMANDS.configs(1)["min.insync.replicas"] == "1"
    assert WALLET_COMMANDS.configs(3)["min.insync.replicas"] == "2"
