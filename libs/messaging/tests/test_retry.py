"""Retry policy, retry-topic routing and dead-letter headers (LM-024). No Docker needed."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest

from ledgermesh_messaging import (
    ALL_TOPICS,
    CATALOG_COMMANDS,
    CATALOG_RESULTS,
    REDEMPTION_EVENTS,
    WALLET_COMMANDS,
    WALLET_RESULTS,
    DeadLetterReason,
    EventEnvelope,
    FailureRouter,
    PermanentError,
    RetryConfigError,
    RetryPolicy,
    TransientError,
    dead_letter_topic,
    retry_topic,
)
from ledgermesh_messaging.dlq import describe, format_line
from ledgermesh_messaging.retry import (
    DLQ_CONSUMER,
    DLQ_ERROR,
    DLQ_FAILED_AT,
    DLQ_REASON,
    LAST_ERROR,
    NOT_BEFORE_MS,
    ORIGINAL_OFFSET,
    ORIGINAL_PARTITION,
    ORIGINAL_TOPIC,
    RETRY_COUNT,
    headers_of,
    not_before,
    retry_count,
    source_topic,
)

NOW = 1_700_000_000.0


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, **kwargs):
        self.sent.append((topic, kwargs))


def envelope():
    return EventEnvelope(
        event_type="wallet.deduct_points",
        transaction_id=uuid4(),
        correlation_id=uuid4(),
        aggregate_type="wallet",
        aggregate_id="user-1",
        workflow_step=1,
    )


def record(topic="wallet.commands", value=None, headers=(), partition=2, offset=41):
    return SimpleNamespace(
        topic=topic,
        partition=partition,
        offset=offset,
        key=b"user-1",
        value=envelope().to_bytes() if value is None else value,
        headers=list(headers),
    )


def as_record(sent, partition=0, offset=0):
    """What a consumer of the target topic would read back."""
    topic, kwargs = sent
    return SimpleNamespace(
        topic=topic,
        partition=partition,
        offset=offset,
        key=kwargs["key"],
        value=kwargs["value"],
        headers=kwargs["headers"],
    )


def router(producer, **policy):
    return FailureRouter(
        producer, consumer="wallet-saga", policy=RetryPolicy(**policy), clock=lambda: NOW
    )


# --- Retry count and backoff are configurable ------------------------------


def test_policy_reads_every_setting_from_the_environment():
    policy = RetryPolicy.from_env(
        {
            "LM_RETRY_ATTEMPTS": "4",
            "LM_RETRY_BACKOFF_SECONDS": "0.5",
            "LM_RETRY_REDELIVERIES": "2",
            "LM_RETRY_REDELIVERY_DELAY_SECONDS": "10",
            "LM_RETRY_MAX_DELAY_SECONDS": "40",
            "LM_RETRY_HANDLER_TIMEOUT_SECONDS": "7.5",
        }
    )

    assert policy == RetryPolicy(
        attempts=4,
        backoff_seconds=0.5,
        redeliveries=2,
        redelivery_delay_seconds=10,
        max_delay_seconds=40,
        handler_timeout_seconds=7.5,
    )


def test_unset_or_empty_variables_keep_the_defaults():
    assert RetryPolicy.from_env({"LM_RETRY_ATTEMPTS": ""}) == RetryPolicy()


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("LM_RETRY_ATTEMPTS", "three"),
        ("LM_RETRY_ATTEMPTS", "0"),
        ("LM_RETRY_REDELIVERIES", "-1"),
        ("LM_RETRY_BACKOFF_SECONDS", "-0.1"),
        ("LM_RETRY_HANDLER_TIMEOUT_SECONDS", "0"),
    ],
)
def test_invalid_settings_fail_at_startup(variable, value):
    with pytest.raises(RetryConfigError):
        RetryPolicy.from_env({variable: value})


def test_backoff_and_redelivery_delay_double_up_to_the_cap():
    policy = RetryPolicy(backoff_seconds=1, redelivery_delay_seconds=5, max_delay_seconds=30)

    assert [policy.backoff(attempt) for attempt in (1, 2, 3, 6)] == [1, 2, 4, 30]
    assert [policy.redelivery_delay(n) for n in (1, 2, 3, 4)] == [5, 10, 20, 30]


# --- Retries preserve event and transaction IDs ----------------------------


def test_first_retry_republishes_the_original_bytes_to_the_retry_topic():
    producer = FakeProducer()
    original = record()

    asyncio.run(
        router(producer, redelivery_delay_seconds=5).retry_or_dead_letter(
            original, TransientError("db busy")
        )
    )

    [(topic, kwargs)] = producer.sent
    assert topic == retry_topic(WALLET_COMMANDS).name
    assert kwargs["value"] == original.value
    assert kwargs["key"] == original.key
    retried = as_record(producer.sent[0])
    sent_envelope = EventEnvelope.from_bytes(retried.value)
    assert sent_envelope == EventEnvelope.from_bytes(original.value)
    headers = headers_of(retried)
    assert headers[RETRY_COUNT] == "1"
    assert headers[ORIGINAL_TOPIC] == "wallet.commands"
    assert (headers[ORIGINAL_PARTITION], headers[ORIGINAL_OFFSET]) == ("2", "41")
    assert headers[NOT_BEFORE_MS] == str(int((NOW + 5) * 1000))
    assert "db busy" in headers[LAST_ERROR]
    assert not_before(retried) == NOW + 5


def test_later_retries_keep_the_original_position_and_count_up():
    producer = FakeProducer()
    route = router(producer, redeliveries=3, redelivery_delay_seconds=1)
    current = record()

    for _ in range(3):
        asyncio.run(route.retry_or_dead_letter(current, TransientError("still busy")))
        current = as_record(producer.sent[-1], partition=5, offset=len(producer.sent))

    assert [retry_count(as_record(sent)) for sent in producer.sent] == [1, 2, 3]
    assert {headers_of(as_record(sent))[ORIGINAL_OFFSET] for sent in producer.sent} == {"41"}
    assert len({sent[1]["value"] for sent in producer.sent}) == 1


# --- Exhausted messages enter a dead-letter topic --------------------------


def test_exhausted_redeliveries_go_to_the_source_topics_dlq():
    producer = FakeProducer()
    route = router(producer, redeliveries=2)
    current = record()

    for _ in range(3):
        asyncio.run(route.retry_or_dead_letter(current, TransientError("still busy")))
        current = as_record(producer.sent[-1], offset=len(producer.sent))

    assert [topic for topic, _ in producer.sent] == [
        "wallet.commands.retry",
        "wallet.commands.retry",
        "wallet.commands.dlq",
    ]
    dlq = headers_of(as_record(producer.sent[-1]))
    assert dlq[DLQ_REASON] == "retries_exhausted"
    assert dlq[RETRY_COUNT] == "2"
    assert dlq[ORIGINAL_OFFSET] == "41"
    assert producer.sent[-1][1]["value"] == producer.sent[0][1]["value"]


def test_zero_redeliveries_dead_letters_immediately():
    producer = FakeProducer()

    asyncio.run(router(producer, redeliveries=0).retry_or_dead_letter(record(), TransientError()))

    assert [topic for topic, _ in producer.sent] == ["wallet.commands.dlq"]


@pytest.mark.parametrize(
    "spec", [WALLET_COMMANDS, WALLET_RESULTS, CATALOG_COMMANDS, CATALOG_RESULTS, REDEMPTION_EVENTS]
)
def test_every_main_and_retry_topic_routes_to_its_own_retry_and_dlq(spec):
    for topic in (spec.name, retry_topic(spec).name):
        assert source_topic(record(topic=topic)) == spec

    producer = FakeProducer()
    asyncio.run(router(producer).retry_or_dead_letter(record(topic=spec.name), TransientError()))
    asyncio.run(
        router(producer).dead_letter(
            record(topic=spec.name), PermanentError(), DeadLetterReason.PERMANENT_ERROR
        )
    )

    assert [topic for topic, _ in producer.sent] == [
        retry_topic(spec).name,
        dead_letter_topic(spec).name,
    ]
    assert {topic for topic, _ in producer.sent} <= {spec.name for spec in ALL_TOPICS}


def test_an_unknown_topic_is_refused_rather_than_guessed():
    with pytest.raises(ValueError, match="not a LedgerMesh"):
        asyncio.run(
            router(FakeProducer()).retry_or_dead_letter(record(topic="other"), TransientError())
        )


# --- Dead-letter reason is recorded and visible ----------------------------


def test_dead_letter_headers_record_reason_error_consumer_and_time():
    producer = FakeProducer()

    asyncio.run(
        router(producer).dead_letter(
            record(), PermanentError("unsupported event"), DeadLetterReason.PERMANENT_ERROR
        )
    )

    headers = headers_of(as_record(producer.sent[0]))
    assert headers[DLQ_REASON] == "permanent_error"
    assert headers[DLQ_ERROR] == "PermanentError('unsupported event')"
    assert headers[DLQ_CONSUMER] == "wallet-saga"
    assert headers[DLQ_FAILED_AT] == "2023-11-14T22:13:20+00:00"
    assert headers[RETRY_COUNT] == "0"


def test_long_errors_are_truncated():
    producer = FakeProducer()

    asyncio.run(
        router(producer).dead_letter(
            record(), PermanentError("x" * 5000), DeadLetterReason.PERMANENT_ERROR
        )
    )

    assert len(headers_of(as_record(producer.sent[0]))[DLQ_ERROR]) == 1000


def test_describe_shows_reason_ids_and_origin_of_a_dead_letter():
    producer = FakeProducer()
    original = record()
    asyncio.run(
        router(producer, redeliveries=0).retry_or_dead_letter(original, TransientError("timeout"))
    )
    parked = as_record(producer.sent[0], offset=9)
    sent_envelope = EventEnvelope.from_bytes(original.value)

    entry = describe(parked)

    assert entry["reason"] == "retries_exhausted"
    assert entry["consumer"] == "wallet-saga"
    assert entry["original"] == {"topic": "wallet.commands", "partition": "2", "offset": "41"}
    assert entry["event"]["event_id"] == str(sent_envelope.event_id)
    assert entry["event"]["transaction_id"] == str(sent_envelope.transaction_id)
    line = format_line(entry)
    assert "retries_exhausted by wallet-saga" in line
    assert str(sent_envelope.transaction_id) in line


def test_describe_handles_an_invalid_envelope():
    producer = FakeProducer()
    asyncio.run(
        router(producer).dead_letter(
            record(value=b"not json"), ValueError("bad"), DeadLetterReason.INVALID_ENVELOPE
        )
    )

    entry = describe(as_record(producer.sent[0]))

    assert entry["reason"] == "invalid_envelope"
    assert entry["event"] is None
    assert "invalid envelope" in format_line(entry)
