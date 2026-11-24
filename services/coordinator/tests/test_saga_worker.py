import asyncio
from types import SimpleNamespace

import pytest
from ledgermesh_messaging import (
    DeadLetterReason,
    FailureRouter,
    RetryPolicy,
    TransientError,
    dead_letter_topic,
    retry_topic,
)

from services.coordinator.app.saga import InvalidSagaResult
from services.coordinator.app.saga_worker import CONSUMER_GROUP, RESULT_TOPICS


class FakeProducer:
    def __init__(self) -> None:
        self.sent = []

    async def send_and_wait(self, topic, **kwargs) -> None:
        self.sent.append((topic, kwargs))


def router(producer: FakeProducer) -> FailureRouter:
    return FailureRouter(producer, consumer=CONSUMER_GROUP, policy=RetryPolicy())


def result_record(topic: str) -> SimpleNamespace:
    return SimpleNamespace(
        topic=topic,
        partition=0,
        offset=7,
        value=b"wrong-correlation-result",
        key=b"transaction-id",
        headers=[],
    )


@pytest.mark.parametrize("spec", RESULT_TOPICS, ids=lambda spec: spec.name)
def test_invalid_result_is_published_to_its_source_topics_dlq(spec) -> None:
    producer = FakeProducer()
    record = result_record(spec.name)

    asyncio.run(
        router(producer).dead_letter(
            record,
            InvalidSagaResult("result correlation_id does not match the redemption"),
            DeadLetterReason.PERMANENT_ERROR,
        )
    )

    [(topic, kwargs)] = producer.sent
    assert topic == dead_letter_topic(spec).name
    assert (kwargs["value"], kwargs["key"]) == (record.value, record.key)
    headers = dict(kwargs["headers"])
    assert headers["lm-dlq-reason"] == b"permanent_error"
    assert headers["lm-dlq-consumer"] == b"coordinator-saga"
    assert b"correlation_id does not match" in headers["lm-dlq-error"]


@pytest.mark.parametrize("spec", RESULT_TOPICS, ids=lambda spec: spec.name)
def test_a_transient_failure_retries_on_its_own_results_retry_topic(spec) -> None:
    producer = FakeProducer()

    asyncio.run(router(producer).retry_or_dead_letter(result_record(spec.name), TransientError()))

    assert [topic for topic, _ in producer.sent] == [retry_topic(spec).name]


def test_both_result_retry_topics_share_one_retry_group() -> None:
    assert {retry_topic(spec).consumer_group for spec in RESULT_TOPICS} == {
        f"{CONSUMER_GROUP}.retry"
    }


def test_unknown_result_topic_is_rejected() -> None:
    producer = FakeProducer()

    with pytest.raises(ValueError, match="not a LedgerMesh"):
        asyncio.run(
            router(producer).dead_letter(
                result_record("unknown.results"),
                InvalidSagaResult("invalid"),
                DeadLetterReason.PERMANENT_ERROR,
            )
        )

    assert producer.sent == []
