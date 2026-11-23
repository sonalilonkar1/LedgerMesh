import asyncio
from types import SimpleNamespace

import pytest

from services.coordinator.app.saga import InvalidSagaResult
from services.coordinator.app.saga_worker import RESULT_DLQ_TOPICS, publish_dead_letter


class FakeProducer:
    def __init__(self) -> None:
        self.sent = []

    async def send_and_wait(self, topic, **kwargs) -> None:
        self.sent.append((topic, kwargs))


@pytest.mark.parametrize(
    ("source_topic", "expected_dlq"),
    RESULT_DLQ_TOPICS.items(),
)
def test_invalid_result_is_published_to_its_source_topics_dlq(
    source_topic: str, expected_dlq: str
) -> None:
    producer = FakeProducer()
    record = SimpleNamespace(
        topic=source_topic,
        offset=7,
        value=b"wrong-correlation-result",
        key=b"transaction-id",
    )

    asyncio.run(
        publish_dead_letter(
            producer,
            record,
            InvalidSagaResult("result correlation_id does not match the redemption"),
        )
    )

    assert producer.sent == [
        (
            expected_dlq,
            {
                "value": record.value,
                "key": record.key,
                "headers": [
                    (
                        "error",
                        b"InvalidSagaResult('result correlation_id does not match the redemption')",
                    )
                ],
            },
        )
    ]


def test_unknown_result_topic_is_rejected() -> None:
    producer = FakeProducer()
    record = SimpleNamespace(topic="unknown.results", offset=1, value=b"event", key=b"key")

    with pytest.raises(ValueError, match="no Coordinator result DLQ"):
        asyncio.run(publish_dead_letter(producer, record, InvalidSagaResult("invalid")))

    assert producer.sent == []
