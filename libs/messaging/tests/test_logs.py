"""Structured JSON logs, correlation context, redaction and fault ids (LM-032). No Docker."""

import asyncio
import contextvars
import io
import json
import logging
from uuid import uuid4

import pytest

from ledgermesh_messaging import (
    CorrelationMiddleware,
    EventEnvelope,
    configure_logging,
    envelope_fields,
    fault,
    log_context,
    log_fault_injected,
)
from ledgermesh_messaging.logs import (
    BASE_FIELDS,
    CORRELATION_FIELDS,
    REDACTED,
    JsonFormatter,
    current_context,
)


@pytest.fixture
def capture():
    """A logger whose output is parsed back into dicts."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter("wallet"))
    logger = logging.getLogger(f"test.logs.{uuid4().hex}")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    def lines():
        return [json.loads(line) for line in stream.getvalue().splitlines()]

    return logger, lines


def envelope(**overrides):
    fields = {
        "event_type": "wallet.deduct_points",
        "transaction_id": uuid4(),
        "correlation_id": uuid4(),
        "causation_id": uuid4(),
        "aggregate_type": "wallet",
        "aggregate_id": "user-1",
        "workflow_step": 1,
    }
    fields.update(overrides)
    return EventEnvelope(**fields)


# --- All services use the same logging field names -------------------------


def test_every_line_has_every_shared_field_even_when_unknown(capture):
    logger, lines = capture

    logger.info("hello %s", "world")

    [line] = lines()
    assert set(BASE_FIELDS + CORRELATION_FIELDS) <= set(line)
    assert line["message"] == "hello world"
    assert line["service"] == "wallet"
    assert line["level"] == "INFO"
    assert line["transaction_id"] is None
    assert line["timestamp"].endswith("+00:00")


def test_bound_context_reaches_every_log_in_the_block(capture):
    logger, lines = capture
    message = envelope()

    with log_context(**envelope_fields(message)):
        logger.info("inside")
        with log_context(state="applied"):
            logger.info("nested")
    logger.info("outside")

    inside, nested, outside = lines()
    for line in (inside, nested):
        assert line["transaction_id"] == str(message.transaction_id)
        assert line["event_id"] == str(message.event_id)
        assert line["correlation_id"] == str(message.correlation_id)
        assert line["causation_id"] == str(message.causation_id)
        assert line["event_type"] == "wallet.deduct_points"
        assert line["workflow_step"] == 1
    assert nested["state"] == "applied"
    assert inside["state"] is None
    assert outside["transaction_id"] is None


def test_none_never_erases_a_bound_id(capture):
    logger, lines = capture
    transaction_id = uuid4()

    with log_context(transaction_id=transaction_id), log_context(transaction_id=None):
        logger.info("still bound", extra={"correlation_id": None})

    [line] = lines()
    assert line["transaction_id"] == str(transaction_id)


def test_extra_fields_override_the_context_and_are_kept(capture):
    logger, lines = capture

    with log_context(state="REQUESTED"):
        logger.info("moved", extra={"state": "DEDUCTING_POINTS", "from_state": "REQUESTED"})

    [line] = lines()
    assert (line["state"], line["from_state"]) == ("DEDUCTING_POINTS", "REQUESTED")


def test_envelope_fields_accept_an_envelope_or_its_json():
    message = envelope()

    assert envelope_fields(message) == envelope_fields(message.to_json())


def test_context_does_not_leak_between_concurrent_tasks(capture):
    logger, lines = capture
    first, second = uuid4(), uuid4()

    async def handle(transaction_id):
        with log_context(transaction_id=transaction_id):
            await asyncio.sleep(0)
            logger.info("handled")

    async def both():
        await asyncio.gather(handle(first), handle(second))

    asyncio.run(both())

    assert {line["transaction_id"] for line in lines()} == {str(first), str(second)}


# --- Sensitive values are excluded -----------------------------------------


@pytest.mark.parametrize(
    "key",
    ["password", "db_password", "api_key", "token", "authorization", "database_url", "secret"],
)
def test_secret_like_fields_are_redacted(capture, key):
    logger, lines = capture

    logger.info("config", extra={key: "hunter2"})

    [line] = lines()
    assert line[key] == REDACTED
    assert "hunter2" not in json.dumps(line)


def test_fencing_tokens_are_not_secrets(capture):
    logger, lines = capture

    logger.info("fenced", extra={"fencing_token": 7})

    assert lines()[0]["fencing_token"] == 7


def test_credentials_in_urls_are_masked_everywhere(capture):
    logger, lines = capture
    url = "postgresql+psycopg://wallet:s3cret@wallet-db:5432/wallet"

    logger.info("connecting to %s", url, extra={"target": url})
    try:
        raise RuntimeError(f"cannot reach {url}")
    except RuntimeError:
        logger.exception("failed")

    text = json.dumps(lines())
    assert "s3cret" not in text
    assert "wallet:***@wallet-db" in text


def test_nested_mappings_are_redacted(capture):
    logger, lines = capture

    logger.info("headers", extra={"request": {"Authorization": "Bearer abc", "path": "/v1"}})

    assert lines()[0]["request"] == {"Authorization": REDACTED, "path": "/v1"}


@pytest.mark.parametrize("container", [list, tuple])
def test_secrets_inside_lists_and_tuples_are_redacted(capture, container):
    logger, lines = capture

    logger.info(
        "outgoing",
        extra={"headers": container([{"authorization": "Bearer secret"}, {"accept": "json"}])},
    )

    [line] = lines()
    assert line["headers"] == [{"authorization": REDACTED}, {"accept": "json"}]
    assert "Bearer secret" not in json.dumps(line)


def test_secrets_are_redacted_at_any_depth(capture):
    logger, lines = capture
    url = "postgresql://wallet:s3cret@db/wallet"

    logger.info(
        "deep",
        extra={
            "calls": [
                {"request": {"headers": ({"Cookie": "session=abc"},)}},
                [{"api_key": "k-123"}, url],
            ]
        },
    )

    text = json.dumps(lines()[0])
    assert "session=abc" not in text
    assert "k-123" not in text
    assert "s3cret" not in text
    assert "wallet:***@db" in text


def test_a_list_under_a_secret_like_key_is_redacted_whole(capture):
    logger, lines = capture

    logger.info("rotated", extra={"tokens": ["old-token", "new-token"]})

    assert lines()[0]["tokens"] == REDACTED


# --- Failure injection logs include the injected fault identifier ----------


def test_fault_block_tags_every_log_inside_it(capture):
    logger, lines = capture

    with fault("kill-wallet-before-commit"):
        logger.info("deducting")
    logger.info("after")

    inside, after = lines()
    assert inside["fault_id"] == "kill-wallet-before-commit"
    assert after["fault_id"] is None


def test_log_fault_injected_records_the_fault_and_where(capture):
    logger, lines = capture

    log_fault_injected(logger, "drop-catalog-reply", "catalog.saga.reply", attempt=2)

    [line] = lines()
    assert line["level"] == "WARNING"
    assert line["fault_id"] == "drop-catalog-reply"
    assert line["message"] == "fault injected at catalog.saga.reply"
    assert line["attempt"] == 2


def test_fault_id_from_the_environment_tags_the_whole_process(monkeypatch):
    monkeypatch.setenv("LEDGERMESH_FAULT_ID", "partition-kafka-30s")
    stream = io.StringIO()

    def run():
        configure_logging("catalog", stream=stream)
        logging.getLogger("test.logs.env").warning("degraded")

    root = logging.getLogger()
    before = list(root.handlers)
    try:
        contextvars.copy_context().run(run)
    finally:
        for handler in list(root.handlers):
            if handler not in before:
                root.removeHandler(handler)

    line = json.loads(stream.getvalue().splitlines()[-1])
    assert (line["service"], line["fault_id"]) == ("catalog", "partition-kafka-30s")


def test_configure_logging_is_idempotent():
    root = logging.getLogger()
    before = list(root.handlers)
    try:
        contextvars.copy_context().run(configure_logging, "wallet", stream=io.StringIO())
        contextvars.copy_context().run(configure_logging, "wallet", stream=io.StringIO())
        ours = [h for h in root.handlers if getattr(h, "_ledgermesh", False)]
        assert len(ours) == 1
    finally:
        for handler in list(root.handlers):
            if handler not in before:
                root.removeHandler(handler)


# --- HTTP requests are correlated -------------------------------------------


def test_middleware_binds_transaction_from_path_and_correlation_from_header():
    seen = {}
    transaction_id, correlation_id = uuid4(), uuid4()

    async def app(scope, receive, send):
        seen.update(current_context())

    scope = {
        "type": "http",
        "path": f"/v1/transactions/{transaction_id}/prepare",
        "headers": [(b"x-correlation-id", str(correlation_id).encode())],
    }
    asyncio.run(CorrelationMiddleware(app)(scope, None, None))

    assert seen == {"transaction_id": str(transaction_id), "correlation_id": str(correlation_id)}
    assert current_context() == {}


def test_middleware_reads_redemption_paths_and_ignores_other_scopes():
    seen = []
    transaction_id = uuid4()

    async def app(scope, receive, send):
        seen.append(current_context())

    asyncio.run(
        CorrelationMiddleware(app)(
            {"type": "http", "path": f"/v1/redemptions/{transaction_id}", "headers": []},
            None,
            None,
        )
    )
    asyncio.run(CorrelationMiddleware(app)({"type": "lifespan"}, None, None))

    assert seen == [{"transaction_id": str(transaction_id)}, {}]
