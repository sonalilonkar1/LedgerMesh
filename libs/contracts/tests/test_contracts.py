import importlib.util
import json
import sys
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from openapi_spec_validator import validate

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
CONTRACTS_ROOT = REPOSITORY_ROOT / "libs" / "contracts"
SCHEMA_PATH = CONTRACTS_ROOT / "schemas" / "events" / "event-envelope.v1.json"
EXAMPLES_ROOT = CONTRACTS_ROOT / "examples" / "events"

# LM-019 owns the runtime messaging implementation. Load only envelope.py instead
# of importing the package initializer, which also imports optional Kafka and
# database runtime dependencies that contract-only CI does not need.
ENVELOPE_PATH = (
    REPOSITORY_ROOT / "libs" / "messaging" / "src" / "ledgermesh_messaging" / "envelope.py"
)
ENVELOPE_MODULE_NAME = "ledgermesh_runtime_envelope"
ENVELOPE_SPEC = importlib.util.spec_from_file_location(ENVELOPE_MODULE_NAME, ENVELOPE_PATH)
if ENVELOPE_SPEC is None or ENVELOPE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load runtime envelope from {ENVELOPE_PATH}")
ENVELOPE_MODULE = importlib.util.module_from_spec(ENVELOPE_SPEC)
sys.modules[ENVELOPE_MODULE_NAME] = ENVELOPE_MODULE
ENVELOPE_SPEC.loader.exec_module(ENVELOPE_MODULE)
EventEnvelope = ENVELOPE_MODULE.EventEnvelope


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def envelope_validator() -> Draft202012Validator:
    schema = load_json(SCHEMA_PATH)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


@pytest.mark.parametrize("spec_path", sorted((CONTRACTS_ROOT / "openapi").glob("*.yaml")))
def test_openapi_document_is_valid(spec_path: Path):
    validate(yaml.safe_load(spec_path.read_text()))


@pytest.mark.parametrize(
    "example_path",
    [EXAMPLES_ROOT / "valid-command.v1.json", EXAMPLES_ROOT / "valid-event.v1.json"],
)
def test_valid_event_examples_match_schema(example_path: Path):
    envelope_validator().validate(load_json(example_path))


def test_malformed_event_is_rejected():
    with pytest.raises(ValidationError):
        envelope_validator().validate(
            load_json(EXAMPLES_ROOT / "invalid-missing-transaction-id.v1.json")
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", "not-a-uuid"),
        ("aggregate_version", -1),
        ("occurred_at", "2026-11-05T01:00:00"),
    ],
)
def test_invalid_envelope_values_are_rejected(field: str, value):
    message = load_json(EXAMPLES_ROOT / "valid-event.v1.json")
    message[field] = value
    with pytest.raises(ValidationError):
        envelope_validator().validate(message)


def test_unknown_envelope_metadata_is_rejected():
    message = load_json(EXAMPLES_ROOT / "valid-event.v1.json")
    message["unexpected"] = True
    with pytest.raises(ValidationError):
        envelope_validator().validate(message)


def test_runtime_envelope_fields_match_json_schema():
    schema_fields = set(load_json(SCHEMA_PATH)["properties"])
    assert set(EventEnvelope.model_fields) == schema_fields


def test_runtime_created_envelope_matches_json_schema():
    message = EventEnvelope(
        event_type="wallet.points_deducted",
        transaction_id="aa594328-beba-41d5-b41d-2155839ff479",
        correlation_id="4b7f8f68-991d-46ce-bfad-3709e448640d",
        aggregate_type="wallet",
        aggregate_id="user-123",
    )
    envelope_validator().validate(message.to_json())


@pytest.mark.parametrize(
    "example_path",
    [EXAMPLES_ROOT / "valid-command.v1.json", EXAMPLES_ROOT / "valid-event.v1.json"],
)
def test_runtime_envelope_accepts_schema_examples(example_path: Path):
    EventEnvelope.model_validate(load_json(example_path), context={"wire": True})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", 2),
        ("event_type", "PointsDeducted"),
        ("event_id", "not-a-uuid"),
        ("aggregate_version", -1),
        ("occurred_at", "not-a-dateZ"),
    ],
)
def test_schema_and_runtime_reject_the_same_invalid_values(field: str, value):
    message = load_json(EXAMPLES_ROOT / "valid-event.v1.json")
    message[field] = value

    with pytest.raises(ValidationError):
        envelope_validator().validate(message)
    with pytest.raises(ValueError):
        EventEnvelope.model_validate(message, context={"wire": True})


@pytest.mark.parametrize("missing_field", ["workflow_step", "causation_id"])
def test_schema_and_runtime_require_all_wire_keys(missing_field: str):
    message = load_json(EXAMPLES_ROOT / "valid-event.v1.json")
    del message[missing_field]

    with pytest.raises(ValidationError):
        envelope_validator().validate(message)
    with pytest.raises(ValueError):
        EventEnvelope.model_validate(message, context={"wire": True})
