import json
from datetime import datetime

import pytest
from ledgermesh_messaging import EnvelopeError, EventEnvelope

from .conftest import make_envelope


def test_round_trip_preserves_every_field():
    envelope = make_envelope(aggregate_version=3, workflow_step=1, fencing_token=7)
    assert EventEnvelope.from_bytes(envelope.to_bytes()) == envelope


def test_rejects_unknown_fields():
    raw = make_envelope().to_json() | {"unexpected": True}
    with pytest.raises(EnvelopeError):
        EventEnvelope.from_bytes(json.dumps(raw).encode())


def test_rejects_missing_transaction_id():
    raw = make_envelope().to_json()
    del raw["transaction_id"]
    with pytest.raises(ValueError):
        EventEnvelope.model_validate(raw, context={"wire": True})


def test_rejects_negative_version():
    with pytest.raises(ValueError):
        make_envelope(aggregate_version=-1)


def test_rejects_naive_timestamp():
    with pytest.raises(ValueError):
        make_envelope(occurred_at=datetime(2026, 11, 2, 12, 0))


def test_rejects_unsupported_schema_version():
    raw = make_envelope().to_json()
    raw["schema_version"] = 2
    with pytest.raises(ValueError):
        EventEnvelope.model_validate(raw, context={"wire": True})


@pytest.mark.parametrize(
    "event_type", ["PointsDeducted", "points_deducted", "wallet.PointsDeducted"]
)
def test_rejects_noncanonical_event_type(event_type):
    with pytest.raises(ValueError):
        make_envelope(event_type=event_type)


@pytest.mark.parametrize("missing_field", ["workflow_step", "causation_id"])
def test_wire_decode_requires_nullable_keys_to_be_present(missing_field):
    raw = make_envelope().to_json()
    del raw[missing_field]
    with pytest.raises(EnvelopeError, match=missing_field):
        EventEnvelope.from_bytes(json.dumps(raw).encode())


@pytest.mark.parametrize("raw", [None, b"", b"not json", b'{"event_type": "wallet.test"}'])
def test_from_bytes_wraps_bad_input(raw):
    with pytest.raises(EnvelopeError):
        EventEnvelope.from_bytes(raw)
