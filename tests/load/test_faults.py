import pytest

from tests.load.faults import FaultError, FaultSpec, parse_seconds


@pytest.mark.parametrize(
    ("text", "seconds"), [("10", 10.0), ("10s", 10.0), ("2m", 120.0), ("0.5s", 0.5), (" 3s ", 3.0)]
)
def test_parse_seconds(text, seconds):
    assert parse_seconds(text) == seconds


@pytest.mark.parametrize("text", ["", "abc", "-5s", "5h"])
def test_parse_seconds_rejects_bad_durations(text):
    with pytest.raises(FaultError):
        parse_seconds(text)


def test_fault_spec_parses_kind_target_and_timing():
    spec = FaultSpec.parse("pause:wallet@10s+5s")
    assert (spec.kind, spec.target, spec.start, spec.length) == ("pause", "wallet", 10.0, 5.0)


def test_fault_spec_round_trips_through_its_text_form():
    assert str(FaultSpec.parse("kill:coordinator@1m+30s")) == "kill:coordinator@60s+30s"


@pytest.mark.parametrize(
    "text",
    [
        "pause:wallet",  # no timing
        "pause@10s+5s",  # no target
        "explode:wallet@10s+5s",  # unknown fault
        "pause:database@10s+5s",  # unknown target
        "pause:wallet@10s",  # no length
    ],
)
def test_fault_spec_rejects_malformed_text(text):
    with pytest.raises(FaultError):
        FaultSpec.parse(text)
