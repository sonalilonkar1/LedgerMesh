"""Shared Prometheus metrics and their recording helpers (LM-034). No Docker needed."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from prometheus_client import REGISTRY

from ledgermesh_messaging import (
    DeadLetterReason,
    EventEnvelope,
    FailureRouter,
    MetricsMiddleware,
    PermanentError,
    RetryPolicy,
    TransientError,
    metrics_on_commit,
    record_invariant_violations,
    record_prepared_hold,
    record_transition,
    render_metrics,
    start_metrics_server,
)


def value(name: str, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


# --- Only committed transitions are counted ---------------------------------


def transitions(state: str, mode: str = "saga") -> float:
    return value("ledgermesh_transitions_total", mode=mode, state=state)


def test_metrics_inside_a_completed_block_are_published_when_it_ends():
    before = transitions("POINTS_DEDUCTED")

    with metrics_on_commit():
        record_transition(mode="saga", from_state="DEDUCTING_POINTS", to_state="POINTS_DEDUCTED")
        assert transitions("POINTS_DEDUCTED") == before  # not yet: still uncommitted

    assert transitions("POINTS_DEDUCTED") == before + 1


def test_metrics_inside_a_failed_block_are_discarded():
    labels = {"mode": "saga", "outcome": "COMPENSATED"}
    outcomes = value("ledgermesh_redemptions_total", **labels)
    compensated = transitions("COMPENSATED")

    with pytest.raises(RuntimeError), metrics_on_commit():
        record_transition(mode="saga", from_state="COMPENSATING", to_state="COMPENSATED")
        raise RuntimeError("a later statement failed; the transaction rolls back")

    assert transitions("COMPENSATED") == compensated
    assert value("ledgermesh_redemptions_total", **labels) == outcomes


def test_a_retry_after_a_rollback_is_counted_once():
    before = transitions("RESERVING_INVENTORY")

    for attempt in (1, 2):
        try:
            with metrics_on_commit():
                record_transition(
                    mode="saga", from_state="POINTS_DEDUCTED", to_state="RESERVING_INVENTORY"
                )
                if attempt == 1:
                    raise TransientError("rolled back, will retry")
        except TransientError:
            continue

    assert transitions("RESERVING_INVENTORY") == before + 1


def test_nested_blocks_publish_only_when_the_outermost_one_completes():
    before = transitions("FAILED")

    with pytest.raises(RuntimeError), metrics_on_commit():
        with metrics_on_commit():
            record_transition(mode="saga", from_state="REQUESTED", to_state="FAILED")
        assert transitions("FAILED") == before  # the inner block ended, the outer did not
        raise RuntimeError("the outer transaction rolls back")

    assert transitions("FAILED") == before


def test_outside_any_block_metrics_publish_immediately():
    before = transitions("PREPARING", mode="two_phase_commit")

    record_transition(mode="two_phase_commit", from_state="REQUESTED", to_state="PREPARING")

    assert transitions("PREPARING", mode="two_phase_commit") == before + 1


# --- Coordinator outcomes, latency and compensations -----------------------


def test_a_terminal_state_counts_the_outcome_and_its_duration():
    labels = {"mode": "saga", "outcome": "COMPLETED"}
    count_before = value("ledgermesh_redemptions_total", **labels)
    observed_before = value("ledgermesh_redemption_duration_seconds_count", **labels)
    sum_before = value("ledgermesh_redemption_duration_seconds_sum", **labels)
    created = datetime.now(UTC) - timedelta(seconds=2)

    record_transition(
        mode="saga",
        from_state="RESERVING_INVENTORY",
        to_state="COMPLETED",
        created_at=created,
        now=created.timestamp() + 2,
    )

    assert value("ledgermesh_redemptions_total", **labels) == count_before + 1
    assert value("ledgermesh_redemption_duration_seconds_count", **labels) == observed_before + 1
    assert value("ledgermesh_redemption_duration_seconds_sum", **labels) == pytest.approx(
        sum_before + 2
    )


def test_a_non_terminal_state_only_counts_the_transition():
    before = value("ledgermesh_redemptions_total", mode="two_phase_commit", outcome="PREPARING")
    transitions = value("ledgermesh_transitions_total", mode="two_phase_commit", state="PREPARING")

    record_transition(mode="two_phase_commit", from_state="REQUESTED", to_state="PREPARING")

    assert value("ledgermesh_transitions_total", mode="two_phase_commit", state="PREPARING") == (
        transitions + 1
    )
    assert (
        value("ledgermesh_redemptions_total", mode="two_phase_commit", outcome="PREPARING")
        == before
    )


def test_compensations_count_first_attempts_and_retries_separately():
    first = value("ledgermesh_compensations_total", mode="saga", attempt="first")
    retry = value("ledgermesh_compensations_total", mode="saga", attempt="retry")

    record_transition(mode="saga", from_state="RESERVING_INVENTORY", to_state="COMPENSATING")
    record_transition(mode="saga", from_state="COMPENSATING", to_state="COMPENSATING")

    assert value("ledgermesh_compensations_total", mode="saga", attempt="first") == first + 1
    assert value("ledgermesh_compensations_total", mode="saga", attempt="retry") == retry + 1


# --- Blocking -----------------------------------------------------------------


def test_prepared_hold_time_is_observed_per_service_and_decision():
    labels = {"service": "wallet", "decision": "commit"}
    before = value("ledgermesh_2pc_prepared_hold_seconds_sum", **labels)
    prepared = datetime.now(UTC)

    record_prepared_hold(
        service="wallet", decision="commit", prepared_at=prepared, now=prepared.timestamp() + 1.5
    )

    assert value("ledgermesh_2pc_prepared_hold_seconds_sum", **labels) == pytest.approx(
        before + 1.5
    )


# --- Retries and dead letters ---------------------------------------------------


class FakeProducer:
    async def send_and_wait(self, topic, **kwargs):
        pass


def wallet_record():
    envelope = EventEnvelope(
        event_type="wallet.deduct_points",
        transaction_id=uuid4(),
        correlation_id=uuid4(),
        aggregate_type="wallet",
        aggregate_id="user-1",
        workflow_step=1,
    )
    return SimpleNamespace(
        topic="wallet.commands",
        partition=0,
        offset=1,
        key=b"user-1",
        value=envelope.to_bytes(),
        headers=[],
    )


def test_router_counts_retry_topic_trips_and_dead_letters_by_reason():
    router = FailureRouter(FakeProducer(), consumer="metrics-test", policy=RetryPolicy())
    retries = value("ledgermesh_retries_total", consumer="metrics-test", tier="retry_topic")
    parked = value(
        "ledgermesh_dead_letters_total", consumer="metrics-test", reason="permanent_error"
    )

    asyncio.run(router.retry_or_dead_letter(wallet_record(), TransientError()))
    asyncio.run(
        router.dead_letter(wallet_record(), PermanentError(), DeadLetterReason.PERMANENT_ERROR)
    )

    assert value("ledgermesh_retries_total", consumer="metrics-test", tier="retry_topic") == (
        retries + 1
    )
    assert value(
        "ledgermesh_dead_letters_total", consumer="metrics-test", reason="permanent_error"
    ) == (parked + 1)


# --- Invariants -------------------------------------------------------------------


def test_invariant_violations_are_published_per_invariant():
    record_invariant_violations({"INV-01": 0, "INV-05": 2})

    assert value("ledgermesh_invariant_violations", invariant="INV-01") == 0
    assert value("ledgermesh_invariant_violations", invariant="INV-05") == 2

    record_invariant_violations({"INV-05": 0})
    assert value("ledgermesh_invariant_violations", invariant="INV-05") == 0


# --- HTTP latency and exposition -------------------------------------------------


def run_request(app, path, method="GET"):
    sent = []

    async def send(message):
        sent.append(message)

    async def receive():
        return {"type": "http.request", "body": b""}

    scope = {"type": "http", "method": method, "path": path, "headers": []}
    asyncio.run(app(scope, receive, send))
    return sent


def test_distinct_unmatched_paths_share_one_route_label():
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 404, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    labels = {"service": "wallet", "method": "GET", "route": "unmatched", "status": "404"}
    before = value("ledgermesh_http_request_duration_seconds_count", **labels)
    paths = [
        f"/unknown/{uuid4()}",
        f"/unknown/{uuid4().hex}",
        "/v1/transactions/not-an-id/prepare",
        "/wp-admin/setup.php",
        "/unknown/value-1",
        "/unknown/value-2",
    ]

    for path in paths:
        run_request(MetricsMiddleware(app, service="wallet"), path)

    assert value("ledgermesh_http_request_duration_seconds_count", **labels) == before + len(paths)
    exposition = render_metrics()[0].decode()
    for path in paths:
        assert path not in exposition


def test_middleware_prefers_the_framework_route_template():
    async def app(scope, receive, send):
        scope["route"] = SimpleNamespace(path="/v1/wallets/{user_id}")
        await send({"type": "http.response.start", "status": 200, "headers": []})

    labels = {
        "service": "wallet",
        "method": "GET",
        "route": "/v1/wallets/{user_id}",
        "status": "200",
    }
    before = value("ledgermesh_http_request_duration_seconds_count", **labels)

    run_request(MetricsMiddleware(app, service="wallet"), "/v1/wallets/user-42")

    assert value("ledgermesh_http_request_duration_seconds_count", **labels) == before + 1


def test_an_unhandled_error_is_recorded_as_500():
    async def app(scope, receive, send):
        raise RuntimeError("boom")

    labels = {"service": "catalog", "method": "GET", "route": "unmatched", "status": "500"}
    before = value("ledgermesh_http_request_duration_seconds_count", **labels)

    with pytest.raises(RuntimeError):
        run_request(MetricsMiddleware(app, service="catalog"), "/boom")

    assert value("ledgermesh_http_request_duration_seconds_count", **labels) == before + 1


def test_render_metrics_exposes_every_shared_metric():
    body, content_type = render_metrics()
    text = body.decode()

    assert content_type.startswith("text/plain")
    for name in (
        "ledgermesh_redemptions_total",
        "ledgermesh_redemption_duration_seconds",
        "ledgermesh_transitions_total",
        "ledgermesh_compensations_total",
        "ledgermesh_2pc_prepared_hold_seconds",
        "ledgermesh_retries_total",
        "ledgermesh_dead_letters_total",
        "ledgermesh_duplicates_rejected_total",
        "ledgermesh_message_handling_seconds",
        "ledgermesh_http_request_duration_seconds",
        "ledgermesh_invariant_violations",
    ):
        assert f"# TYPE {name.removesuffix('_total')}" in text


def test_the_worker_metrics_server_can_be_disabled(monkeypatch):
    monkeypatch.setenv("LEDGERMESH_METRICS_PORT", "0")

    assert start_metrics_server(9101) is None
