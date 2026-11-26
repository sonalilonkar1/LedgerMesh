"""The Prometheus, Grafana and Compose configuration for LM-034 is complete and consistent."""

import json
import re
from pathlib import Path

import pytest
import yaml

from infra.scripts.invariant_exporter import KNOWN_INVARIANTS, classify

ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = json.loads((ROOT / "infra/grafana/dashboards/ledgermesh-comparison.json").read_text())
PROMETHEUS = yaml.safe_load((ROOT / "infra/prometheus/prometheus.yml").read_text())
COMPOSE = yaml.safe_load((ROOT / "infra/compose.yaml").read_text())
METRICS_SOURCE = (ROOT / "libs/messaging/src/ledgermesh_messaging/metrics.py").read_text()

PANELS = [panel for panel in DASHBOARD["panels"] if panel["type"] != "row"]
EXPRESSIONS = [target["expr"] for panel in PANELS for target in panel["targets"]]


def panel(title_fragment: str) -> dict:
    return next(p for p in PANELS if title_fragment.lower() in p["title"].lower())


# --- Prometheus scrapes every service ----------------------------------------------


def test_prometheus_scrapes_every_service_and_worker():
    targets = {
        job["job_name"]: {target for group in job["static_configs"] for target in group["targets"]}
        for job in PROMETHEUS["scrape_configs"]
    }

    assert targets["services"] == {
        "coordinator:8000",
        "wallet:8001",
        "catalog:8002",
        "audit-ledger:8003",
    }
    assert targets["workers"] == {f"host.docker.internal:{port}" for port in range(9100, 9104)}
    assert targets["invariants"] == {"host.docker.internal:9104"}


def test_compose_runs_prometheus_and_grafana_with_provisioning():
    services = COMPOSE["services"]

    assert (
        "./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro"
        in (services["prometheus"]["volumes"])
    )
    assert "host.docker.internal:host-gateway" in services["prometheus"]["extra_hosts"]
    assert "./grafana/dashboards:/var/lib/grafana/dashboards:ro" in services["grafana"]["volumes"]


# --- Every dashboard query uses a metric that exists --------------------------------


def test_every_queried_metric_is_declared_in_the_shared_module():
    declared = set(re.findall(r'"(ledgermesh_[a-z0-9_]+)"', METRICS_SOURCE)) | {
        "ledgermesh_invariant_check_up"
    }
    used = {
        re.sub(r"_(total|bucket|count|sum)$", "", name)
        for expr in EXPRESSIONS
        for name in re.findall(r"ledgermesh_[a-z0-9_]+", expr)
    }

    assert used <= declared, used - declared


# --- The dashboard separates 2PC and Saga ---------------------------------------------


def test_redemption_panels_separate_2pc_from_saga():
    assert "mode" in panel("Redemptions per minute")["targets"][0]["expr"]
    assert "by (mode)" in panel("Success ratio")["targets"][0]["expr"]
    assert all(
        'mode="two_phase_commit"' in t["expr"] for t in panel("2PC redemption latency")["targets"]
    )
    assert all('mode="saga"' in t["expr"] for t in panel("Saga redemption latency")["targets"])


# --- p50, p95 and p99 latency are visible --------------------------------------------


@pytest.mark.parametrize(
    "title", ["2PC redemption latency", "Saga redemption latency", "2PC blocking"]
)
def test_latency_panels_show_p50_p95_and_p99(title):
    targets = panel(title)["targets"]

    assert [t["legendFormat"] for t in targets] == ["p50", "p95", "p99"]
    assert [re.search(r"histogram_quantile\((0\.\d+)", t["expr"]).group(1) for t in targets] == [
        "0.50",
        "0.95",
        "0.99",
    ]


# --- Blocking, retries, compensations, duplicates and invariants are visible ---------


@pytest.mark.parametrize(
    ("title", "metric"),
    [
        ("2PC blocking", "ledgermesh_2pc_prepared_hold_seconds_bucket"),
        ("Retries per minute", "ledgermesh_retries_total"),
        ("Dead letters", "ledgermesh_dead_letters_total"),
        ("Saga compensations", "ledgermesh_compensations_total"),
        ("Duplicates rejected", "ledgermesh_duplicates_rejected_total"),
        ("Invariant violations", "ledgermesh_invariant_violations"),
    ],
)
def test_each_required_signal_has_a_panel(title, metric):
    assert metric in panel(title)["targets"][0]["expr"]


def test_panels_do_not_overlap():
    cells = set()
    for item in DASHBOARD["panels"]:
        grid = item["gridPos"]
        for x in range(grid["x"], grid["x"] + grid["w"]):
            for y in range(grid["y"], grid["y"] + grid["h"]):
                assert (x, y) not in cells, f"{item['title']} overlaps another panel"
                cells.add((x, y))
    assert len({p["id"] for p in DASHBOARD["panels"]}) == len(DASHBOARD["panels"])


# --- Invariant exporter ------------------------------------------------------------------


def test_invariant_violations_are_classified_with_every_invariant_present():
    counts = classify(
        [
            "INV-01: 2 wallet balances are negative",
            "INV-05: committed 1234 is incomplete",
            "INV-05: committed 5678 is incomplete",
            "wallet: 1 prepared transactions leaked",
        ]
    )

    assert set(KNOWN_INVARIANTS) <= set(counts)
    assert counts["INV-01"] == 1
    assert counts["INV-05"] == 2
    assert counts["prepared_leak"] == 1
    assert counts["INV-02"] == 0


def test_in_flight_redemptions_are_not_reported_as_live_violations():
    # INV-07 is a post-test check: every in-flight redemption would trip it.
    counts = classify(
        ["INV-07: 3 redemptions remain non-terminal after the test: saga DEDUCTING_POINTS=3"]
    )

    assert "INV-07" not in counts
    assert sum(counts.values()) == 0
