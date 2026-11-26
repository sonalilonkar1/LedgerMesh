"""Every service serves /metrics for Prometheus, with its HTTP latency (LM-034)."""

import importlib
import re
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

SERVICES = {
    "coordinator": "services.coordinator.app.main",
    "wallet": "services.wallet.app.main",
    "catalog": "services.catalog.app.main",
    "audit-ledger": "services.audit_ledger.app.main",
}


@pytest.mark.parametrize(("service", "module"), SERVICES.items())
def test_service_exposes_prometheus_metrics_including_its_own_requests(service, module):
    client = TestClient(importlib.import_module(module).app)

    assert client.get("/health").status_code == 200
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    # Label order in the exposition depends on the prometheus_client version,
    # so compare the label sets rather than the raw text.
    expected = {
        f'service="{service}"',
        'method="GET"',
        'route="/health"',
        'status="200"',
    }
    counts = [
        set(line.split("{", 1)[1].split("}", 1)[0].split(","))
        for line in response.text.splitlines()
        if line.startswith("ledgermesh_http_request_duration_seconds_count{")
    ]
    assert expected in counts


def route_labels(text: str, service: str) -> set[str]:
    labels = set()
    for line in text.splitlines():
        if line.startswith("ledgermesh_http_request_duration_seconds_count{"):
            found = dict(re.findall(r'(\w+)="([^"]*)"', line.split("}", 1)[0]))
            if found.get("service") == service:
                labels.add(found["route"])
    return labels


@pytest.mark.parametrize(("service", "module"), SERVICES.items())
def test_unknown_paths_are_labelled_unmatched_not_by_their_raw_path(service, module):
    client = TestClient(importlib.import_module(module).app)
    unknown = [f"/unknown/{uuid4()}", f"/unknown/{uuid4()}", "/no/such/route"]

    for path in unknown:
        assert client.get(path).status_code == 404
    routes = route_labels(client.get("/metrics").text, service)

    assert "unmatched" in routes
    assert not any(path in routes for path in unknown)
    assert all(route == "unmatched" or route.startswith("/") for route in routes)


@pytest.mark.parametrize("module", SERVICES.values())
def test_metrics_route_is_not_part_of_the_public_api_contract(module):
    app = importlib.import_module(module).app

    assert "/metrics" not in app.openapi()["paths"]
