"""Every service serves /metrics for Prometheus, with its HTTP latency (LM-034)."""

import importlib

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


@pytest.mark.parametrize("module", SERVICES.values())
def test_metrics_route_is_not_part_of_the_public_api_contract(module):
    app = importlib.import_module(module).app

    assert "/metrics" not in app.openapi()["paths"]
