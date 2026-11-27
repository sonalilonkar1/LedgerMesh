"""The local stack sends every service's traces to Jaeger (LM-033)."""

from pathlib import Path

import yaml

COMPOSE = yaml.safe_load((Path(__file__).parents[2] / "infra" / "compose.yaml").read_text())
SERVICES = ("coordinator", "wallet", "catalog", "audit-ledger")


def test_jaeger_runs_a_pinned_image_and_accepts_otlp():
    jaeger = COMPOSE["services"]["jaeger"]

    image, _, tag = jaeger["image"].partition(":")
    assert image == "jaegertracing/all-in-one"
    assert tag and tag != "latest"
    assert jaeger["environment"]["COLLECTOR_OTLP_ENABLED"] == "true"
    assert jaeger["networks"] == ["backend"]


def test_only_the_jaeger_ui_is_published():
    ports = COMPOSE["services"]["jaeger"]["ports"]

    assert ports == ["${JAEGER_UI_PORT:-16686}:16686"]


def test_every_service_exports_traces_to_jaeger_on_the_private_network():
    for name in SERVICES:
        environment = COMPOSE["services"][name]["environment"]
        assert environment["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://jaeger:4318", name
        assert "backend" in COMPOSE["services"][name]["networks"], name
