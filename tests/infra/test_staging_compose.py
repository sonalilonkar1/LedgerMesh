"""infra/staging/compose.yaml: the hosted staging stack keeps its guarantees (LM-048).

Read as YAML, offline. Live proof (ports, topics, prepared transactions,
database isolation, persistence) is `infra/staging/stack.sh verify all`
against a running stack; see docs/deployment/staging.md.
"""

import importlib.util
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
STAGING = ROOT / "infra" / "staging"
COMPOSE = yaml.safe_load((STAGING / "compose.yaml").read_text())
LOCAL = yaml.safe_load((ROOT / "infra" / "compose.yaml").read_text())
SERVICES = COMPOSE["services"]

DATABASES = {
    "coordinator-db": "COORDINATOR_DB_PASSWORD",
    "wallet-db": "WALLET_DB_PASSWORD",
    "catalog-db": "CATALOG_DB_PASSWORD",
    "audit-ledger-db": "AUDIT_LEDGER_DB_PASSWORD",
}
# API -> (database service, password variable, image variable)
APIS = {
    "coordinator": ("coordinator-db", "COORDINATOR_DB_PASSWORD", "LEDGERMESH_COORDINATOR_IMAGE"),
    "wallet": ("wallet-db", "WALLET_DB_PASSWORD", "LEDGERMESH_WALLET_IMAGE"),
    "catalog": ("catalog-db", "CATALOG_DB_PASSWORD", "LEDGERMESH_CATALOG_IMAGE"),
    "audit-ledger": (
        "audit-ledger-db",
        "AUDIT_LEDGER_DB_PASSWORD",
        "LEDGERMESH_AUDIT_LEDGER_IMAGE",
    ),
}
# Worker -> (API it shares an image and database with, module it runs)
WORKERS = {
    "coordinator-saga": ("coordinator", "services.coordinator.app.saga_worker"),
    "wallet-saga": ("wallet", "services.wallet.app.saga_worker"),
    "catalog-saga": ("catalog", "services.catalog.app.saga_worker"),
    "audit-consumer": ("audit-ledger", "services.audit_ledger.app.event_worker"),
}


def test_stack_has_exactly_the_expected_services():
    assert set(SERVICES) == {"kafka", "caddy", *DATABASES, *APIS, *WORKERS}


def test_only_caddy_publishes_host_ports_and_only_tcp_80_and_443():
    published = {name for name, service in SERVICES.items() if service.get("ports")}
    assert published == {"caddy"}
    assert SERVICES["caddy"]["ports"] == [
        "${LEDGERMESH_HTTP_PORT:-80}:80/tcp",
        "${LEDGERMESH_HTTPS_PORT:-443}:443/tcp",
    ]
    for service in SERVICES.values():
        assert "network_mode" not in service
        assert "expose" not in service


@pytest.mark.parametrize("name", sorted(SERVICES))
def test_every_service_has_restart_logging_and_limits(name):
    service = SERVICES[name]
    assert service["restart"] == "unless-stopped"
    assert service["logging"]["driver"] == "json-file"
    assert service["logging"]["options"]["max-size"]
    assert service["logging"]["options"]["max-file"]
    limits = service["deploy"]["resources"]["limits"]
    assert re.fullmatch(r"\d+[mg]", limits["memory"])
    assert float(limits["cpus"]) > 0


@pytest.mark.parametrize("name", sorted(set(SERVICES) - set(WORKERS)))
def test_apis_and_infrastructure_have_health_checks(name):
    healthcheck = SERVICES[name]["healthcheck"]
    assert healthcheck["test"]
    assert not healthcheck.get("disable")


@pytest.mark.parametrize("name", sorted(WORKERS))
def test_workers_disable_the_http_health_check_they_cannot_pass(name):
    # The image HEALTHCHECK calls /health; workers have no HTTP server.
    assert SERVICES[name]["healthcheck"] == {"disable": True}


def test_kafka_has_no_external_listener_and_no_topic_auto_creation():
    env = SERVICES["kafka"]["environment"]
    assert env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"
    for key in (
        "KAFKA_LISTENERS",
        "KAFKA_ADVERTISED_LISTENERS",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
    ):
        assert "EXTERNAL" not in env[key]
    assert env["KAFKA_ADVERTISED_LISTENERS"] == "INTERNAL://kafka:19092"
    assert SERVICES["kafka"]["image"] == LOCAL["services"]["kafka"]["image"]
    # Its own fixed cluster id, never the local stack's.
    assert env["CLUSTER_ID"] != LOCAL["services"]["kafka"]["environment"]["CLUSTER_ID"]
    assert re.fullmatch(r"[A-Za-z0-9_-]{22}", env["CLUSTER_ID"])


@pytest.mark.parametrize(("name", "password"), sorted(DATABASES.items()))
def test_databases_enable_prepared_transactions_and_require_their_own_password(name, password):
    service = SERVICES[name]
    local = LOCAL["services"][name]
    assert service["image"] == local["image"]
    command = " ".join(service["command"])
    match = re.search(r"max_prepared_transactions=(\d+)", command)
    assert match and int(match.group(1)) > 0
    env = service["environment"]
    assert env["POSTGRES_USER"] == local["environment"]["POSTGRES_USER"]
    assert env["POSTGRES_DB"] == local["environment"]["POSTGRES_DB"]
    # Required, never defaulted.
    assert env["POSTGRES_PASSWORD"].startswith("${" + password + ":?")


def test_each_database_has_a_distinct_password_variable():
    variables = [
        SERVICES[name]["environment"]["POSTGRES_PASSWORD"].split(":?")[0] for name in DATABASES
    ]
    assert len(set(variables)) == len(DATABASES)


@pytest.mark.parametrize(("name", "spec"), sorted(APIS.items()))
def test_apis_run_published_images_in_staging_mode_with_their_own_database(name, spec):
    database, password, image = spec
    service = SERVICES[name]
    env = service["environment"]
    assert service["image"].startswith("${" + image + ":?")
    assert "build" not in service
    assert "command" not in service  # the image's start-service.sh
    assert env["LEDGERMESH_ENV"] == "staging"
    assert env["RUN_MIGRATIONS"] == "false"
    user = SERVICES[database]["environment"]["POSTGRES_USER"]
    db_name = SERVICES[database]["environment"]["POSTGRES_DB"]
    assert env["DATABASE_URL"] == (
        f"postgresql+psycopg://{user}:${{{password}:?{password} is required}}"
        f"@{database}:5432/{db_name}"
    )
    assert env["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:19092"


def test_coordinator_keeps_the_local_recovery_and_participant_settings():
    staging = SERVICES["coordinator"]["environment"]
    local = LOCAL["services"]["coordinator"]["environment"]
    for key in (
        "COORDINATOR_RECOVERY_ENABLED",
        "COORDINATOR_RECOVERY_INTERVAL_SECONDS",
        "WALLET_URL",
        "CATALOG_URL",
    ):
        assert staging[key] == local[key]


@pytest.mark.parametrize(("name", "spec"), sorted(WORKERS.items()))
def test_workers_run_an_existing_module_from_their_service_image(name, spec):
    api, module = spec
    service = SERVICES[name]
    assert service["command"] == ["python", "-m", module]
    assert importlib.util.find_spec(module) is not None
    assert service["image"] == SERVICES[api]["image"]
    env = service["environment"]
    assert env["DATABASE_URL"] == SERVICES[api]["environment"]["DATABASE_URL"]
    assert env["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:19092"
    assert env["LEDGERMESH_ENV"] == "staging"
    assert service["depends_on"] == {api: {"condition": "service_healthy"}}


def test_coordinator_saga_worker_can_reach_the_catalog():
    # services/coordinator/app/saga_worker.py reads CATALOG_URL.
    assert SERVICES["coordinator-saga"]["environment"]["CATALOG_URL"] == "http://catalog:8002"


def test_private_network_has_no_egress_and_caddy_cannot_reach_data():
    assert COMPOSE["networks"]["backend"] == {"internal": True}
    assert SERVICES["caddy"]["networks"] == ["edge"]
    for name in ("kafka", *DATABASES, *WORKERS):
        assert SERVICES[name]["networks"] == ["backend"], name
    for name in APIS:
        assert set(SERVICES[name]["networks"]) == {"backend", "edge"}


def test_caddy_is_pinned_by_digest_and_mounts_persistent_certificates():
    caddy = SERVICES["caddy"]
    assert re.fullmatch(r"caddy:[0-9.]+-alpine@sha256:[0-9a-f]{64}", caddy["image"])
    volumes = caddy["volumes"]
    assert "./Caddyfile:/etc/caddy/Caddyfile:ro" in volumes
    assert any(volume.endswith("/caddy/data:/data") for volume in volumes)


def test_all_data_lives_under_the_data_directory():
    mounts = {
        "kafka": "/var/lib/kafka/data",
        **{name: "/var/lib/postgresql/data" for name in DATABASES},
    }
    for name, target in mounts.items():
        (volume,) = SERVICES[name]["volumes"]
        assert volume.startswith("${LEDGERMESH_DATA_DIR:?"), name
        assert volume.endswith(":" + target), name
    assert "volumes" not in COMPOSE  # no named volumes on the boot disk


def test_kafka_uid_is_the_same_in_the_startup_script_and_stack_script():
    pattern = re.compile(r"^KAFKA_UID=(\d+)$", re.MULTILINE)
    startup = pattern.findall((STAGING / "vm-startup.sh").read_text())
    stack = pattern.findall((STAGING / "stack.sh").read_text())
    assert startup == stack and len(startup) == 1
