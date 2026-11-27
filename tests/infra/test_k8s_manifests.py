"""Static checks for the Kubernetes manifests (LM-041).

The manifests must run the same processes, with the same settings, as Docker
Compose (infra/compose.yaml) and the Makefile worker targets. These tests render
the kind overlay with ``kubectl kustomize`` and compare. They are skipped when
``kubectl`` is not installed.
"""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[2]
K8S = ROOT / "infra" / "k8s"
COMPOSE = yaml.safe_load((ROOT / "infra" / "compose.yaml").read_text())

API_SERVICES = {
    # Kubernetes/Compose name -> (python package, port)
    "coordinator": ("coordinator", 8000),
    "wallet": ("wallet", 8001),
    "catalog": ("catalog", 8002),
    "audit-ledger": ("audit_ledger", 8003),
}
WORKERS = {
    # Deployment -> (image, Makefile target)
    "coordinator-saga": ("coordinator", "run-coordinator-saga"),
    "wallet-saga": ("wallet", "run-wallet-saga"),
    "catalog-saga": ("catalog", "run-catalog-saga"),
    "audit-consumer": ("audit-ledger", "run-audit-consumer"),
}


@pytest.fixture(scope="module")
def rendered(tmp_path_factory) -> list[dict]:
    if shutil.which("kubectl") is None:
        pytest.skip("kubectl is not installed")
    copy = tmp_path_factory.mktemp("k8s") / "k8s"
    shutil.copytree(K8S, copy, ignore=shutil.ignore_patterns(".env.secrets"))
    (copy / "overlays" / "kind" / ".env.secrets").write_text("POSTGRES_PASSWORD=0123abcd\n")
    result = subprocess.run(
        ["kubectl", "kustomize", str(copy / "overlays" / "kind")],
        capture_output=True,
        text=True,
        check=True,
    )
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def find(docs: list[dict], kind: str, name: str) -> dict:
    matches = [d for d in docs if d["kind"] == kind and d["metadata"]["name"] == name]
    assert len(matches) == 1, f"expected one {kind}/{name}"
    return matches[0]


def pod_specs(docs: list[dict]):
    for doc in docs:
        if doc["kind"] in {"Deployment", "StatefulSet", "Job"}:
            yield doc["metadata"]["name"], doc["spec"]["template"]["spec"]


def env_of(container: dict) -> dict[str, str]:
    return {e["name"]: e.get("value") for e in container.get("env", [])}


def config(docs: list[dict], name: str) -> dict[str, str]:
    return find(docs, "ConfigMap", name)["data"]


def test_everything_is_in_the_ledgermesh_namespace(rendered):
    for doc in rendered:
        if doc["kind"] != "Namespace":
            assert doc["metadata"]["namespace"] == "ledgermesh", doc["metadata"]["name"]


def test_every_pod_disables_service_links_and_token(rendered):
    # A Service named "kafka" would inject KAFKA_PORT into the broker's config.
    for name, spec in pod_specs(rendered):
        assert spec["enableServiceLinks"] is False, name
        assert spec["automountServiceAccountToken"] is False, name


def test_application_containers_are_unprivileged_and_read_only(rendered):
    for name, spec in pod_specs(rendered):
        for container in spec.get("initContainers", []) + spec["containers"]:
            security = container["securityContext"]
            assert security["allowPrivilegeEscalation"] is False, name
            assert security["capabilities"]["drop"] == ["ALL"], name
            if container["image"].startswith("ledgermesh-"):
                assert spec["securityContext"]["runAsUser"] == 10001, name
                assert spec["securityContext"]["runAsNonRoot"] is True, name
                assert security["readOnlyRootFilesystem"] is True, name


def test_api_ports_probes_and_database_urls_match_compose(rendered):
    for name, (package, port) in API_SERVICES.items():
        compose = COMPOSE["services"][name]
        assert compose["ports"][0].endswith(f":{port}")
        service = find(rendered, "Service", name)
        assert service["spec"]["ports"][0]["port"] == port
        spec = find(rendered, "Deployment", name)["spec"]["template"]["spec"]
        container = spec["containers"][0]
        assert container["image"] == f"ledgermesh-{name}:local"
        assert "command" not in container  # the image's start-service.sh
        assert container["ports"][0]["containerPort"] == port
        for probe in ("startupProbe", "readinessProbe", "livenessProbe"):
            assert container[probe]["httpGet"]["path"] == "/health"
        env = env_of(container)
        assert env["RUN_MIGRATIONS"] == "false"
        expected_url = compose["environment"]["DATABASE_URL"].replace(
            "${POSTGRES_PASSWORD:-ledgermesh_dev}", "$(POSTGRES_PASSWORD)"
        )
        assert env["DATABASE_URL"] == expected_url
        migrate = spec["initContainers"][0]
        assert f"alembic -c services/{package}/alembic.ini upgrade head" in migrate["command"][2]
        assert env_of(migrate)["DATABASE_URL"] == expected_url


def test_shared_config_matches_compose(rendered):
    data = config(rendered, "ledgermesh-config")
    compose = COMPOSE["services"]["coordinator"]["environment"]
    for key in (
        "KAFKA_BOOTSTRAP_SERVERS",
        "WALLET_URL",
        "CATALOG_URL",
        "COORDINATOR_RECOVERY_ENABLED",
        "COORDINATOR_RECOVERY_INTERVAL_SECONDS",
        "LEDGERMESH_ENV",
    ):
        assert data[key] == str(compose[key]), key


def test_base_leaves_internal_routes_disabled():
    text = (K8S / "base" / "config.yaml").read_text()
    assert not re.search(r"^\s*LEDGERMESH_ENV:", text, re.MULTILINE)


def test_kafka_config_matches_compose(rendered):
    data = config(rendered, "kafka-config")
    compose = COMPOSE["services"]["kafka"]["environment"]
    changed = {"KAFKA_CONTROLLER_QUORUM_VOTERS", "KAFKA_ADVERTISED_LISTENERS"}
    for key, value in compose.items():
        if key not in changed:
            assert data[key] == str(value), key
    assert data["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"
    assert data["KAFKA_ADVERTISED_LISTENERS"].startswith("INTERNAL://kafka:19092,")
    kafka = find(rendered, "StatefulSet", "kafka")["spec"]["template"]["spec"]["containers"][0]
    assert kafka["image"] == COMPOSE["services"]["kafka"]["image"]
    # The image declares VOLUME /var/lib/kafka/data: the PVC must be mounted
    # exactly there (compose.yaml mounts kafka_data there too), or the broker
    # writes to an ephemeral volume and loses its topics on restart.
    compose_mount = COMPOSE["services"]["kafka"]["volumes"][0].split(":", 1)[1]
    assert [m["mountPath"] for m in kafka["volumeMounts"]] == [compose_mount]
    assert data["KAFKA_LOG_DIRS"] == compose_mount


def test_databases_match_compose(rendered):
    for name in ("coordinator-db", "wallet-db", "catalog-db", "audit-ledger-db"):
        compose = COMPOSE["services"][name]
        container = find(rendered, "StatefulSet", name)["spec"]["template"]["spec"]["containers"][0]
        assert container["image"] == compose["image"]
        assert container["args"] == compose["command"]
        env = env_of(container)
        assert env["POSTGRES_USER"] == compose["environment"]["POSTGRES_USER"]
        assert env["POSTGRES_DB"] == compose["environment"]["POSTGRES_DB"]


def test_workers_run_the_makefile_modules(rendered):
    makefile = (ROOT / "Makefile").read_text()
    for name, (image, target) in WORKERS.items():
        recipe = makefile.split(f"\n{target}:\n", 1)[1].split("\n\n", 1)[0]
        module = re.search(r"python -m (\S+)", recipe).group(1)
        deployment = find(rendered, "Deployment", name)
        assert deployment["spec"]["replicas"] == 1
        assert deployment["spec"]["strategy"]["type"] == "Recreate"
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        assert container["image"] == f"ledgermesh-{image}:local"
        assert container["command"] == ["python", "-m", module]
        assert "DATABASE_URL" in env_of(container)


def test_coordinator_saga_worker_gets_catalog_url(rendered):
    # saga_worker.py defaults CATALOG_URL to localhost, which is wrong in a pod.
    container = find(rendered, "Deployment", "coordinator-saga")["spec"]["template"]["spec"][
        "containers"
    ][0]
    refs = [source["configMapRef"]["name"] for source in container.get("envFrom", [])]
    assert "ledgermesh-config" in refs


def test_topics_are_created_by_the_project_bootstrap(rendered):
    job = find(rendered, "Job", "kafka-topics")
    command = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
    assert "ledgermesh-topics --bootstrap-servers kafka:19092 --replication-factor 1" in command


def test_no_secret_values_are_committed(rendered):
    secrets = [doc for doc in rendered if doc["kind"] == "Secret"]
    assert [s["metadata"]["name"].startswith("ledgermesh-db-") for s in secrets] == [True]
    for path in K8S.rglob("*"):
        if path.is_file() and path.suffix in {".yaml", ".yml"}:
            assert not re.search(r"^kind:\s*Secret\b", path.read_text(), re.MULTILINE), path
    tracked = subprocess.run(
        ["git", "ls-files", "infra/k8s"], cwd=ROOT, capture_output=True, text=True, check=False
    ).stdout.split()
    assert not [path for path in tracked if path.endswith(".env.secrets")]
