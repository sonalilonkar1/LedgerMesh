"""infra/staging/deploy.sh, offline (LM-049).

A fake ``docker`` keeps a small host state in JSON (registry, local images and
running containers), and a fake ``stack.sh`` in the deployed bundle plays the
LM-048 bring-up: it records its phases, "starts" the containers with the
images deploy.sh exported, and fails in a chosen phase for a chosen image.
deploy.sh itself, render-env.sh and the release records are the real ones.
"""

import json
import os
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path

import pytest

from .conftest import install

ROOT = Path(__file__).resolve().parents[2]
STAGING = ROOT / "infra" / "staging"
REGISTRY = "ghcr.io/sonalilonkar1"
SERVICES = ["coordinator", "wallet", "catalog", "audit-ledger"]
CONTAINERS = {
    "coordinator": "coordinator",
    "wallet": "wallet",
    "catalog": "catalog",
    "audit-ledger": "audit-ledger",
    "coordinator-saga": "coordinator",
    "wallet-saga": "wallet",
    "catalog-saga": "catalog",
    "audit-consumer": "audit-ledger",
}
PASSWORDS = {
    "COORDINATOR_DB_PASSWORD": "coordinator-test-pw-0123456789",  # gitleaks:allow
    "WALLET_DB_PASSWORD": "wallet-test-pw-0123456789",  # gitleaks:allow
    "CATALOG_DB_PASSWORD": "catalog.test~pw-0123456789",
    "AUDIT_LEDGER_DB_PASSWORD": "audit_ledger-test-pw-0123456789",  # gitleaks:allow
}
A = "a" * 40
B = "b" * 40
C = "c" * 40
MISSING = "d" * 40


def digest_of(sha: str, service: str) -> str:
    return "sha256:" + (sha[:8] + format(SERVICES.index(service), "x") * 56)[:64]


FAKE_DOCKER = f"""#!{sys.executable} -IS
import json, os, sys

state_path = os.environ["FAKE_STATE"]
state = json.load(open(state_path))
args = sys.argv[1:]
with open(os.environ["FAKE_DOCKER_LOG"], "a") as log:
    log.write(json.dumps(args) + "\\n")


def save():
    json.dump(state, open(state_path, "w"), indent=1)


def find_image(ref):
    if ref in state["images"]:
        return state["images"][ref]
    for image in state["images"].values():
        if ref in image.get("digests", []):
            return image
    return None


def show(obj, template, kind):
    if template == "{{{{.Id}}}}":
        print(obj["id"])
    elif "RepoDigests" in template:
        for item in obj.get("digests", []):
            print(item)
    elif "com.docker.compose.service" in template:
        print(obj["service"], obj["image"], obj["status"], obj["revision"])
    elif "LEDGERMESH_REVISION" in template:
        print(obj["revision"])
    else:
        sys.exit(98)


if args[0] == "pull":
    ref = args[-1]
    entry = state["registry"].get(ref)
    if entry is None:
        for key, value in state["registry"].items():
            if key.rsplit(":", 1)[0] + "@" + value["digest"] == ref:
                entry = value
                ref = key
    if entry is None:
        print("Error response from daemon: manifest unknown", file=sys.stderr)
        sys.exit(1)
    repo = ref.rsplit(":", 1)[0]
    state["images"][ref] = {{
        "id": "sha256:id-" + entry["digest"][7:19],
        "digests": [repo + "@" + entry["digest"]],
        "revision": entry["revision"],
    }}
    save()
    sys.exit(0)

if args[:2] == ["image", "inspect"] or args[0] == "inspect":
    rest = args[2:] if args[0] == "image" else args[1:]
    template = None
    if rest[0] == "-f":
        template, rest = rest[1], rest[2:]
    ref = rest[0]
    if args[0] == "inspect" and ref in state["containers"]:
        for name in rest:
            show(state["containers"][name], template, "container")
        sys.exit(0)
    image = find_image(ref)
    if image is None:
        print("Error: No such image: " + ref, file=sys.stderr)
        sys.exit(1)
    if template is None:
        print("[{{}}]")
    else:
        show(image, template, "image")
    sys.exit(0)

if args[:2] == ["image", "rm"]:
    ref = args[2]
    if ref in state["images"]:
        del state["images"][ref]
    else:
        for image in state["images"].values():
            if ref in image.get("digests", []):
                image["digests"].remove(ref)
    save()
    sys.exit(0)

if args[0] == "ps":
    filters = [args[i + 1] for i, a in enumerate(args) if a == "--filter"]
    services = [f.rsplit("=", 1)[1] for f in filters if "compose.service" in f]
    for name, container in state["containers"].items():
        if not services or container["service"] in services:
            print(name)
    sys.exit(0)

sys.exit(97)
"""

# The bring-up, as stack.sh would do it, against the fake docker state.
FAKE_STACK = f"""#!{sys.executable} -IS
import json, os, sys, tarfile, io

args = sys.argv[1:]
with open(os.environ["FAKE_STACK_LOG"], "a") as log:
    log.write(json.dumps({{
        "args": args,
        "bundle": os.path.dirname(sys.argv[0]),
        "images": {{
            k: v for k, v in os.environ.items()
            if k.startswith("LEDGERMESH_") and k.endswith("_IMAGE")
        }},
    }}) + "\\n")

def value(flag):
    return args[args.index(flag) + 1] if flag in args else None

command = [a for a in args if a in ("up", "collect-logs", "verify")][0]
state_path = os.environ["FAKE_STATE"]
state = json.load(open(state_path))
images = {{
    "coordinator": os.environ.get("LEDGERMESH_COORDINATOR_IMAGE", ""),
    "wallet": os.environ.get("LEDGERMESH_WALLET_IMAGE", ""),
    "catalog": os.environ.get("LEDGERMESH_CATALOG_IMAGE", ""),
    "audit-ledger": os.environ.get("LEDGERMESH_AUDIT_LEDGER_IMAGE", ""),
}}

if command == "collect-logs":
    out = args[args.index("collect-logs") + 1]
    data = b"service log line\\n"
    with tarfile.open(out, "w:gz") as bundle:
        info = tarfile.TarInfo("logs/coordinator.log")
        info.size = len(data)
        bundle.addfile(info, io.BytesIO(data))
    sys.exit(0)
if command == "verify":
    print("ok  coordinator-db max_prepared_transactions=100 prepared_now=0 oldest_age_s=0")
    sys.exit(0)

fail_phase = os.environ.get("FAKE_FAIL_PHASE", "")
fail_image = os.environ.get("FAKE_FAIL_IMAGE", "")
phases = [
    "data-tier", "prepared-transactions", "migrations", "topics",
    "services", "workers", "verify", "complete",
]
for phase in phases:
    if phase == "migrations" and "--skip-migrations" in args:
        continue
    open(value("--phase-file"), "w").write(phase + "\\n")
    # The bring-up output a deploy log keeps; never a password.
    print("stack: phase: " + phase, file=sys.stderr)
    if phase == fail_phase and fail_image and any(fail_image in ref for ref in images.values()):
        print("stack: failing in " + phase, file=sys.stderr)
        sys.exit(1)
    if phase == "services":
        for name, image_service in {json.dumps(CONTAINERS)}.items():
            ref = images[image_service]
            image = state["images"].get(ref)
            if image is None:
                image = next(i for i in state["images"].values() if ref in i.get("digests", []))
            state["containers"]["lmtest-" + name + "-1"] = {{
                "service": name,
                "image": ref,
                "status": "running",
                "revision": image["revision"],
                "password": {json.dumps(PASSWORDS["WALLET_DB_PASSWORD"])},
            }}
        json.dump(state, open(state_path, "w"), indent=1)
"""


class Host:
    """A fake staging host with a registry; runs the real deploy.sh."""

    def __init__(self, tmp_path: Path):
        self.tmp = tmp_path
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        install(self.bin / "docker", FAKE_DOCKER)
        self.state_path = tmp_path / "state.json"
        self.state = {"registry": {}, "images": {}, "containers": {}}
        self.save()
        self.docker_log = tmp_path / "docker.log"
        self.stack_log = tmp_path / "stack.log"
        self.data = tmp_path / "data"
        self.link = tmp_path / "opt" / "staging"
        self.env_file = tmp_path / "etc" / "staging.env"
        self.source = tmp_path / "source.env"
        self.source.write_text(
            "LEDGERMESH_SITE_ADDRESS=34-105-1-2.sslip.io\n"
            "LEDGERMESH_TLS=ops@example.com\n" + "".join(f"{k}={v}\n" for k, v in PASSWORDS.items())
        )
        self.source.chmod(0o600)
        # The bundle the workflow uploads: infra/staging with a fake stack.sh.
        self.bundle = tmp_path / "bundle"
        self.bundle.mkdir()
        for name in ("deploy.sh", "render-env.sh", "compose.yaml", "Caddyfile"):
            shutil.copy(STAGING / name, self.bundle / name)
        install(self.bundle / "fake_stack.py", FAKE_STACK)
        (self.bundle / "stack.sh").write_text(
            '#!/usr/bin/env bash\nexec "$(dirname "$0")/fake_stack.py" "$@"\n'
        )

    def save(self):
        self.state_path.write_text(json.dumps(self.state))

    def load(self):
        self.state = json.loads(self.state_path.read_text())
        return self.state

    def publish(self, sha: str, revision: str | None = None):
        self.load()
        for service in SERVICES:
            self.state["registry"][f"{REGISTRY}/ledgermesh-{service}:{sha}"] = {
                "digest": digest_of(sha, service),
                "revision": revision or sha,
            }
        self.save()

    def build_local(self, sha: str, image_id: str = "local"):
        self.load()
        for service in SERVICES:
            self.state["images"][f"{REGISTRY}/ledgermesh-{service}:{sha}"] = {
                "id": f"sha256:{image_id}-{service}-{sha[:6]}",
                "digests": [],
                "revision": sha,
            }
        self.save()

    def run(self, *args: str, env: dict[str, str] | None = None, options=()):
        return subprocess.run(
            [
                "bash",
                str(self.bundle / "deploy.sh"),
                "--env-file",
                str(self.env_file),
                "--secrets-source",
                f"file:{self.source}",
                "--data-dir",
                str(self.data),
                "--project-name",
                "lmtest",
                "--current-link",
                str(self.link),
                *options,
                *args,
            ],
            env={
                **os.environ,
                "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
                "FAKE_STATE": str(self.state_path),
                "FAKE_DOCKER_LOG": str(self.docker_log),
                "FAKE_STACK_LOG": str(self.stack_log),
                **(env or {}),
            },
            capture_output=True,
            text=True,
            check=False,
        )

    def deploy(self, sha: str, *extra: str, env=None, options=()):
        return self.run("deploy", sha, "--actor", "Deven-0012", *extra, env=env, options=options)

    def release(self, sha: str) -> dict:
        return json.loads((self.data / "releases" / sha / "release.json").read_text())

    def pointer(self, name: str) -> str:
        path = self.data / "releases" / name
        return path.read_text().strip() if path.exists() else ""

    def history(self) -> list[dict]:
        path = self.data / "releases" / "history.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()]

    def running_revisions(self) -> set[str]:
        return {c["revision"] for c in self.load()["containers"].values()}

    def stack_calls(self) -> list[dict]:
        if not self.stack_log.exists():
            return []
        return [json.loads(line) for line in self.stack_log.read_text().splitlines()]

    def docker_calls(self) -> list[list[str]]:
        return [json.loads(line) for line in self.docker_log.read_text().splitlines()]


@pytest.fixture
def host(tmp_path):
    return Host(tmp_path)


def up_calls(host: Host) -> list[dict]:
    return [call for call in host.stack_calls() if "up" in call["args"]]


# ---- successful deploys ----------------------------------------------------------


def test_deploy_pins_digests_records_the_release_and_promotes(host):
    host.publish(A)
    digests = ",".join(f"{s}={digest_of(A, s)}" for s in SERVICES)

    result = host.deploy(
        A, "--run-url", "https://github.com/o/r/actions/runs/1", "--digests", digests
    )

    assert result.returncode == 0, result.stderr
    record = host.release(A)
    assert record["status"] == "promoted"
    assert record["sha"] == A
    assert record["actor"] == "Deven-0012"
    assert record["url"] == "https://34-105-1-2.sslip.io"
    assert record["run_url"] == "https://github.com/o/r/actions/runs/1"
    assert record["attempts"] == 1
    assert record["started_at"] <= record["finished_at"]
    assert record["digests"] == {s: digest_of(A, s) for s in SERVICES}
    assert record["images"] == {s: f"{REGISTRY}/ledgermesh-{s}@{digest_of(A, s)}" for s in SERVICES}
    assert host.pointer("current") == A
    assert host.pointer("last-known-good") == A
    assert [e["event"] for e in host.history()] == ["deploy_started", "healthy", "promoted"]
    assert host.running_revisions() == {A}
    # The stack ran the digest-pinned images, with migrations and topics.
    (call,) = up_calls(host)
    assert call["images"]["LEDGERMESH_WALLET_IMAGE"] == record["images"]["wallet"]
    assert "--skip-migrations" not in call["args"]
    # The bundle is kept with the release and linked as the current one.
    assert (host.data / "releases" / A / "bundle" / "compose.yaml").exists()
    assert host.link.resolve() == (host.data / "releases" / A / "bundle").resolve()
    env_mode = host.env_file.stat().st_mode & 0o777
    assert env_mode == 0o600


def test_redeploying_the_same_sha_is_idempotent(host):
    host.publish(A)
    assert host.deploy(A).returncode == 0
    first = host.release(A)

    result = host.deploy(A)

    assert result.returncode == 0, result.stderr
    record = host.release(A)
    assert record["attempts"] == 2
    assert record["status"] == "promoted"
    assert record["images"] == first["images"]
    assert host.pointer("last-known-good") == A
    assert host.running_revisions() == {A}


def test_no_promote_leaves_the_previous_release_last_known_good(host):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0

    result = host.deploy(B, "--no-promote")

    assert result.returncode == 0, result.stderr
    assert host.release(B)["status"] == "healthy"
    assert host.pointer("current") == B
    assert host.pointer("last-known-good") == A

    promoted = host.run("promote", B)
    assert promoted.returncode == 0, promoted.stderr
    assert host.pointer("last-known-good") == B
    assert host.release(B)["status"] == "promoted"


def test_deploy_refuses_a_bundle_from_another_commit(host):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0
    (host.bundle / ".bundle-revision").write_text(C + "\n")

    wrong = host.deploy(B)

    assert wrong.returncode == 1
    assert f"infra/staging at {C}, not {B}" in wrong.stderr
    assert not (host.data / "releases" / B).exists()

    # An installed release bundle (/opt/ledgermesh/staging) can redeploy its
    # own commit, but never another one.
    (host.bundle / ".bundle-revision").unlink()
    host.bundle = host.data / "releases" / A / "bundle"
    assert host.deploy(A).returncode == 0
    refused = host.deploy(B)
    assert refused.returncode == 1
    assert "another release's bundle" in refused.stderr
    assert host.running_revisions() == {A}


# ---- nothing changes when the images are wrong -------------------------------------


@pytest.mark.parametrize(
    ("prepare", "extra", "message"),
    [
        (lambda h: None, (), "could not pull"),
        (lambda h: h.publish(MISSING, revision=C), (), "built from revision"),
        (
            lambda h: h.publish(MISSING),
            ("--digests", ",".join(f"{s}={digest_of(C, s)}" for s in SERVICES)),
            "registry lookup said",
        ),
        (lambda h: h.publish(MISSING), ("--digests", "coordinator=sha256:abc"), "no valid digest"),
    ],
    ids=["missing", "wrong-revision", "digest-mismatch", "bad-digest-list"],
)
def test_bad_images_change_nothing(host, prepare, extra, message):
    host.publish(A)
    assert host.deploy(A).returncode == 0
    prepare(host)
    stack_calls_before = len(host.stack_calls())

    result = host.deploy(MISSING, *extra)

    assert result.returncode == 10, result.stderr
    assert message in result.stderr
    assert "Nothing was changed" in result.stderr or "no valid digest" in result.stderr
    assert len(host.stack_calls()) == stack_calls_before
    assert not (host.data / "releases" / MISSING).exists()
    assert host.pointer("current") == A
    assert host.running_revisions() == {A}


# ---- failures roll back to the last known good release --------------------------------


@pytest.mark.parametrize("phase", ["migrations", "topics", "services"])
def test_failure_in_any_phase_restores_last_known_good(host, phase):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0

    result = host.deploy(B, env={"FAKE_FAIL_PHASE": phase, "FAKE_FAIL_IMAGE": B[:8]})

    assert result.returncode == 20, result.stderr
    record = host.release(B)
    assert record["status"] == "failed_rolled_back"
    assert record["failed_phase"] == phase
    assert record["rolled_back_to"] == A
    assert host.pointer("current") == A
    assert host.pointer("last-known-good") == A
    assert host.running_revisions() == {A}
    # The rollback used A's own bundle and pinned images, without migrations or topics.
    rollback = up_calls(host)[-1]
    assert "--skip-migrations" in rollback["args"]
    assert "--skip-topics" in rollback["args"]
    assert rollback["images"]["LEDGERMESH_CATALOG_IMAGE"] == host.release(A)["images"]["catalog"]
    assert rollback["bundle"] == str(host.data / "releases" / A / "bundle")
    # The failing release's logs were kept before the rollback.
    failed_bundles = list((host.data / "deploy-logs").glob(f"{B}-*-failed.tgz"))
    assert len(failed_bundles) == 1
    events = [e["event"] for e in host.history()]
    assert events[-3:] == ["deploy_failed", "rollback_started", "rolled_back"]


def test_revision_mismatch_after_bring_up_fails_and_rolls_back(host, tmp_path):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0
    # The stack "starts" B but one container keeps running A's image.
    stack = host.bundle / "fake_stack.py"
    stack.write_text(
        stack.read_text().replace(
            '        json.dump(state, open(state_path, "w"), indent=1)',
            '        if images["wallet"].find("bbbbbbbb") >= 0:\n'
            '            state["containers"]["lmtest-wallet-saga-1"]["revision"] = "x"\n'
            '        json.dump(state, open(state_path, "w"), indent=1)',
        )
    )

    result = host.deploy(B)

    assert result.returncode == 20, result.stderr
    assert host.release(B)["failed_phase"] == "revision"
    assert "FAIL wallet-saga" in result.stderr
    assert host.running_revisions() == {A}


def test_first_deploy_failure_has_nothing_to_restore(host):
    host.publish(A)

    result = host.deploy(A, env={"FAKE_FAIL_PHASE": "services", "FAKE_FAIL_IMAGE": A[:8]})

    assert result.returncode == 22, result.stderr
    assert host.release(A)["status"] == "failed_no_rollback"
    assert host.pointer("last-known-good") == ""


def test_rollback_after_an_env_failure_reuses_the_env_file(host):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0

    # Secrets unavailable: rendering the env file fails, nothing has changed.
    host.source.unlink()
    result = host.deploy(B)

    assert result.returncode == 20, result.stderr
    assert host.release(B)["status"] == "failed_rolled_back"
    assert "reusing the existing env file" in result.stderr
    assert host.running_revisions() == {A}
    assert host.pointer("current") == A


def test_failed_rollback_exits_21(host):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0

    # Every bring-up now fails in `services`, including the rollback.
    result = host.deploy(B, env={"FAKE_FAIL_PHASE": "services", "FAKE_FAIL_IMAGE": "ghcr.io/"})

    assert result.returncode == 21, result.stderr
    assert host.release(B)["status"] == "rollback_failed"
    assert "ROLLBACK TO" in result.stderr
    assert host.history()[-1]["event"] == "rollback_failed"


# ---- rollback command -----------------------------------------------------------------


def test_rollback_after_a_smoke_failure(host):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0
    assert host.deploy(B, "--no-promote").returncode == 0

    result = host.run("rollback", "last-known-good", "--after-failed", B)

    assert result.returncode == 0, result.stderr
    assert host.running_revisions() == {A}
    assert host.pointer("current") == A
    assert host.release(B)["status"] == "smoke_failed_rolled_back"
    assert "--skip-migrations" in up_calls(host)[-1]["args"]


def test_rollback_after_the_last_known_good_itself_failed_smoke_says_so(host):
    # An idempotent redeploy of the live, last-known-good A failed smoke.
    host.publish(A)
    assert host.deploy(A).returncode == 0
    assert host.deploy(A, "--no-promote").returncode == 0
    status_before = host.release(A)["status"]

    result = host.run("rollback", "last-known-good", "--after-failed", A)

    assert result.returncode == 0, result.stderr
    assert "is itself the last-known-good release; nothing older is restored" in result.stderr
    assert host.pointer("current") == host.pointer("last-known-good") == A
    assert host.release(A)["status"] == status_before
    assert host.release(A)["status"] != "smoke_failed_rolled_back"
    assert host.history()[-1]["event"] == "smoke_failed_on_last_known_good"


def test_rollback_when_the_deploy_already_restored_is_a_no_op(host):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0
    failed = host.deploy(B, env={"FAKE_FAIL_PHASE": "topics", "FAKE_FAIL_IMAGE": B[:8]})
    assert failed.returncode == 20
    calls = len(up_calls(host))

    result = host.run("rollback", "last-known-good", "--after-failed", B)

    assert result.returncode == 0, result.stderr
    assert "already running" in result.stderr
    assert len(up_calls(host)) == calls
    assert host.release(B)["status"] == "failed_rolled_back"


def test_manual_rollback_to_previous_and_to_a_sha(host):
    for sha in (A, B, C):
        host.publish(sha)
        assert host.deploy(sha).returncode == 0

    assert host.run("resolve", "previous").stdout.strip() == B
    result = host.run("rollback", "previous")
    assert result.returncode == 0, result.stderr
    assert host.running_revisions() == {B}
    assert host.pointer("last-known-good") == B

    result = host.run("rollback", A)
    assert result.returncode == 0, result.stderr
    assert host.running_revisions() == {A}
    assert host.pointer("current") == A
    assert host.pointer("last-known-good") == A


def test_rollback_re_pulls_pruned_images_by_digest(host):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0
    assert host.deploy(B).returncode == 0
    state = host.load()
    state["images"] = {k: v for k, v in state["images"].items() if A not in k}
    host.save()

    result = host.run("rollback", A)

    assert result.returncode == 0, result.stderr
    pulls = [call[-1] for call in host.docker_calls() if call[0] == "pull"]
    assert f"{REGISTRY}/ledgermesh-wallet@{digest_of(A, 'wallet')}" in pulls
    assert host.running_revisions() == {A}


@pytest.mark.parametrize(
    ("target", "code"),
    [(MISSING, 3), ("previous", 3), ("not-a-sha", 1)],
)
def test_rollback_refuses_unknown_targets(host, target, code):
    host.publish(A)
    assert host.deploy(A).returncode == 0

    result = host.run("rollback", target)

    assert result.returncode == code, result.stderr
    assert host.running_revisions() == {A}


def test_rollback_refuses_a_release_that_was_never_good(host):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0
    assert (
        host.deploy(B, env={"FAKE_FAIL_PHASE": "verify", "FAKE_FAIL_IMAGE": B[:8]}).returncode == 20
    )

    result = host.run("rollback", B)

    assert result.returncode == 3
    assert "never a last-known-good" in result.stderr


def test_promote_refuses_a_failed_or_other_release(host):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0
    assert (
        host.deploy(B, env={"FAKE_FAIL_PHASE": "verify", "FAKE_FAIL_IMAGE": B[:8]}).returncode == 20
    )

    result = host.run("promote", B)
    assert result.returncode != 0
    assert "refusing to promote" in result.stderr
    assert host.pointer("last-known-good") == A


# ---- status, logs, lock, retention, local images ----------------------------------------


def test_status_expect(host):
    host.publish(A)
    assert host.deploy(A).returncode == 0

    ok = host.run("status", "--expect", A)
    assert ok.returncode == 0, ok.stderr
    assert f"current_sha={A}" in ok.stdout
    assert "ok  wallet-saga running" in ok.stdout
    assert "max_prepared_transactions" in ok.stdout

    wrong = host.run("status", "--expect", B)
    assert wrong.returncode != 0
    assert "expected" in wrong.stderr

    state = host.load()
    state["containers"]["lmtest-catalog-1"]["status"] = "exited"
    host.save()
    broken = host.run("status", "--expect", A)
    assert broken.returncode != 0
    assert "FAIL catalog" in broken.stdout


def test_no_password_reaches_output_records_or_the_log_bundle(host, tmp_path):
    host.publish(A)
    host.publish(B)
    assert host.deploy(A).returncode == 0
    failed = host.deploy(B, env={"FAKE_FAIL_PHASE": "verify", "FAKE_FAIL_IMAGE": B[:8]})
    assert failed.returncode == 20
    status = host.run("status")
    out = tmp_path / "logs.tgz"

    result = host.run("logs", str(out), "--sha", B)

    assert result.returncode == 0, result.stderr
    with tarfile.open(out) as bundle:
        names = [m.name for m in bundle.getmembers() if m.isfile()]
        text = "".join(
            bundle.extractfile(m).read().decode(errors="replace")
            for m in bundle.getmembers()
            if m.isfile()
        )
    assert any(name.endswith(f"releases/{B}.json") for name in names)
    assert any("history.jsonl" in name for name in names)
    assert any(f"{B}-" in name and name.endswith(".log") for name in names)
    assert any(name.endswith("logs/coordinator.log") for name in names)
    everything = [failed.stdout, failed.stderr, status.stdout, status.stderr, text]
    for path in (host.data / "releases").rglob("*"):
        if path.is_file() and "bundle" not in path.parts:
            everything.append(path.read_text(errors="replace"))
    for path in (host.data / "deploy-logs").glob("*.log"):
        everything.append(path.read_text())
    for value in PASSWORDS.values():
        for item in everything:
            assert value not in item


def test_a_running_deploy_holds_the_lock_and_a_stale_lock_is_cleared(host):
    host.publish(A)
    lock = host.data / "releases" / ".lock"
    lock.mkdir(parents=True)
    (lock / "pid").write_text(str(os.getpid()))  # this test process is alive

    busy = host.deploy(A, options=("--lock-timeout", "2"))
    assert busy.returncode == 1
    assert "is still running after 2s" in busy.stderr
    assert lock.exists()

    (lock / "pid").write_text("999999")
    result = host.deploy(A)
    assert result.returncode == 0, result.stderr
    assert "stale lock" in result.stderr
    assert not lock.exists()


def test_a_lock_that_never_got_a_pid_is_stale_after_a_minute(host):
    host.publish(A)
    lock = host.data / "releases" / ".lock"
    lock.mkdir(parents=True)
    old = time.time() - 120
    os.utime(lock, (old, old))

    result = host.deploy(A, options=("--lock-timeout", "5"))
    assert result.returncode == 0, result.stderr
    assert "no pid after a minute" in result.stderr


def test_overlapping_invocations_queue_instead_of_failing(host):
    # Another deploy.sh (e.g. `fixtures` from a manual smoke run) holds the
    # lock for about two seconds and then releases it, as release_lock does.
    host.publish(A)
    lock = host.data / "releases" / ".lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    holder = subprocess.Popen(
        ["bash", "-c", 'mkdir "$1" && echo $$ >"$1/pid" && sleep 2 && rm -rf "$1"', "_", str(lock)]
    )
    try:
        deadline = time.monotonic() + 10
        while not (lock / "pid").exists():
            assert time.monotonic() < deadline, "the holder never took the lock"
            time.sleep(0.05)
        result = host.deploy(A, options=("--lock-timeout", "60"))
    finally:
        holder.wait(timeout=30)

    assert holder.returncode == 0
    assert result.returncode == 0, result.stderr
    assert "waiting up to 60s" in result.stderr
    assert "stale lock" not in result.stderr
    assert host.pointer("current") == A
    assert not lock.exists()


def test_old_release_images_are_removed_but_never_current_or_last_known_good(host):
    shas = [format(i, "x") * 40 for i in range(1, 6)]
    for sha in shas:
        host.publish(sha)
        assert host.deploy(sha, options=("--keep", "2")).returncode == 0

    removed = {call[2] for call in host.docker_calls() if call[:2] == ["image", "rm"]}
    for sha in shas[:3]:
        assert f"{REGISTRY}/ledgermesh-wallet:{sha}" in removed
        assert host.release(sha)["images_pruned"] == "true"
    for sha in shas[3:]:
        assert not any(sha in ref for ref in removed)


def test_local_images_are_used_by_tag_and_checked_on_rollback(host):
    options = ("--image-source", "local")
    host.build_local(A)
    host.build_local(B)
    assert host.deploy(A, options=options).returncode == 0
    assert host.deploy(B, options=options).returncode == 0
    assert not [call for call in host.docker_calls() if call[0] == "pull"]
    record = host.release(A)
    assert record["images"]["wallet"] == f"{REGISTRY}/ledgermesh-wallet:{A}"
    assert record["image_ids"]["wallet"].startswith("sha256:local-wallet")

    host.build_local(A, image_id="rebuilt")  # the tag now points at another image
    result = host.run("rollback", A, options=options)

    assert result.returncode == 21
    assert "not the recorded" in result.stderr
    assert host.running_revisions() == {B}


@pytest.mark.parametrize(
    "args",
    [
        ["deploy", "abc"],
        ["deploy", A, "--actor", "bad actor"],
        ["deploy", A, "--run-url", "http://x"],
        ["status", "--expect", "nope"],
        ["--image-source", "s3", "status"],
        ["--data-dir", "relative", "status"],
    ],
)
def test_rejects_unsafe_arguments(host, args):
    result = host.run(*args)
    assert result.returncode != 0
