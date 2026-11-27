"""The deploy workflow's helper scripts, offline (LM-049).

- ci/gate.sh decides whether a run may deploy, against a real temporary git
  repository, a fake ``gh`` (the triggering run's jobs) and a fake digest lookup.
- ci/remote.sh uploads the bundle and runs deploy.sh on the VM; a fake
  ``gcloud`` plays the VM by running the remote command locally, and a fake
  ``sudo`` stands in for deploy.sh.
- ci/github-deployment.sh sends the right deployment API requests.
"""

import hashlib
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from .conftest import install

ROOT = Path(__file__).resolve().parents[2]
CI = ROOT / "infra" / "staging" / "ci"
IMAGES = ["coordinator", "wallet", "catalog", "audit-ledger"]


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def commit(repo: Path, path: str, message: str) -> str:
    (repo / path).parent.mkdir(parents=True, exist_ok=True)
    (repo / path).write_text(message)
    git(repo, "add", path)
    git(repo, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-qm", message)
    return git(repo, "rev-parse", "HEAD")


def digest(image: str) -> str:
    return "sha256:" + hashlib.sha256(image.encode()).hexdigest()


@pytest.mark.parametrize("script", sorted(CI.glob("*.sh")), ids=lambda path: path.name)
def test_ci_script_parses_and_fails_closed(script):
    assert subprocess.run(["bash", "-n", str(script)], check=False).returncode == 0
    text = script.read_text()
    assert text.startswith("#!/usr/bin/env bash\n")
    assert "set -euo pipefail" in text
    assert "|| true" not in text
    assert os.access(script, os.X_OK), f"{script.name} is not executable"


# ---- gate.sh -------------------------------------------------------------------------


@pytest.fixture
def gate(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    old = commit(repo, "README.md", "before LM-049")
    good = commit(repo, "infra/staging/deploy.sh", "deploy tooling")
    git(repo, "update-ref", "refs/remotes/origin/main", good)
    git(repo, "checkout", "-q", "-b", "feature")
    feature = commit(repo, "infra/staging/deploy.sh", "unmerged change")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    install(
        bin_dir / "gh",
        f"""#!{sys.executable}
import json, os, sys
assert sys.argv[1:3] == ["api", "--paginate"], sys.argv
assert sys.argv[3].endswith("/actions/runs/" + os.environ["RUN_ID"] + "/jobs"), sys.argv
for name, conclusion in json.loads(os.environ["FAKE_JOBS"]):
    print(name + "\\t" + conclusion)
""",
    )
    lookup = tmp_path / "ghcr_digest.sh"
    lookup.write_text(
        "#!/usr/bin/env bash\n"
        'image="${1#*/}"\n'
        'if [[ "${FAKE_DIGEST_STATUS:-0}" != 0 ]]; then exit "$FAKE_DIGEST_STATUS"; fi\n'
        f"{sys.executable} -c 'import hashlib, sys; "
        'print("sha256:" + hashlib.sha256(sys.argv[1].encode()).hexdigest())\' '
        '"${image#ledgermesh-}"\n'
    )

    def run(event: str, **env: str):
        output = tmp_path / "output"
        output.write_text("")
        result = subprocess.run(
            ["bash", str(CI / "gate.sh")],
            cwd=repo,
            env={
                **os.environ,
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "GITHUB_OUTPUT": str(output),
                "GITHUB_REPOSITORY": "sonalilonkar1/LedgerMesh",
                "GITHUB_REF": "refs/heads/main",
                "EVENT": event,
                "LEDGERMESH_GHCR_DIGEST": str(lookup),
                **env,
            },
            capture_output=True,
            text=True,
            check=False,
        )
        outputs = dict(
            line.split("=", 1) for line in output.read_text().splitlines() if "=" in line
        )
        return result, outputs

    run.old, run.good, run.feature = old, good, feature
    return run


def published(conclusion: str = "success", images=IMAGES):
    jobs = [[f"publish / publish ledgermesh-{image}", conclusion] for image in images]
    return json.dumps([*jobs, ["check", "skipped"], ["release ${{ github.ref_name }}", "skipped"]])


def triggered(gate, **overrides: str) -> dict[str, str]:
    env = {
        "RUN_ID": "45898392727",
        "RUN_CONCLUSION": "success",
        "RUN_EVENT": "workflow_run",
        "RUN_BRANCH": "main",
        "RUN_REPO": "sonalilonkar1/LedgerMesh",
        "RUN_SHA": gate.good,
        "FAKE_JOBS": published(),
    }
    env.update(overrides)
    return env


def test_gate_deploys_a_published_commit_on_main(gate):
    result, outputs = gate("workflow_run", **triggered(gate))

    assert result.returncode == 0, result.stderr
    assert outputs["deploy"] == "true"
    assert outputs["sha"] == gate.good
    assert outputs["digests"] == ",".join(f"{i}={digest(i)}" for i in IMAGES)


@pytest.mark.parametrize(
    "overrides",
    [
        {"RUN_CONCLUSION": "failure"},
        {"RUN_EVENT": "pull_request"},
        {"RUN_BRANCH": "feature"},
        {"RUN_REPO": "someone/LedgerMesh"},
        {"FAKE_JOBS": published("skipped")},
    ],
    ids=["images-failed", "pull-request", "other-branch", "fork", "core-ci-failed"],
)
def test_gate_skips_runs_that_published_nothing_for_main(gate, overrides):
    result, outputs = gate("workflow_run", **triggered(gate, **overrides))

    assert result.returncode == 0, result.stderr
    assert outputs == {"deploy": "false"}
    assert "Not deploying" in result.stdout


@pytest.mark.parametrize(
    ("event", "overrides", "message"),
    [
        ("workflow_run", {"FAKE_JOBS": published(images=IMAGES[:3])}, "only 3 of 4"),
        ("workflow_run", {"FAKE_JOBS": published("failure")}, "concluded 'failure'"),
        ("workflow_run", {"RUN_SHA": "feature"}, "invalid head SHA"),
        ("workflow_run", {"FAKE_DIGEST_STATUS": "3"}, "never published"),
        ("workflow_run", {"FAKE_DIGEST_STATUS": "1"}, "could not look up"),
        ("workflow_dispatch", {"GITHUB_REF": "refs/heads/feature"}, "only run from main"),
        ("workflow_dispatch", {"INPUT_SHA": "abc123"}, "40-character"),
        ("push", {}, "does not handle"),
    ],
)
def test_gate_fails_closed(gate, event, overrides, message):
    env = triggered(gate, **overrides) if event == "workflow_run" else overrides
    if event == "workflow_dispatch":
        env.setdefault("INPUT_SHA", gate.good)
    result, outputs = gate(event, **env)

    assert result.returncode == 1
    assert message in result.stderr
    assert "deploy" not in outputs


def test_gate_dispatch_requires_a_commit_on_main_with_the_deploy_tooling(gate):
    ok, outputs = gate("workflow_dispatch", INPUT_SHA=gate.good)
    assert ok.returncode == 0, ok.stderr
    assert outputs["sha"] == gate.good

    unmerged, _ = gate("workflow_dispatch", INPUT_SHA=gate.feature)
    assert unmerged.returncode == 1
    assert "is not on main" in unmerged.stderr

    before, _ = gate("workflow_dispatch", INPUT_SHA=gate.old)
    assert before.returncode == 1
    assert "predates LM-049" in before.stderr

    unknown, _ = gate("workflow_dispatch", INPUT_SHA="f" * 40)
    assert unknown.returncode == 1
    assert "is not a commit" in unknown.stderr


# ---- remote.sh -------------------------------------------------------------------------

# Fake gcloud: `compute scp` copies between here and FAKE_HOME (the VM user's
# home); `compute ssh --command` runs the command with bash in FAKE_HOME.
FAKE_GCLOUD = f"""#!{sys.executable}
import json, os, shutil, subprocess, sys

args = sys.argv[1:]
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps(args) + "\\n")
home = os.environ["FAKE_HOME"]
vm = os.environ["FAKE_VM"]
if args[:2] == ["compute", "scp"]:
    paths = [a for a in args[2:] if not a.startswith("--") and a not in
             (args[args.index(f) + 1] for f in ("--project", "--zone", "--ssh-key-expire-after"))]
    src, dest = paths[-2], paths[-1]
    if dest.startswith(vm + ":"):
        shutil.copy(src, os.path.join(home, dest.split(":", 1)[1]))
    else:
        shutil.copy(os.path.join(home, src.split(":", 1)[1]), dest)
    sys.exit(0)
if args[:2] == ["compute", "ssh"]:
    command = args[args.index("--command") + 1]
    sys.exit(subprocess.run(["bash", "-c", command], cwd=home,
                            env={{**os.environ, "HOME": home}}).returncode)
sys.exit(97)
"""

# Fake sudo on the "VM": records deploy.sh's arguments, checks the bundle was
# unpacked next to it, and writes a log bundle for `logs`.
FAKE_SUDO = f"""#!{sys.executable}
import json, os, sys, tarfile
args = sys.argv[1:]
assert args[0] == "bash" and args[1].endswith("/deploy.sh"), args
bundle = os.path.dirname(args[1])
with open(os.environ["FAKE_SUDO_LOG"], "a") as log:
    log.write(json.dumps({{"args": args[2:], "files": sorted(os.listdir(bundle))}}) + "\\n")
if args[2] == "logs":
    with tarfile.open(args[3], "w:gz") as out:
        out.add(args[1], arcname="deploy.sh")
sys.exit(int(os.environ.get("FAKE_REMOTE_EXIT", "0")))
"""


# GNU sha256sum as on the Debian VM and the Ubuntu runner (macOS's lacks --quiet).
FAKE_SHA256SUM = f"""#!{sys.executable}
import hashlib, sys

def digest(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()

args = sys.argv[1:]
if args[:2] == ["--check", "--quiet"]:
    for line in sys.stdin.read().splitlines():
        expected, path = line.split("  ", 1)
        if digest(path) != expected:
            print(path + ": FAILED", file=sys.stderr)
            sys.exit(1)
    sys.exit(0)
for path in args:
    print(digest(path) + "  " + path)
"""


@pytest.fixture
def remote(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    install(bin_dir / "gcloud", FAKE_GCLOUD)
    install(bin_dir / "sudo", FAKE_SUDO)
    install(bin_dir / "sha256sum", FAKE_SHA256SUM)
    home = tmp_path / "home"
    home.mkdir()
    log = tmp_path / "gcloud.log"
    sudo_log = tmp_path / "sudo.log"

    def run(*args: str, **env: str):
        result = subprocess.run(
            [
                "bash",
                str(CI / "remote.sh"),
                "--project",
                "deven-cloud-lab",
                "--zone",
                "us-west1-b",
                *args,
            ],
            env={
                **os.environ,
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "FAKE_HOME": str(home),
                "FAKE_VM": "ledgermesh-staging",
                "FAKE_LOG": str(log),
                "FAKE_SUDO_LOG": str(sudo_log),
                **env,
            },
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=False,
        )
        calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        sudo = (
            [json.loads(line) for line in sudo_log.read_text().splitlines()]
            if sudo_log.exists()
            else []
        )
        return result, calls, sudo

    run.home = home
    return run


HEAD = subprocess.run(
    ["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
).stdout.strip()


def test_remote_deploy_uploads_that_commits_bundle_over_iap(remote):
    result, calls, sudo = remote(
        "deploy",
        HEAD,
        "--actor",
        "Deven-0012",
        "--run-url",
        "https://github.com/sonalilonkar1/LedgerMesh/actions/runs/1",
        "--digests",
        ",".join(f"{i}={digest(i)}" for i in IMAGES),
        "--no-promote",
    )

    assert result.returncode == 0, result.stderr
    assert [call[:2] for call in calls] == [["compute", "scp"], ["compute", "ssh"]]
    for call in calls:
        assert "--tunnel-through-iap" in call
        assert call[call.index("--project") + 1] == "deven-cloud-lab"
        assert call[call.index("--zone") + 1] == "us-west1-b"
        assert call[call.index("--ssh-key-expire-after") + 1] == "1h"
    (ran,) = sudo
    assert ran["args"][:2] == ["deploy", HEAD]
    assert ran["args"][-1] == "--no-promote"
    # The bundle is infra/staging as committed, unpacked on the "VM".
    assert {"deploy.sh", "stack.sh", "compose.yaml", "Caddyfile"} <= set(ran["files"])
    assert ".env.local" not in ran["files"]
    # Nothing is left in the VM user's home.
    assert list(remote.home.iterdir()) == []


@pytest.mark.parametrize("code", ["10", "20", "21", "22"])
def test_remote_returns_deploy_sh_exit_status(remote, code):
    result, _, _ = remote("deploy", HEAD, FAKE_REMOTE_EXIT=code)
    assert result.returncode == int(code)


def test_remote_refuses_a_tampered_bundle(remote, tmp_path):
    # A gcloud whose upload is corrupted: the checksum on the VM must fail.
    corrupt = tmp_path / "bin" / "gcloud"
    corrupt.write_text(
        FAKE_GCLOUD.replace(
            '        shutil.copy(src, os.path.join(home, dest.split(":", 1)[1]))',
            "        open(os.path.join(home, dest.split(\":\", 1)[1]), 'wb').write(b'tampered')",
        )
    )
    result, _, sudo = remote("status")
    assert result.returncode != 0
    assert sudo == []


def test_remote_logs_copies_the_bundle_back(remote, tmp_path):
    out = tmp_path / "logs.tgz"
    result, calls, sudo = remote("logs", str(out), "--sha", HEAD)

    assert result.returncode == 0, result.stderr
    assert sudo[0]["args"][0] == "logs"
    assert sudo[0]["args"][2] == "--owner"
    assert sudo[0]["args"][4:] == ["--sha", HEAD]
    with tarfile.open(out) as bundle:
        assert bundle.getnames() == ["deploy.sh"]
    assert list(remote.home.iterdir()) == []
    assert [call[:2] for call in calls][-1] == ["compute", "ssh"]


@pytest.mark.parametrize(
    "args",
    [
        ["deploy", HEAD, "--actor", "x;reboot"],
        ["deploy", HEAD, "--run-url", "https://x/$(id)"],
        ["deploy", HEAD, "--digests", "wallet=sha256:short"],
        ["deploy", "main"],
        ["rollback", "HEAD~1"],
        ["status", "--expect", "abc"],
        ["status", "; reboot"],
        ["--vm", "Bad VM", "status"],
        ["--zone", "us-west1-b;x", "status"],
        ["explode"],
    ],
)
def test_remote_rejects_unsafe_arguments_before_connecting(remote, args):
    result, calls, _ = remote(*args)
    assert result.returncode == 2
    assert calls == []


# ---- github-deployment.sh -----------------------------------------------------------------


@pytest.fixture
def deployment(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "gh.log"
    install(
        bin_dir / "gh",
        f"""#!{sys.executable}
import json, sys
body = json.loads(sys.stdin.read())
open({str(log)!r}, "a").write(json.dumps({{"args": sys.argv[1:], "body": body}}) + "\\n")
if "--jq" in sys.argv:
    print(4242)
""",
    )

    def run(*args: str):
        result = subprocess.run(
            ["bash", str(CI / "github-deployment.sh"), *args],
            env={
                **os.environ,
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "GITHUB_REPOSITORY": "sonalilonkar1/LedgerMesh",
                "STAGING_URL": "https://34-105-1-2.sslip.io",
                "RUN_URL": "https://github.com/sonalilonkar1/LedgerMesh/actions/runs/1",
            },
            capture_output=True,
            text=True,
            check=False,
        )
        calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        return result, calls

    return run


def test_deployment_is_recorded_for_the_exact_commit(deployment):
    result, calls = deployment("create", "a" * 40, "Deploy aaaa to staging")

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "4242"
    (call,) = calls
    assert call["args"][:4] == [
        "api",
        "--method",
        "POST",
        "repos/sonalilonkar1/LedgerMesh/deployments",
    ]
    assert call["body"]["ref"] == "a" * 40
    assert call["body"]["environment"] == "staging"
    assert call["body"]["required_contexts"] == []
    assert call["body"]["auto_merge"] is False


def test_deployment_status_carries_the_url_and_the_run(deployment):
    result, calls = deployment("status", "4242", "success", "live")

    assert result.returncode == 0, result.stderr
    (call,) = calls
    assert call["args"][3] == "repos/sonalilonkar1/LedgerMesh/deployments/4242/statuses"
    assert call["body"] == {
        "state": "success",
        "description": "live",
        "environment_url": "https://34-105-1-2.sslip.io",
        "log_url": "https://github.com/sonalilonkar1/LedgerMesh/actions/runs/1",
        "auto_inactive": True,
    }


@pytest.mark.parametrize(
    "args",
    [
        ["create", "main", "x"],
        ["status", "12", "done", "x"],
        ["status", "abc", "success", "x"],
        ["status", "12", "success"],
        ["create", "a" * 40, "x" * 141],
    ],
)
def test_deployment_rejects_bad_input(deployment, args):
    result, calls = deployment(*args)
    assert result.returncode != 0
    assert calls == []
