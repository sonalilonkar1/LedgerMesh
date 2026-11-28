"""The staging deploy and rollback workflows, statically (LM-049).

actionlint checks syntax; these tests check the decisions: what can trigger
a deploy, that pull requests cannot, pinned actions, least privilege, one
deploy at a time, untrusted values only through env, and the contract with
the hosted smoke workflow (LM-050).
"""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
DEPLOY = WORKFLOWS / "deploy-staging.yml"
ROLLBACK = WORKFLOWS / "rollback-staging.yml"
SMOKE = WORKFLOWS / "hosted-smoke.yml"
PINNED = re.compile(r"^[\w.-]+/[\w.-]+@[0-9a-f]{40}$")


def load(path: Path) -> dict:
    data = yaml.safe_load(path.read_text())
    # PyYAML reads the `on:` key as True.
    data["on"] = data.pop(True)
    return data


def steps(workflow: dict):
    for job in workflow["jobs"].values():
        yield from job.get("steps", [])


def existing_pins() -> dict[str, str]:
    pins = {}
    for path in (WORKFLOWS / "images.yml", WORKFLOWS / "image-build.yml"):
        for match in re.finditer(r"uses: ([\w.-]+/[\w.-]+)@([0-9a-f]{40})", path.read_text()):
            pins[match.group(1)] = match.group(2)
    return pins


@pytest.fixture(params=[DEPLOY, ROLLBACK], ids=lambda p: p.name)
def workflow(request):
    return request.param, load(request.param)


def test_every_action_is_pinned_and_reuses_existing_pins(workflow):
    path, data = workflow
    text = path.read_text()
    pins = existing_pins()
    for step in steps(data):
        uses = step.get("uses")
        if uses is None:
            continue
        assert PINNED.match(uses), f"{uses} is not pinned to a full commit SHA"
        action, sha = uses.split("@")
        if action in pins:
            assert sha == pins[action], f"{action} differs from the pin in images.yml"
        assert re.search(rf"{re.escape(uses)} # v\d+\.\d+\.\d+", text), f"{uses} has no version"


def test_no_expression_is_interpolated_into_a_shell_script(workflow):
    _, data = workflow
    for step in steps(data):
        assert "${{" not in step.get("run", ""), step.get("name")


def test_one_staging_change_at_a_time_and_read_only_by_default(workflow):
    _, data = workflow
    assert data["concurrency"] == {"group": "staging-deploy", "cancel-in-progress": False}
    assert data["permissions"] == {"contents": "read"}
    for name, job in data["jobs"].items():
        permissions = job.get("permissions")
        assert permissions is not None, f"{name} has no explicit permissions"
        assert "write" not in {
            v for k, v in permissions.items() if k not in {"id-token", "deployments"}
        }
        if "runs-on" in job:
            assert job.get("timeout-minutes"), f"{name} has no timeout"
        if "environment" in job:
            assert job["environment"]["name"] == "staging"
            assert job["environment"]["deployment"] is False


def test_google_credentials_only_where_the_vm_is_reached(workflow):
    _, data = workflow
    for name, job in data["jobs"].items():
        authenticates = any(
            str(step.get("uses", "")).startswith("google-github-actions/auth@")
            for step in job.get("steps", [])
        )
        if authenticates:
            assert job["permissions"].get("id-token") == "write", name
            assert job["environment"]["name"] == "staging", name
        elif "uses" not in job:
            assert "id-token" not in job["permissions"], name


def test_deploy_triggers_only_after_images_publish_on_main_or_by_hand():
    data = load(DEPLOY)
    images_name = yaml.safe_load((WORKFLOWS / "images.yml").read_text())["name"]
    assert set(data["on"]) == {"workflow_run", "workflow_dispatch"}
    assert data["on"]["workflow_run"] == {
        "workflows": [images_name],
        "types": ["completed"],
        "branches": ["main"],
    }
    assert data["on"]["workflow_dispatch"]["inputs"]["sha"]["required"] is True


def test_rollback_is_manual_only():
    data = load(ROLLBACK)
    assert set(data["on"]) == {"workflow_dispatch"}
    assert data["jobs"]["rollback"]["if"] == "github.ref == 'refs/heads/main'"


def test_gate_matches_the_image_workflow_job_names():
    # images.yml calls image-build.yml as job `publish`; image-build names each
    # matrix job "publish <image>", so GitHub shows "publish / publish <image>".
    images = yaml.safe_load((WORKFLOWS / "images.yml").read_text())
    build = yaml.safe_load((WORKFLOWS / "image-build.yml").read_text())
    assert images["jobs"]["publish"]["uses"] == "./.github/workflows/image-build.yml"
    assert build["jobs"]["image"]["name"].startswith(
        "${{ inputs.publish && 'publish' || 'check' }}"
    )
    gate = (ROOT / "infra" / "staging" / "ci" / "gate.sh").read_text()
    assert '"publish / publish ledgermesh-$image"' in gate
    matrix = [entry["image"] for entry in build["jobs"]["image"]["strategy"]["matrix"]["include"]]
    assert matrix == [
        f"ledgermesh-{n}" for n in ("coordinator", "wallet", "catalog", "audit-ledger")
    ]


def test_deploy_job_order_and_rollback_on_any_failure():
    jobs = load(DEPLOY)["jobs"]
    assert jobs["deploy"]["needs"] == "gate"
    assert jobs["deploy"]["if"] == "needs.gate.outputs.deploy == 'true'"
    assert jobs["smoke"]["needs"] == ["gate", "deploy"]
    assert jobs["promote"]["needs"] == ["gate", "deploy", "smoke"]
    rollback = jobs["rollback"]
    assert rollback["needs"] == ["gate", "deploy", "smoke", "promote"]
    condition = " ".join(rollback["if"].split())
    assert condition.startswith("always() && needs.gate.outputs.deploy == 'true'")
    assert "contains(needs.*.result, 'failure')" in condition
    assert "contains(needs.*.result, 'cancelled')" in condition
    runs = " ".join(step.get("run", "") for step in rollback["steps"])
    assert "remote.sh logs" in runs
    assert "remote.sh rollback last-known-good --after-failed" in runs
    upload = next(s for s in rollback["steps"] if "upload-artifact" in s.get("uses", ""))
    assert upload["if"] == "always()"
    deploy_runs = " ".join(step.get("run", "") for step in jobs["deploy"]["steps"])
    assert "--no-promote" in deploy_runs
    assert "status --expect" in deploy_runs


@pytest.mark.parametrize("path", [DEPLOY, ROLLBACK], ids=lambda p: p.name)
def test_smoke_is_called_with_the_hosted_smoke_contract(path):
    jobs = load(path)["jobs"]
    smoke = jobs["smoke"]
    assert smoke["uses"] == "./.github/workflows/hosted-smoke.yml"
    assert set(smoke["with"]) == {"base_url", "expected_revision"}
    # A called workflow cannot get more than the caller job grants.
    assert smoke["permissions"] == {"contents": "read", "id-token": "write"}


@pytest.mark.skipif(not SMOKE.exists(), reason="hosted-smoke.yml is added by LM-050")
def test_hosted_smoke_accepts_the_contract_and_cannot_deadlock():
    data = load(SMOKE)
    inputs = data["on"]["workflow_call"]["inputs"]
    assert inputs["base_url"]["required"] is True
    assert inputs.get("expected_revision", {}).get("required", False) is False
    # Sharing the caller's concurrency group would wait on the caller forever.
    assert "staging-deploy" not in str(data.get("concurrency", ""))
    for job in data["jobs"].values():
        assert "staging-deploy" not in str(job.get("concurrency", ""))


def test_core_ci_runs_for_these_workflows():
    paths = yaml.safe_load((WORKFLOWS / "ci.yml").read_text())[True]["pull_request"]["paths"]
    for name in ("deploy-staging.yml", "rollback-staging.yml"):
        assert f".github/workflows/{name}" in paths
    assert "infra/staging/**" in paths
