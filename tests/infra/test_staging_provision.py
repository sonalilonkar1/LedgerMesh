"""infra/staging/provision.sh and bootstrap-wif.sh, offline (LM-048).

A fake ``gcloud`` (and ``openssl``) is first on PATH. Dry runs must never call
it; the idempotency test lets it report every resource as present and checks
that nothing is created again.
"""

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import install

ROOT = Path(__file__).resolve().parents[2]
STAGING = ROOT / "infra" / "staging"
IAP_RANGE = "35.235.240.0/20"
UP_ARGS = [
    "--billing-account",
    "000000-000000-000000",
    "--acme-email",
    "ops@example.com",
    "--admin-member",
    "user:admin@example.com",
]

# Logs every call. FAKE_PRESENT=1 answers every lookup as an existing resource.
FAKE_GCLOUD = f"""#!{sys.executable}
import json, os, sys

args = sys.argv[1:]
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps(args) + "\\n")
if os.environ.get("FAKE_PRESENT") != "1":
    sys.exit(99)
joined = " ".join(args)
if "addresses describe" in joined and "value(address)" in joined:
    print("34.105.1.2")
elif "disks describe" in joined and "value(resourcePolicies)" in joined:
    print("https://www.googleapis.com/compute/v1/projects/p/regions/us-west1/resourcePolicies/ledgermesh-data-daily")
elif "secrets versions list" in joined:
    print("1")
elif "billing budgets list" in joined:
    print("billingAccounts/000000-000000-000000/budgets/abc")
"""


@pytest.fixture
def gcloud(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    install(bin_dir / "gcloud", FAKE_GCLOUD)
    install(bin_dir / "openssl", f"#!{sys.executable}\nimport sys\nsys.exit(98)\n")
    log = tmp_path / "gcloud.log"

    def run(script: str, *args: str, present: bool = False):
        env = {
            **os.environ,
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "FAKE_LOG": str(log),
            "FAKE_PRESENT": "1" if present else "0",
        }
        env.pop("LEDGERMESH_GCP_PROJECT", None)
        result = subprocess.run(
            ["bash", str(STAGING / script), *args],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        return result, calls

    return run


def commands(output: str) -> list[list[str]]:
    """The commands a dry run prints, as argument lists ('+ ' lines, pipelines skipped)."""
    return [
        shlex.split(line[2:])
        for line in output.splitlines()
        if line.startswith("+ ") and "|" not in line
    ]


def find(cmds: list[list[str]], *words: str) -> list[list[str]]:
    return [cmd for cmd in cmds if all(word in cmd for word in words)]


def flag(cmd: list[str], name: str) -> list[str]:
    return [cmd[i + 1] for i, arg in enumerate(cmd) if arg == name]


def test_up_dry_run_runs_nothing_and_prints_every_create(gcloud):
    result, calls = gcloud("provision.sh", "up", "--dry-run", *UP_ARGS)

    assert result.returncode == 0, result.stderr
    assert calls == []
    cmds = commands(result.stdout)
    assert all(cmd[0] == "gcloud" for cmd in cmds)
    assert all(flag(cmd, "--project") == ["deven-cloud-lab"] for cmd in cmds)
    for resource in (
        ["compute", "networks", "create"],
        ["compute", "networks", "subnets", "create"],
        ["compute", "addresses", "create"],
        ["iam", "service-accounts", "create"],
        ["compute", "resource-policies", "create", "snapshot-schedule"],
        ["compute", "disks", "create"],
        ["compute", "instances", "create"],
        ["billing", "budgets", "create"],
    ):
        assert find(cmds, *resource), resource
    assert len(find(cmds, "secrets", "create")) == 4
    assert "https://203-0-113-10.sslip.io" in result.stdout


def test_firewall_allows_only_web_publicly_and_ssh_from_iap(gcloud):
    result, _ = gcloud("provision.sh", "up", "--dry-run", *UP_ARGS)
    rules = find(commands(result.stdout), "firewall-rules", "create")

    assert len(rules) == 2
    by_ports = {flag(rule, "--rules")[0]: rule for rule in rules}
    assert set(by_ports) == {"tcp:80,tcp:443", "tcp:22"}
    assert flag(by_ports["tcp:80,tcp:443"], "--source-ranges") == ["0.0.0.0/0"]
    assert flag(by_ports["tcp:22"], "--source-ranges") == [IAP_RANGE]
    for rule in rules:
        assert flag(rule, "--network") == ["ledgermesh-staging"]
        assert flag(rule, "--direction") == ["INGRESS"]
        assert flag(rule, "--action") == ["ALLOW"]


def test_never_uses_the_default_network(gcloud):
    result, _ = gcloud("provision.sh", "up", "--dry-run", *UP_ARGS)
    for cmd in commands(result.stdout):
        assert "default" not in flag(cmd, "--network")
        assert "default" not in flag(cmd, "--subnet")


def test_vm_shape_disk_and_access_settings(gcloud):
    result, _ = gcloud("provision.sh", "up", "--dry-run", *UP_ARGS)
    (vm,) = find(commands(result.stdout), "instances", "create")

    assert flag(vm, "--machine-type") == ["e2-standard-4"]
    assert flag(vm, "--zone") == ["us-west1-b"]
    assert flag(vm, "--image-family") == ["debian-12"]
    assert flag(vm, "--image-project") == ["debian-cloud"]
    assert {"--shielded-secure-boot", "--shielded-vtpm"} <= set(vm)
    assert flag(vm, "--disk") == [
        "name=ledgermesh-data,device-name=ledgermesh-data,mode=rw,auto-delete=no"
    ]
    (metadata,) = flag(vm, "--metadata")
    assert "enable-oslogin=TRUE" in metadata
    assert "block-project-ssh-keys=TRUE" in metadata
    assert "ledgermesh-site-address=203-0-113-10.sslip.io" in metadata
    (startup,) = flag(vm, "--metadata-from-file")
    assert startup.endswith("infra/staging/vm-startup.sh")
    assert flag(vm, "--service-account") == [
        "ledgermesh-staging-vm@deven-cloud-lab.iam.gserviceaccount.com"
    ]


def test_admin_ssh_is_limited_to_port_22_through_iap(gcloud):
    result, _ = gcloud("provision.sh", "up", "--dry-run", *UP_ARGS)
    (binding,) = find(commands(result.stdout), "roles/iap.tunnelResourceAccessor")

    assert flag(binding, "--member") == ["user:admin@example.com"]
    assert flag(binding, "--condition")[0].startswith("expression=destination.port == 22,")
    assert find(commands(result.stdout), "roles/compute.osAdminLogin", "user:admin@example.com")


def test_secrets_are_readable_only_by_the_vm_service_account_one_by_one(gcloud):
    result, _ = gcloud("provision.sh", "up", "--dry-run", *UP_ARGS)
    bindings = find(commands(result.stdout), "secrets", "add-iam-policy-binding")

    assert len(bindings) == 4
    for binding in bindings:
        assert flag(binding, "--role") == ["roles/secretmanager.secretAccessor"]
        assert flag(binding, "--member") == [
            "serviceAccount:ledgermesh-staging-vm@deven-cloud-lab.iam.gserviceaccount.com"
        ]
    # No project-wide Secret Manager role.
    assert not find(commands(result.stdout), "projects", "roles/secretmanager.secretAccessor")
    assert result.stdout.count("openssl rand -hex 32 | tr -d") == 4


def test_budget_has_three_alert_thresholds_for_this_project(gcloud):
    result, _ = gcloud("provision.sh", "up", "--dry-run", *UP_ARGS)
    (budget,) = find(commands(result.stdout), "budgets", "create")

    assert flag(budget, "--threshold-rule") == ["percent=0.5", "percent=0.9", "percent=1.0"]
    assert flag(budget, "--filter-projects") == ["projects/deven-cloud-lab"]
    assert flag(budget, "--billing-account") == ["000000-000000-000000"]
    assert flag(budget, "--budget-amount") == ["50USD"]


def test_snapshot_schedule_is_daily_with_seven_day_retention(gcloud):
    result, _ = gcloud("provision.sh", "up", "--dry-run", *UP_ARGS)
    (schedule,) = find(commands(result.stdout), "snapshot-schedule")

    assert "--daily-schedule" in schedule
    assert flag(schedule, "--max-retention-days") == ["7"]


@pytest.mark.parametrize(
    ("drop", "message"),
    [("--billing-account", "--billing-account"), ("--acme-email", "--acme-email")],
)
def test_up_requires_billing_account_and_acme_email(gcloud, drop, message):
    args = list(UP_ARGS)
    index = args.index(drop)
    del args[index : index + 2]
    result, calls = gcloud("provision.sh", "up", "--dry-run", *args)

    assert result.returncode != 0
    assert message in result.stderr
    assert calls == []


def test_admin_members_must_be_iam_principals(gcloud):
    result, _ = gcloud("provision.sh", "up", "--dry-run", *UP_ARGS, "--admin-member", "bob")
    assert result.returncode != 0
    assert "--admin-member" in result.stderr


def test_parameters_are_overridable(gcloud):
    result, _ = gcloud(
        "provision.sh",
        "up",
        "--dry-run",
        *UP_ARGS,
        "--project",
        "other-proj-1",
        "--zone",
        "us-west1-a",
    )
    assert result.returncode == 0, result.stderr
    cmds = commands(result.stdout)
    assert all(flag(cmd, "--project") == ["other-proj-1"] for cmd in cmds)
    assert flag(find(cmds, "instances", "create")[0], "--zone") == ["us-west1-a"]


def test_rerun_against_existing_resources_creates_nothing(gcloud):
    result, calls = gcloud("provision.sh", "up", *UP_ARGS, present=True)

    assert result.returncode == 0, result.stderr
    verbs = {" ".join(call[3:6]) for call in calls}  # after gcloud --project P --quiet
    assert not [call for call in calls if "create" in call or "create-oidc" in call]
    assert not [call for call in calls if "versions" in call and "add" in call]
    # The VM's startup script and metadata are refreshed instead.
    assert any("add-metadata" in call for call in calls)
    assert verbs


def test_teardown_requires_confirmation(gcloud):
    result, calls = gcloud("provision.sh", "teardown", "--dry-run")
    assert result.returncode != 0
    assert "--confirm deven-cloud-lab" in result.stderr
    assert calls == []


def test_teardown_dry_run_deletes_everything_up_created(gcloud):
    result, calls = gcloud(
        "provision.sh",
        "teardown",
        "--dry-run",
        "--confirm",
        "deven-cloud-lab",
        "--billing-account",
        "000000-000000-000000",
    )
    assert result.returncode == 0, result.stderr
    assert calls == []
    cmds = commands(result.stdout)
    for resource in (
        ["instances", "delete"],
        ["disks", "delete", "ledgermesh-data"],
        ["snapshots", "delete"],
        ["resource-policies", "delete"],
        ["addresses", "delete"],
        ["networks", "delete"],
        ["iam", "service-accounts", "delete"],
        ["budgets", "delete"],
    ):
        assert find(cmds, *resource), resource
    assert len(find(cmds, "firewall-rules", "delete")) == 2
    assert len(find(cmds, "secrets", "delete")) == 4
    assert "bootstrap-wif.sh teardown" in result.stdout


@pytest.mark.parametrize("command", ["stop", "start", "status"])
def test_lifecycle_commands_dry_run(gcloud, command):
    result, calls = gcloud("provision.sh", command, "--dry-run")
    assert result.returncode == 0, result.stderr
    assert calls == []


# ---- bootstrap-wif.sh ---------------------------------------------------------------


def test_wif_condition_restricts_to_this_repository_and_main(gcloud):
    result, calls = gcloud("bootstrap-wif.sh", "--dry-run")

    assert result.returncode == 0, result.stderr
    assert calls == []
    (provider,) = find(commands(result.stdout), "create-oidc")
    assert flag(provider, "--attribute-condition") == [
        "assertion.repository=='sonalilonkar1/LedgerMesh'"
        " && assertion.repository_id=='1370369282'"
        " && assertion.ref=='refs/heads/main'"
    ]
    assert flag(provider, "--issuer-uri") == ["https://token.actions.githubusercontent.com"]
    assert "attribute.ref=assertion.ref" in flag(provider, "--attribute-mapping")[0]


def test_deployer_gets_least_privilege(gcloud):
    result, _ = gcloud("bootstrap-wif.sh", "--dry-run")
    cmds = commands(result.stdout)
    deployer = "serviceAccount:ledgermesh-deployer@deven-cloud-lab.iam.gserviceaccount.com"

    roles = sorted(
        flag(cmd, "--role")[0]
        for cmd in cmds
        if "add-iam-policy-binding" in cmd and deployer in cmd
    )
    assert roles == [
        "roles/compute.osAdminLogin",
        "roles/compute.viewer",
        "roles/iam.serviceAccountUser",
        "roles/iap.tunnelResourceAccessor",
    ]
    assert "secretmanager" not in result.stdout
    (impersonation,) = find(cmds, "roles/iam.workloadIdentityUser")
    assert flag(impersonation, "--member")[0].endswith(
        "/workloadIdentityPools/github/attribute.repository/sonalilonkar1/LedgerMesh"
    )
    (tunnel,) = find(cmds, "roles/iap.tunnelResourceAccessor")
    assert flag(tunnel, "--condition")[0].startswith("expression=destination.port == 22,")


def test_wif_prints_the_github_environment_variables(gcloud):
    result, _ = gcloud("bootstrap-wif.sh", "--dry-run")
    for name in (
        "GCP_PROJECT_ID=",
        "GCP_ZONE=",
        "STAGING_VM_NAME=",
        "GCP_WORKLOAD_IDENTITY_PROVIDER=projects/",
        "GCP_DEPLOY_SERVICE_ACCOUNT=ledgermesh-deployer@",
    ):
        assert name in result.stdout


def test_wif_teardown_requires_confirmation(gcloud):
    result, _ = gcloud("bootstrap-wif.sh", "teardown", "--dry-run")
    assert result.returncode != 0
    result, calls = gcloud(
        "bootstrap-wif.sh", "teardown", "--dry-run", "--confirm", "deven-cloud-lab"
    )
    assert result.returncode == 0, result.stderr
    assert calls == []
    assert find(commands(result.stdout), "workload-identity-pools", "delete")
