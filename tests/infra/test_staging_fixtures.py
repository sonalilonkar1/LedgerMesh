"""The hosted smoke fixture mechanism, offline (LM-050).

infra/staging/fixtures.py runs inside the wallet and catalog containers. Here
it runs against an SQLite copy of each service's own table, which is enough to
prove the rules: only ``smoke-<run_id>-*`` rows, a plain INSERT that fails on
a reused run id, no other row touched, and a refusal outside staging/local/test.
stack.sh's ``fixtures`` command is checked with a fake ``docker``.
"""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, insert, select

from services.catalog.app.db.schema import rewards
from services.wallet.app.db.schema import wallets
from tests.hosted import _support

from .conftest import install

ROOT = Path(__file__).resolve().parents[2]
STAGING = ROOT / "infra" / "staging"
SHA = "0123456789abcdef0123456789abcdef01234567"
RUN_ID = "gh123456a1"


def load_fixtures():
    spec = importlib.util.spec_from_file_location("staging_fixtures", STAGING / "fixtures.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses look their module up
    # No __pycache__ in infra/staging: it is the deploy bundle.
    previous, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


fixtures = load_fixtures()


@pytest.fixture
def staging_env(monkeypatch):
    monkeypatch.setenv("LEDGERMESH_ENV", "staging")


def sqlite_table(tmp_path: Path, table) -> str:
    url = f"sqlite:///{tmp_path / (table.name + '.db')}"
    engine = create_engine(url)
    table.create(engine)
    engine.dispose()
    return url


def rows(url: str, table) -> list[dict]:
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            return [dict(r._mapping) for r in connection.execute(select(table))]
    finally:
        engine.dispose()


def test_the_suite_and_the_script_agree_on_the_fixtures():
    assert {w.name: w.available_points for w in fixtures.WALLETS} == _support.FIXTURE_WALLETS
    assert {
        r.name: (r.point_cost, r.total_inventory) for r in fixtures.REWARDS
    } == _support.FIXTURE_REWARDS
    assert fixtures.RUN_ID_PATTERN.pattern == _support.RUN_ID_PATTERN.pattern


def test_every_identifier_carries_the_run_prefix():
    for row in fixtures.wallet_rows(RUN_ID):
        assert row["user_id"].startswith(f"smoke-{RUN_ID}-")
    for row in fixtures.reward_rows(RUN_ID):
        assert row["reward_id"].startswith(f"smoke-{RUN_ID}-")
        assert row["available_inventory"] == row["total_inventory"]
    ids = [r["user_id"] for r in fixtures.wallet_rows(RUN_ID)]
    assert len(ids) == len(set(ids))


@pytest.mark.parametrize(
    "run_id", ["", "abc", "ABC123", "abc-123", "abc_123", "a" * 41, "abc123;rm", "../abc123"]
)
def test_bad_run_ids_are_refused(run_id):
    with pytest.raises(ValueError):
        fixtures.fixture_id(run_id, "completed")


@pytest.mark.parametrize("environment", [None, "", "production", "prod", "Staging"])
def test_refuses_outside_staging_local_and_test(monkeypatch, tmp_path, environment):
    if environment is None:
        monkeypatch.delenv("LEDGERMESH_ENV", raising=False)
    else:
        monkeypatch.setenv("LEDGERMESH_ENV", environment)
    url = sqlite_table(tmp_path, wallets)
    with pytest.raises(SystemExit, match="refusing"):
        fixtures.create("wallet", RUN_ID, url=url)
    assert rows(url, wallets) == []


@pytest.mark.parametrize(("service", "table"), [("wallet", wallets), ("catalog", rewards)])
def test_inserts_only_this_runs_rows_and_never_reuses_a_run_id(
    staging_env, tmp_path, service, table
):
    url = sqlite_table(tmp_path, table)
    key = "user_id" if service == "wallet" else "reward_id"
    existing = (
        {"user_id": "user-alice", "available_points": 7}
        if service == "wallet"
        else {
            "reward_id": "reward-coffee",
            "name": "Coffee",
            "point_cost": 100,
            "total_inventory": 9,
            "available_inventory": 4,
        }
    )
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(insert(table), [existing])
    engine.dispose()

    created = fixtures.create(service, RUN_ID, url=url)

    stored = {row[key]: row for row in rows(url, table)}
    assert set(stored) == {existing[key]} | {row[key] for row in created}
    # The row that was already there is untouched.
    for column, value in existing.items():
        assert stored[existing[key]][column] == value
    # Plain INSERT: the same run id again fails instead of reusing the rows.
    with pytest.raises(Exception, match="UNIQUE|unique|IntegrityError"):
        fixtures.create(service, RUN_ID, url=url)
    assert len(rows(url, table)) == 1 + len(created)


def test_cli_uses_the_services_own_database_url_and_reports_reuse(
    staging_env, monkeypatch, tmp_path, capsys
):
    url = sqlite_table(tmp_path, wallets)
    # services/wallet/app/repository.py database_url(): WALLET_DATABASE_URL first.
    monkeypatch.setenv("WALLET_DATABASE_URL", url)

    assert fixtures.main(["wallet", "--run-id", RUN_ID]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["run_id"] == RUN_ID
    assert [r["user_id"] for r in printed["created"]] == [
        r["user_id"] for r in fixtures.wallet_rows(RUN_ID)
    ]

    assert fixtures.main(["wallet", "--run-id", RUN_ID]) == 1
    err = capsys.readouterr().err
    assert "already exist; use a new run id" in err
    assert url not in err


def test_cli_rejects_a_bad_run_id_or_service(staging_env):
    with pytest.raises(SystemExit) as error:
        fixtures.main(["wallet", "--run-id", "BAD"])
    assert error.value.code == 2
    with pytest.raises(SystemExit):
        fixtures.main(["coordinator", "--run-id", RUN_ID])


def test_errors_never_echo_a_connection_url():
    class Driver(Exception):
        pass

    class OperationalError(Exception):
        orig = Driver('connection to server at "wallet-db" failed: password authentication')

    message = fixtures.describe_error("wallet", RUN_ID, OperationalError("postgresql://u:p@h/db"))
    assert "postgresql://" not in message
    assert "password authentication" in message


# ---- stack.sh fixtures ------------------------------------------------------------------

FAKE_DOCKER = f"""#!{sys.executable}
import json, os, sys

args = sys.argv[1:]
stdin = sys.stdin.read() if "exec" in args else ""
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({{"args": args, "stdin": stdin}}) + "\\n")
fail = os.environ.get("FAKE_FAIL_SERVICE")
if fail and fail in args:
    sys.exit(1)
"""


@pytest.fixture
def stack(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    install(bin_dir / "docker", FAKE_DOCKER)
    env_file = tmp_path / "staging.env"
    env_file.write_text(
        "LEDGERMESH_SITE_ADDRESS=localhost\nLEDGERMESH_TLS=internal\n"
        "COORDINATOR_DB_PASSWORD=coordinator-test-pw-0123456789\n"  # gitleaks:allow
        "WALLET_DB_PASSWORD=wallet-test-pw-0123456789\n"  # gitleaks:allow
        "CATALOG_DB_PASSWORD=catalog-test-pw-0123456789\n"  # gitleaks:allow
        "AUDIT_LEDGER_DB_PASSWORD=audit-test-pw-0123456789\n"  # gitleaks:allow
    )
    env_file.chmod(0o600)
    log = tmp_path / "docker.log"

    def run(*args: str, **env: str):
        result = subprocess.run(
            [
                "bash",
                str(STAGING / "stack.sh"),
                "--env-file",
                str(env_file),
                "--data-dir",
                str(tmp_path / "data"),
                "--project-name",
                "lmtest",
                "--sha",
                SHA,
                *args,
            ],
            env={
                **os.environ,
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "FAKE_LOG": str(log),
                **env,
            },
            capture_output=True,
            text=True,
            check=False,
        )
        calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        return result, calls

    return run


def test_stack_runs_the_script_in_each_services_own_container(stack):
    result, calls = stack("fixtures", RUN_ID)

    assert result.returncode == 0, result.stderr
    script = (STAGING / "fixtures.py").read_text()
    assert len(calls) == 2
    for call, service in zip(calls, ("wallet", "catalog"), strict=True):
        args = call["args"]
        assert args[0] == "compose"
        assert args[args.index("-p") + 1] == "lmtest"
        tail = args[args.index("exec") :]
        assert tail == ["exec", "-T", service, "python", "-", service, "--run-id", RUN_ID]
        assert call["stdin"] == script


def test_stack_fixtures_fail_closed(stack):
    result, calls = stack("fixtures", RUN_ID, FAKE_FAIL_SERVICE="wallet")
    assert result.returncode != 0
    assert "wallet fixtures for run" in result.stderr
    assert len(calls) == 1, "catalog must not run after wallet failed"


@pytest.mark.parametrize("run_id", ["", "BAD", "abc", "x;reboot", "a" * 41])
def test_stack_refuses_bad_run_ids_before_docker(stack, run_id):
    result, calls = stack("fixtures", run_id)
    assert result.returncode != 0
    assert calls == []
