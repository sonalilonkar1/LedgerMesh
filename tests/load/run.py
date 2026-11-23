"""Run one repeatable load experiment and save everything under a run ID (LM-035).

From the repository root, with the stack running (``make infra-up``). The reset
step wipes the databases, so you must state that this is a local or test setup:

    LEDGERMESH_ENV=local uv run python -m tests.load.run \
        --mode two_phase_commit --users 20 --run-time 30s

Add faults with ``--fault kind:target@start+length`` (repeat the option for
several). This pauses the Wallet from second 10 to second 15 of the load:

    LEDGERMESH_ENV=local uv run python -m tests.load.run \
        --mode two_phase_commit --users 20 --run-time 30s --fault pause:wallet@10s+5s

Each run:

1. checks that the Coordinator is reachable and every database is local,
2. resets every database to the seed data, so runs start from the same state,
3. runs the Locust workload and any scheduled faults,
4. waits for the accepted transactions to settle and records how each one ended,
5. saves the settings, statistics, per-transaction results and fault timing to
   ``artifacts/load/<run-id>/`` (the artifacts folder is not committed).

The exit status is 0 for a valid run. It is 2 when the experiment itself went
wrong (for example a fault could not be injected or a service never recovered).
Request failures caused by a fault that did run are expected and are not errors.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from infra.scripts.db import SAFE_ENVIRONMENTS, SERVICES, database_url
from tests.load.faults import (
    TARGETS,
    FaultError,
    FaultRunner,
    FaultSpec,
    parse_seconds,
    validate_schedule,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
LOCUSTFILE = Path(__file__).with_name("locustfile.py")
MODES = ("two_phase_commit", "saga")
COORDINATOR_URL = "http://localhost:8000"
WALLET_URL = "http://localhost:8001"
CATALOG_URL = "http://localhost:8002"
LOAD_START_TIMEOUT_SECONDS = 60.0
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
TERMINAL_STATUSES = {"COMMITTED", "ABORTED", "COMPLETED", "FAILED", "COMPENSATED"}


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def make_run_id(mode: str, users: int) -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    # The random suffix keeps two runs started in the same second apart.
    return f"{stamp}-{mode}-u{users}-{uuid.uuid4().hex[:6]}"


def git_commit() -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True
    )
    return result.stdout.strip() if result.returncode == 0 else None


def fetch_json(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            return json.load(response)
    except (urllib.error.URLError, TimeoutError, ConnectionError, ValueError):
        return None


def require_coordinator() -> None:
    if fetch_json(f"{COORDINATOR_URL}/health") is None:
        sys.exit(f"Coordinator is not reachable at {COORDINATOR_URL}. Run: make infra-up")


def require_safe_reset() -> None:
    """Refuse to reset anything that is not a local or test database.

    The reset wipes every service database. The caller must say the setup is
    local or test, and every database URL (including any *_DATABASE_URL
    override) must point at this machine.
    """
    environment = os.environ.get("LEDGERMESH_ENV", "")
    if environment not in SAFE_ENVIRONMENTS:
        sys.exit(
            "Refusing to reset the databases: set LEDGERMESH_ENV=local or LEDGERMESH_ENV=test "
            f"(currently {environment!r}), or pass --no-reset."
        )
    for service in SERVICES:
        host = urlsplit(database_url(service)).hostname
        if host not in LOCAL_HOSTS:
            sys.exit(
                f"Refusing to reset the databases: {service} points at {host!r}, "
                "which is not this machine."
            )


def reset_data() -> None:
    # The seed script repeats the LEDGERMESH_ENV check; the environment is passed
    # through unchanged so that check stays in force.
    subprocess.run(
        [sys.executable, "-m", "infra.scripts.seed", "baseline"],
        cwd=REPO_ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def starting_state(user_id: str, reward_id: str) -> dict | None:
    """Balances at the start, so a run that exhausts them can be told apart."""
    wallet = fetch_json(f"{WALLET_URL}/v1/wallets/{user_id}")
    reward = fetch_json(f"{CATALOG_URL}/v1/rewards/{reward_id}")
    if not wallet or not reward:
        return None
    affordable = wallet["available_points"] // reward["point_cost"]
    return {
        "available_points": wallet["available_points"],
        "reward_point_cost": reward["point_cost"],
        "available_inventory": reward["available_inventory"],
        "max_successful_redemptions": min(affordable, reward["available_inventory"]),
    }


def wait_for_load_start(run_dir: Path, locust: subprocess.Popen) -> float | None:
    """Epoch time at which Locust began the load, or None if it never did."""
    marker = run_dir / "load_started.txt"
    deadline = time.monotonic() + LOAD_START_TIMEOUT_SECONDS
    while time.monotonic() < deadline and locust.poll() is None:
        if marker.exists():
            try:
                return float(marker.read_text())
            except ValueError:  # still being written
                pass
        time.sleep(0.1)
    return None


def resolve_transactions(run_dir: Path, timeout: float) -> tuple[Counter, Counter]:
    """Find out how every accepted transaction really ended.

    Polls each one until it reaches a final state or the deadline passes, then
    rewrites ``transactions.jsonl`` with the result. Transactions still not final
    are reported as unresolved, separately from business failures.
    """
    path = run_dir / "transactions.jsonl"
    if not path.exists():
        return Counter(), Counter()
    rows = [json.loads(line) for line in path.read_text().splitlines() if line]
    pending = {row["transaction_id"]: row for row in rows}
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        for transaction_id, row in list(pending.items()):
            body = fetch_json(f"{COORDINATOR_URL}/v1/redemptions/{transaction_id}")
            status = body["status"] if body else None
            if status:
                row["last_status"] = status
            if status in TERMINAL_STATUSES:
                row["final_status"] = status
                row["seconds_to_final_observed"] = round(time.time() - row["submitted_at"], 3)
                del pending[transaction_id]
        if pending:
            time.sleep(0.5)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    outcomes = Counter(row["final_status"] for row in rows if "final_status" in row)
    unresolved = Counter(
        row.get("last_status", "UNKNOWN") for row in rows if "final_status" not in row
    )
    return outcomes, unresolved


def parse_faults(texts: list[str]) -> list[FaultSpec]:
    try:
        faults = [FaultSpec.parse(text) for text in texts]
        validate_schedule(faults)
    except FaultError as error:
        sys.exit(f"Invalid --fault: {error}")
    return faults


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--users", type=int, default=10, help="concurrent virtual users")
    parser.add_argument("--spawn-rate", type=int, help="users started per second (default: all)")
    parser.add_argument("--run-time", default="30s", help="for example 30s or 2m")
    parser.add_argument("--settle-timeout", default="30s", help="how long to wait for results")
    parser.add_argument("--lm-user", default="user-alice")
    parser.add_argument("--lm-reward", default="reward-coffee")
    parser.add_argument(
        "--fault",
        action="append",
        default=[],
        metavar="KIND:TARGET@START+LENGTH",
        help=f"kind is kill, pause or stop; target is one of {', '.join(TARGETS)}",
    )
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "artifacts" / "load")
    parser.add_argument("--no-reset", action="store_true", help="skip the database reset")
    args = parser.parse_args()

    faults = parse_faults(args.fault)
    settle_timeout = parse_seconds(args.settle_timeout)
    require_coordinator()
    if not args.no_reset:
        require_safe_reset()

    run_id = make_run_id(args.mode, args.users)
    run_dir = args.out / run_id
    run_dir.mkdir(parents=True)
    print(f"Run {run_id}\nResults: {run_dir}")

    if not args.no_reset:
        print("Resetting databases to the seed data...")
        reset_data()
    initial_state = starting_state(args.lm_user, args.lm_reward)

    started_at = utc_now()
    command = [
        sys.executable,
        "-m",
        "locust",
        "-f",
        str(LOCUSTFILE),
        "--headless",
        "--mode",
        args.mode,
        "--users",
        str(args.users),
        "--spawn-rate",
        str(args.spawn_rate or args.users),
        "--run-time",
        args.run_time,
        "--lm-user",
        args.lm_user,
        "--lm-reward",
        args.lm_reward,
        "--csv",
        str(run_dir / "locust"),
        # Failed requests are expected while a fault is active; they are reported
        # in the results, not through the exit status.
        "--exit-code-on-error",
        "0",
    ]
    locust = subprocess.Popen(
        command, cwd=REPO_ROOT, env={**os.environ, "LM_RUN_DIR": str(run_dir)}
    )

    problems: list[str] = []
    runner: FaultRunner | None = None
    try:
        if faults:
            load_started_at = wait_for_load_start(run_dir, locust)
            if load_started_at is None:
                problems.append("faults were requested but the load never started")
            else:
                runner = FaultRunner(faults, run_dir / "faults.jsonl", load_started_at)
                runner.start()
        locust_exit_code = locust.wait()
    except KeyboardInterrupt:
        problems.append("the run was interrupted")
        locust.terminate()
        locust_exit_code = locust.wait()
    finally:
        # Always undo any fault that is still active, even if the run failed.
        if runner is not None:
            runner.finish()
            problems.extend(runner.problems)
    finished_at = utc_now()

    if locust_exit_code != 0:
        problems.append(f"Locust exited with status {locust_exit_code}")

    outcomes: dict | None = None
    unresolved: dict | None = None
    if args.mode == "two_phase_commit":
        print(f"Waiting up to {args.settle_timeout} for transactions to settle...")
        final, stuck = resolve_transactions(run_dir, settle_timeout)
        outcomes, unresolved = dict(final), dict(stuck)
    # Saga redemptions are only submitted until LM-022 can drive them.
    workload = "full_2pc_execution" if args.mode == "two_phase_commit" else "submission_only"

    summary = {
        "run_id": run_id,
        "valid": not problems,
        "problems": problems,
        "mode": args.mode,
        "workload": workload,
        "users": args.users,
        "spawn_rate": args.spawn_rate or args.users,
        "run_time": args.run_time,
        "user_id": args.lm_user,
        "reward_id": args.lm_reward,
        "starting_state": initial_state,
        "faults": [str(fault) for fault in faults],
        "database_reset": not args.no_reset,
        "git_commit": git_commit(),
        "started_at": started_at,
        "finished_at": finished_at,
        "locust_exit_code": locust_exit_code,
        "final_outcomes": outcomes,
        "unresolved": unresolved,
    }
    (run_dir / "run.json").write_text(json.dumps(summary, indent=2))

    print(f"\nWorkload: {workload}")
    if outcomes is not None:
        print(f"Final outcomes: {outcomes}")
        print(f"Unresolved after {args.settle_timeout}: {unresolved}")
    if problems:
        print("RUN INVALID:\n  - " + "\n  - ".join(problems))
    print(f"Saved run {run_id} to {run_dir}")
    sys.exit(0 if not problems else 2)


if __name__ == "__main__":
    main()
