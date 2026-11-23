"""Run one repeatable load experiment and save everything under a run ID (LM-035).

From the repository root, with the stack running (``make infra-up``):

    uv run python -m tests.load.run --mode two_phase_commit --users 20 --run-time 30s

Add faults with ``--fault kind:target@start+length`` (repeat the option for
several). This pauses the Wallet from second 10 to second 15 of the load:

    uv run python -m tests.load.run --mode two_phase_commit --users 20 --run-time 30s \
        --fault pause:wallet@10s+5s

Each run:

1. checks that the Coordinator is reachable,
2. resets every database to the seed data, so runs start from the same state,
3. runs the Locust workload and any scheduled faults,
4. saves the settings, Locust statistics, redemption outcomes and fault timing to
   ``artifacts/load/<run-id>/`` (the artifacts folder is not committed).
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
from datetime import UTC, datetime
from pathlib import Path

from tests.load.faults import TARGETS, FaultError, FaultRunner, FaultSpec

REPO_ROOT = Path(__file__).resolve().parents[2]
LOCUSTFILE = Path(__file__).with_name("locustfile.py")
MODES = ("two_phase_commit", "saga")
COORDINATOR_URL = "http://localhost:8000"
LOAD_START_TIMEOUT_SECONDS = 60.0


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def make_run_id(mode: str, users: int) -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{mode}-u{users}"


def git_commit() -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True
    )
    return result.stdout.strip() if result.returncode == 0 else None


def require_coordinator() -> None:
    try:
        with urllib.request.urlopen(f"{COORDINATOR_URL}/health", timeout=3):
            return
    except (urllib.error.URLError, TimeoutError) as error:
        sys.exit(f"Coordinator is not reachable at {COORDINATOR_URL} ({error}). Run: make infra-up")


def reset_data() -> None:
    # Wipes and reseeds every service database. The seed script refuses to run
    # unless LEDGERMESH_ENV is local or test, so this never touches a real system.
    env = {**os.environ, "LEDGERMESH_ENV": "local"}
    subprocess.run(
        [sys.executable, "-m", "infra.scripts.seed", "baseline"],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


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


def parse_faults(texts: list[str]) -> list[FaultSpec]:
    try:
        return [FaultSpec.parse(text) for text in texts]
    except FaultError as error:
        sys.exit(f"Invalid --fault: {error}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--users", type=int, default=10, help="concurrent virtual users")
    parser.add_argument("--spawn-rate", type=int, help="users started per second (default: all)")
    parser.add_argument("--run-time", default="30s", help="for example 30s or 2m")
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
    require_coordinator()

    run_id = make_run_id(args.mode, args.users)
    run_dir = args.out / run_id
    run_dir.mkdir(parents=True)
    print(f"Run {run_id}\nResults: {run_dir}")

    if not args.no_reset:
        print("Resetting databases to the seed data...")
        reset_data()

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
    ]
    locust = subprocess.Popen(
        command, cwd=REPO_ROOT, env={**os.environ, "LM_RUN_DIR": str(run_dir)}
    )

    runner: FaultRunner | None = None
    try:
        if faults:
            load_started_at = wait_for_load_start(run_dir, locust)
            if load_started_at is None:
                print("Load never started; no faults were run.")
            else:
                runner = FaultRunner(faults, run_dir / "faults.jsonl", load_started_at)
                runner.start()
        exit_code = locust.wait()
    except KeyboardInterrupt:
        locust.terminate()
        exit_code = locust.wait()
    finally:
        # Always undo any fault that is still active, even if the run failed.
        if runner is not None:
            runner.finish()
    finished_at = utc_now()

    outcomes_file = run_dir / "outcomes.json"
    outcomes = json.loads(outcomes_file.read_text()) if outcomes_file.exists() else None
    (run_dir / "run.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "mode": args.mode,
                "users": args.users,
                "spawn_rate": args.spawn_rate or args.users,
                "run_time": args.run_time,
                "user_id": args.lm_user,
                "reward_id": args.lm_reward,
                "faults": [str(fault) for fault in faults],
                "database_reset": not args.no_reset,
                "git_commit": git_commit(),
                "started_at": started_at,
                "finished_at": finished_at,
                "locust_exit_code": exit_code,
                "outcomes": outcomes,
            },
            indent=2,
        )
    )
    print(f"\nSaved run {run_id} to {run_dir}")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
