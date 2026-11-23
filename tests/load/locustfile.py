"""Locust workload for the load and fault-injection harness (LM-035).

Every virtual user repeatedly asks the Coordinator to redeem one reward. The
coordination mode and the concurrency are chosen when the run starts:

    uv run locust -f tests/load/locustfile.py --headless \
        --mode two_phase_commit --users 20 --spawn-rate 20 --run-time 30s

``--users`` is the concurrency: how many virtual users send requests at once.
Reset the data first (``LEDGERMESH_ENV=local make db-baseline``) so every run
starts from the same balances and inventory.

The workload only submits and drives redemptions. It records every transaction
the Coordinator accepted in ``transactions.jsonl``; ``tests/load/run.py`` then
checks how each one really ended, so a crash during a request cannot hide a
transaction that recovery later completes.

Saga mode is submission-only until LM-022 can drive a Saga to completion: it
creates redemptions but nothing finishes them.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

from locust import HttpUser, between, events, task

MODES = ("two_phase_commit", "saga")

# Every redemption the Coordinator accepted, whatever happened to the follow-up call.
accepted: list[dict] = []


def run_dir() -> Path | None:
    value = os.environ.get("LM_RUN_DIR")  # set by tests/load/run.py
    return Path(value) if value else None


@events.init_command_line_parser.add_listener
def add_arguments(parser) -> None:
    parser.add_argument("--mode", choices=MODES, default="two_phase_commit")
    parser.add_argument("--lm-user", default="user-alice", help="user that redeems")
    parser.add_argument("--lm-reward", default="reward-coffee", help="reward being redeemed")


@events.test_start.add_listener
def mark_load_start(environment, **_kwargs) -> None:
    # Faults are scheduled relative to this moment, not to when Locust launched.
    directory = run_dir()
    if directory:
        (directory / "load_started.txt").write_text(str(time.time()))
    if environment.parsed_options.mode == "saga":
        print("Saga mode is submission-only: redemptions are created but nothing completes them.")


@events.test_stop.add_listener
def save_transactions(environment, **_kwargs) -> None:
    print(f"\nCoordinator accepted {len(accepted)} redemptions ({environment.parsed_options.mode})")
    directory = run_dir()
    if directory:
        lines = (json.dumps(entry) for entry in accepted)
        (directory / "transactions.jsonl").write_text("".join(f"{line}\n" for line in lines))


class RedemptionUser(HttpUser):
    host = "http://localhost:8000"
    wait_time = between(0.2, 0.6)

    @task
    def redeem(self) -> None:
        options = self.environment.parsed_options
        body = {
            "user_id": options.lm_user,
            "reward_id": options.lm_reward,
            "quantity": 1,
            "coordination_mode": options.mode,
        }
        # A new key per request: a repeated key would return the old redemption.
        headers = {"Idempotency-Key": uuid.uuid4().hex}

        with self.client.post(
            "/v1/redemptions",
            json=body,
            headers=headers,
            name="create redemption",
            catch_response=True,
        ) as response:
            if response.status_code != 202:
                response.failure(f"HTTP {response.status_code}: {response.text[:200]}")
                return
            transaction_id = response.json()["transaction_id"]

        # In 2PC mode this stays "not_finished" if the run stops mid-call; the
        # transaction is still tracked and run.py reports how it really ended.
        entry = {
            "transaction_id": transaction_id,
            "submitted_at": time.time(),
            "execute_call": "not_finished"
            if options.mode == "two_phase_commit"
            else "not_applicable",
        }
        accepted.append(entry)
        if options.mode == "two_phase_commit":
            entry["execute_call"] = self._execute_two_phase_commit(transaction_id)
        # Saga: nothing drives a Saga redemption yet (LM-022), so there is no
        # second call to make.

    def _execute_two_phase_commit(self, transaction_id: str) -> str:
        # In local mode the Coordinator exposes this call to run one 2PC
        # redemption to its final state. If it fails, the transaction stays in
        # the list and run.py checks later how it actually ended.
        with self.client.post(
            f"/internal/v1/redemptions/{transaction_id}/execute-2pc",
            name="execute 2PC",
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f"HTTP {response.status_code}: {response.text[:200]}")
                return f"failed_http_{response.status_code}"
            return "ok"
