"""Locust workload for the load and fault-injection harness (LM-035).

Every virtual user repeatedly asks the Coordinator to redeem one reward. The
coordination mode and the concurrency are chosen when the run starts:

    uv run locust -f tests/load/locustfile.py --headless \
        --mode two_phase_commit --users 20 --spawn-rate 20 --run-time 30s

``--users`` is the concurrency: how many virtual users send requests at once.
Reset the data first (``LEDGERMESH_ENV=local make db-baseline``) so every run
starts from the same balances and inventory.
"""

from __future__ import annotations

import json
import os
import uuid
from collections import Counter
from pathlib import Path

from locust import HttpUser, between, events, task

MODES = ("two_phase_commit", "saga")

# How each redemption ended, e.g. {"COMMITTED": 98, "ABORTED": 2}.
outcomes: Counter[str] = Counter()


@events.init_command_line_parser.add_listener
def add_arguments(parser) -> None:
    parser.add_argument("--mode", choices=MODES, default="two_phase_commit")
    parser.add_argument("--lm-user", default="user-alice", help="user that redeems")
    parser.add_argument("--lm-reward", default="reward-coffee", help="reward being redeemed")


@events.test_stop.add_listener
def report_outcomes(environment, **_kwargs) -> None:
    mode = environment.parsed_options.mode
    print(f"\nRedemption outcomes ({mode}): {dict(outcomes)}")
    # tests/load/run.py sets LM_RUN_DIR so the tally is saved with the run.
    run_dir = os.environ.get("LM_RUN_DIR")
    if run_dir:
        (Path(run_dir) / "outcomes.json").write_text(json.dumps(dict(outcomes), indent=2))


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

        if options.mode == "two_phase_commit":
            self._execute_two_phase_commit(transaction_id)
        # Saga: nothing drives a Saga redemption yet (LM-022), so there is no
        # second call to make. Outcome tracking for Saga is added with it.

    def _execute_two_phase_commit(self, transaction_id: str) -> None:
        # In local mode the Coordinator exposes this call to run one 2PC
        # redemption to its final state and return it.
        with self.client.post(
            f"/internal/v1/redemptions/{transaction_id}/execute-2pc",
            name="execute 2PC",
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f"HTTP {response.status_code}: {response.text[:200]}")
                return
            outcomes[response.json()["status"]] += 1
