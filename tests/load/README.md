# Load and fault-injection harness (LM-035)

Runs the same redemption workload against the Coordinator in either mode, can
break services on a schedule while it runs, and saves everything under a run ID.

## Run an experiment

Start the local stack, then run from the repository root:

```bash
make infra-up
uv run python -m tests.load.run --mode two_phase_commit --users 20 --run-time 30s
```

| Option | Meaning |
|---|---|
| `--mode` | `two_phase_commit` or `saga` |
| `--users` | Concurrent virtual users (the concurrency) |
| `--run-time` | How long to apply load, for example `30s` or `2m` |
| `--fault` | A scheduled fault; repeat the option for several (see below) |
| `--lm-user`, `--lm-reward` | Who redeems what (default `user-alice`, `reward-coffee`) |
| `--no-reset` | Skip the database reset |

Every run resets all databases to the seed data first. This wipes the local
Compose databases, so it only works when `LEDGERMESH_ENV` is `local`, which the
wrapper sets itself.

Saga mode creates redemptions but nothing completes them until LM-022 is merged.
In 2PC mode each user also calls the local-only `execute-2pc` endpoint.

## Faults

A fault is written `kind:target@start+length`:

```bash
uv run python -m tests.load.run --mode two_phase_commit --users 20 --run-time 30s \
    --fault pause:wallet@10s+5s \
    --fault kill:coordinator@20s+4s
```

`start` is measured from the moment the load begins. Cleanup always runs, even
if the run fails or is interrupted.

| Kind | Trigger | Cleanup | Simulates |
|---|---|---|---|
| `kill` | `docker compose kill` | `docker compose start` | Crash |
| `pause` | `docker compose pause` | `docker compose unpause` | Hang, callers time out |
| `stop` | `docker compose stop` | `docker compose start` | Clean shutdown |

Targets: `coordinator`, `wallet`, `catalog`, `audit-ledger`, `kafka`.

To cause or undo one by hand:

```bash
uv run python -m tests.load.faults inject pause wallet
uv run python -m tests.load.faults cleanup pause wallet
```

## What a run saves

Results go to `artifacts/load/<run-id>/` (not committed). The run ID is a UTC
timestamp plus the mode and user count.

| File | Contents |
|---|---|
| `run.json` | Settings, git commit, start and end times, outcomes |
| `outcomes.json` | How many redemptions ended in each final state |
| `faults.jsonl` | One line per fault step, with offsets from the load start |
| `locust_stats.csv`, `locust_stats_history.csv` | Request counts and latency |
| `locust_failures.csv` | Requests that failed, such as connection refused |

In `faults.jsonl`, `issued_offset_seconds` is when the command was sent and
`offset_seconds` is when it finished. On cleanup, `healthy_after_seconds` is how
long the service took to answer `/health` again.

## Reading the results

- Several users redeeming from the same wallet and reward collide, so some 2PC
  transactions abort even with no fault. Compare against a one-user run.
- A crash shows up as failed requests (`HTTP 0`). A transaction left in a
  non-final state such as `ABORT_DECIDED` is waiting for recovery.
- The seed has few wallets. Pick users and rewards deliberately: `user-carol`
  can afford exactly one coffee, and `reward-last-unit` has one unit left.

## Not yet covered

Network latency and cut connections (Toxiproxy), duplicate and out-of-order
events, and Saga outcomes. Saga needs LM-022.
