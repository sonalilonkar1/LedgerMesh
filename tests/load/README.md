# Load and fault-injection harness (LM-035)

Runs the same redemption workload against the Coordinator in either mode, can
break services on a schedule while it runs, and saves everything under a run ID.

## Run an experiment

Start the local stack, then run from the repository root:

```bash
make infra-up
LEDGERMESH_ENV=local uv run python -m tests.load.run --mode two_phase_commit --users 20 --run-time 30s
```

| Option | Meaning |
|---|---|
| `--mode` | `two_phase_commit` or `saga` |
| `--users` | Concurrent virtual users (the concurrency) |
| `--run-time` | How long to apply load, for example `30s` or `2m` |
| `--settle-timeout` | How long to wait afterwards for transactions to reach a final state (default `30s`) |
| `--fault` | A scheduled fault; repeat the option for several (see below) |
| `--lm-user`, `--lm-reward` | Who redeems what (default `user-alice`, `reward-coffee`) |
| `--no-reset` | Skip the database reset |

### The reset is destructive

Every run first resets all four databases to the seed data. The wrapper refuses
unless you set `LEDGERMESH_ENV=local` (or `test`) yourself, and it refuses if any
database URL, including a `<SERVICE>_DATABASE_URL` override, points at another
machine. Use `--no-reset` to run against whatever data is there.

### Workloads

- **2PC** (`full_2pc_execution`): each user creates a redemption and then calls
  the local-only `execute-2pc` endpoint.
- **Saga** (`submission_only`): redemptions are created but nothing completes them
  until LM-022 is merged, so Saga results are not comparable with 2PC yet.

### Exit status

`0` is a valid run and `2` means the experiment itself went wrong, for example a
fault could not be injected, was cut short, or a service never became healthy
again. `run.json` then has `"valid": false` and lists the reasons. Request
failures caused by a fault that did run are expected and do not change the status.

## Faults

A fault is written `kind:target@start+length`:

```bash
LEDGERMESH_ENV=local uv run python -m tests.load.run --mode two_phase_commit --users 20 --run-time 30s \
    --fault pause:wallet@10s+5s \
    --fault kill:coordinator@20s+4s
```

`start` is measured from the moment the load begins. Cleanup always runs, even
if the run fails, is interrupted, or the injection command itself fails or times
out (the service may already have been affected). Two faults on the same service
may not overlap in the schedule. Because a command such as `stop` can run longer
than planned, the runner also allows only one fault at a time per service, from
injection until the service is healthy again; a later fault that would start
earlier than that is skipped and the run is marked invalid.

A service counts as recovered when `/health` answers again, or, for Kafka, when
Docker reports it healthy. Starting a container is not enough: Kafka took about
six seconds to become ready after `docker compose start`.

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
timestamp, the mode, the user count and a random suffix.

| File | Contents |
|---|---|
| `run.json` | Settings, validity and problems, starting balances, git commit, times, final outcomes |
| `submissions.jsonl` | One line per redemption request sent, with how creation ended and the final state |
| `faults.jsonl` | One line per fault step, with offsets from the load start |
| `locust_stats.csv`, `locust_stats_history.csv` | Request counts and latency |
| `locust_failures.csv` | Requests that failed, such as connection refused |

Every request is written down before it is sent, so a lost response cannot hide
a transaction. `submissions` in `run.json` counts requests by how creation ended:

| Value | Meaning |
|---|---|
| `accepted` | The Coordinator answered 202 |
| `rejected_http_<code>` | The Coordinator answered and refused; nothing was created |
| `not_sent` | The connection could not be made, so the request was never delivered |
| `recovered_existing` | No answer arrived, but a read-only lookup by Idempotency-Key found the redemption in the Coordinator database |
| `never_created` | No answer arrived and the lookup found nothing, so the request never took effect |
| `unknown` | The lookup was not possible (database unreachable or not local); the run is marked invalid |

Resolving a lost answer only reads the Coordinator database, and only a local
one. It never sends a request to the Coordinator, so settling a run cannot create
a redemption that recovery might then execute.

After the load, every transaction that exists is polled until it reaches a final
state (`COMMITTED`, `ABORTED`, `COMPLETED`, `FAILED` or `COMPENSATED`).
`final_outcomes` counts those; `unresolved` counts the ones still not final when
`--settle-timeout` ended, kept separate from business failures. The timeout is a
single deadline for all of this work, including each individual request.

Latency: the Locust CSV files give request latency, including the `execute 2PC`
call, which is the work of one 2PC redemption. The run does not store a
per-transaction completion time, because polling happens after the load and would
only measure how long the run lasted. End-to-end latency for Saga needs the
terminal state to be observed during the workload, which has to wait for LM-022.

In `faults.jsonl`, `issued_offset_seconds` is when the command was sent and
`offset_seconds` is when it finished. On cleanup, `healthy_after_seconds` is how
long the service took to answer `/health` again.

## Reading the results

- `starting_state` in `run.json` shows what the seed allowed: the default wallet
  affords 100 coffees and the reward has 100 in stock. A long run eventually
  measures insufficient-points or sold-out rejections, not successful redemptions.
  Compare `final_outcomes` with `max_successful_redemptions`.
- Several users redeeming from the same wallet and reward collide, so some 2PC
  transactions abort even with no fault. Compare against a one-user run.
- A crash shows up as failed requests (`HTTP 0`). A transaction that is not final
  yet is waiting for recovery.

## Not yet covered

Network latency and cut connections (Toxiproxy), duplicate and out-of-order
events, scenario runs for competing redemptions (LM-045), and Saga outcomes
(needs LM-022).
