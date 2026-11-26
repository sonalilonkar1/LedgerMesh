# LM-038 apply and test guide

This bundle adds a three-member local etcd cluster and lease-backed admission
for new Coordinator redemptions. Apply it on a clean branch from current
`origin/main`; do not apply it on top of another feature branch.

## Apply

```bash
git status --short
git fetch origin
git switch -c feat/lm-038-coordinator-etcd-leadership origin/main
unzip -o ~/Downloads/LedgerMesh_LM038_files.zip -d .
```

If you already created the branch, switch to it instead of creating it again.
Review `git diff` before staging. The `main.py` and `compose.yaml` in this
bundle are complete versions based on the supplied LedgerMesh snapshot. If
your current `main` has additional merged changes, preserve them and merge the
LM-038 additions (imports, Coordinator lifespan startup/shutdown, the
`require_leadership` dependency on `POST /v1/redemptions`, etcd services, and
environment wiring) into those newer files rather than replacing them.

If LM-034 is already merged, keep its existing `/metrics` route and use its
shared metrics registry; remove the duplicate `/metrics` route from this
bundle's `main.py`. `leadership.py` registers both gauges with that shared
registry when available.

## Verify locally

```bash
uv sync --locked
make infra-up
make etcd-smoke
make check
make db-test
```

`make infra-up` builds the Coordinator image with the updated lockfile and
waits for the three etcd members and services. `make etcd-smoke` checks health
for every etcd member and asserts membership count is exactly three. The
Coordinator's leadership gauges are exposed at `/metrics`:

```bash
curl -fsS http://localhost:8000/metrics | grep -E 'ledgermesh_coordinator_leader|ledgermesh_etcd_quorum_available'
```

Test two Coordinator instances and failover:

```bash
docker compose -f infra/compose.yaml --profile leadership-demo up -d --wait coordinator-follower
curl -i http://localhost:8000/health
curl -i http://localhost:8004/health
```

Submit a valid redemption request to each Coordinator (use a different key
only if you intend two separate redemptions):

```bash
curl -i http://localhost:8000/v1/redemptions \\
  -H 'Content-Type: application/json' -H 'Idempotency-Key: lm038-demo' \\
  -d '{"user_id":"user-alice","reward_id":"reward-coffee","quantity":1,"coordination_mode":"saga"}'
curl -i http://localhost:8004/v1/redemptions \\
  -H 'Content-Type: application/json' -H 'Idempotency-Key: lm038-demo' \\
  -d '{"user_id":"user-alice","reward_id":"reward-coffee","quantity":1,"coordination_mode":"saga"}'
```

Exactly one instance should accept the creation (`202`); a nonleader should
return `503` with `Retry-After`. The elected instance can vary.
Then stop the current leader container, wait longer than the configured lease
TTL (9 seconds), and retry against the remaining instance; it should acquire
the lease and accept new work. Restart the stopped container and verify it
does not accept writes while another process owns the key.

To exercise quorum loss, stop two etcd members. The Coordinator should fail
closed for new redemptions and export `ledgermesh_etcd_quorum_available 0`.
Restore the members and verify the lease is reacquired. Do this only against
the local Compose cluster; never use broad volume-removal commands on a shared
or production environment.

Focused tests (also included in `make check`):

```bash
uv run pytest services/coordinator/tests/test_leadership.py services/coordinator/tests/test_leadership_api.py -q
```

The focused suite uses a fake etcd transport, so it tests lease exclusivity,
stale-owner rejection, failed renewal, and API admission without requiring
Docker. The `make etcd-smoke` and manual Compose checks validate the actual
three-member cluster.

**ADR scope note:** ADR-001 section 12 additionally describes monotonically
increasing fencing tokens that Wallet and Catalog persist and enforce. This
bundle implements LM-038's listed admission, lease, quorum, logging, and
metrics criteria; it does not change participant schemas or participant
fencing. If the team treats that ADR clause as part of issue #40's definition
of done, add the participant-token propagation and enforcement work before
marking the ADR clause complete.

## Commit and PR

```bash
git diff --check
git status --short
git add Makefile docs/leadership.md infra/.env.example infra/README.md infra/compose.yaml infra/scripts/etcd-smoke.sh pyproject.toml uv.lock services/coordinator/app/leadership.py services/coordinator/app/main.py services/coordinator/tests/test_leadership.py services/coordinator/tests/test_leadership_api.py
git commit -m "feat(coordinator): add etcd leadership lease (LM-038)"
git push -u origin feat/lm-038-coordinator-etcd-leadership
```

Create a PR from `feat/lm-038-coordinator-etcd-leadership` into `main`.
