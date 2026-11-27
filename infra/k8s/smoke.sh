#!/usr/bin/env bash
# LedgerMesh Kubernetes smoke test (LM-041).
#
# Proves that a deployed cluster completes one redemption in each transaction
# mode, end to end:
#   - 2PC:  POST /v1/redemptions -> recovery worker -> COMMITTED
#   - Saga: POST /v1/redemptions -> Saga workers over Kafka -> COMPLETED
# and that, for each, the wallet balance and reward inventory moved by exactly
# one redemption and the Audit Ledger recorded the terminal event (outbox ->
# Kafka -> audit consumer).
#
# Uses only kubectl, curl, python3, base64 and uv. Every wait is bounded.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER="${KIND_CLUSTER:-lm041}"
export KUBECONFIG="${KUBECONFIG_LM041:-/tmp/${CLUSTER}.kubeconfig}"
KCTX="${KUBE_CONTEXT:-kind-${CLUSTER}}"
NS=ledgermesh
# High local ports so the smoke never clashes with Compose (8000-8003, 5432x).
COORDINATOR_PORT=18000
WALLET_PORT=18001
CATALOG_PORT=18002
AUDIT_PORT=18003
WALLET_DB_PORT=15432
CATALOG_DB_PORT=15433
USER_ID="${SMOKE_USER_ID:-user-alice}"
REWARD_ID="${SMOKE_REWARD_ID:-reward-coffee}"
# 0 (default): 2PC is driven only by the Coordinator's recovery worker, the path
# a hosted deployment uses. 1: also call the internal execute-2pc route, which
# exists only when LEDGERMESH_ENV is local or test.
SMOKE_EXECUTE_2PC="${SMOKE_EXECUTE_2PC:-0}"
# Seconds to wait for each redemption to reach a terminal state. Right after a
# Kafka or PostgreSQL pod restart the workers can take ~2 minutes to reconnect,
# so raise this when running the smoke straight after a fault test.
SMOKE_TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-90}"

kc() { kubectl --context "$KCTX" -n "$NS" "$@"; }
json() { python3 -c "import json, sys; d = json.load(sys.stdin); print($1)"; }

if [[ ! -f "$KUBECONFIG" ]]; then
  echo "kubeconfig $KUBECONFIG not found: run make k8s-up first, or" >&2
  echo "  kind export kubeconfig --name $CLUSTER --kubeconfig $KUBECONFIG" >&2
  exit 1
fi

WORKDIR="$(mktemp -d)"
SEED_ERR="$WORKDIR/seed.err"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

echo "== Preflight: every workload rolled out"
for sts in kafka coordinator-db wallet-db catalog-db audit-ledger-db; do
  kc rollout status "statefulset/$sts" --timeout=180s
done
for deploy in coordinator wallet catalog audit-ledger \
  coordinator-saga wallet-saga catalog-saga audit-consumer; do
  kc rollout status "deployment/$deploy" --timeout=180s
done
# The topic Job may have been deleted (for example before a re-apply); the
# worker init containers run the same idempotent bootstrap, so only wait for the
# Job when it exists.
if kc get job/kafka-topics >/dev/null 2>&1; then
  kc wait --for=condition=complete job/kafka-topics --timeout=180s
fi

echo "== Prepared-transaction support (as infra/scripts/smoke-test.sh)"
for entry in "coordinator-db coordinator" "wallet-db wallet" "catalog-db catalog" \
  "audit-ledger-db audit_ledger"; do
  read -r sts db <<<"$entry"
  value="$(kc exec "$sts-0" -c postgres -- psql -U "$db" -d "$db" -Atqc "SHOW max_prepared_transactions")"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$sts has invalid max_prepared_transactions=$value" >&2
    exit 1
  fi
  echo "$sts max_prepared_transactions=$value"
done

echo "== Port-forwards"
# kubectl runs directly in the background (not through the kc function), so
# $! is kubectl's own PID and cleanup really stops the forward. Output goes to
# a log so a forward that cannot bind its port is visible.
forward() {
  kubectl --context "$KCTX" -n "$NS" port-forward "$1" "$2" \
    >"$WORKDIR/pf-${2%%:*}.log" 2>&1 &
  PIDS+=("$!")
}
forward svc/coordinator "$COORDINATOR_PORT:8000"
forward svc/wallet "$WALLET_PORT:8001"
forward svc/catalog "$CATALOG_PORT:8002"
forward svc/audit-ledger "$AUDIT_PORT:8003"
DB_PIDS=()
forward_databases() {
  # Stop the old forwards and wait until they have released their ports.
  for pid in "${DB_PIDS[@]:-}"; do
    [[ -n "$pid" ]] || continue
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  DB_PIDS=()
  forward svc/wallet-db "$WALLET_DB_PORT:5432"
  DB_PIDS+=("$!")
  forward svc/catalog-db "$CATALOG_DB_PORT:5432"
  DB_PIDS+=("$!")
}
forward_databases

check_health() {
  local expected="$1" port="$2" response="" actual=""
  for _ in $(seq 1 30); do
    if response="$(curl --fail --silent --show-error "http://localhost:$port/health" 2>/dev/null)"; then
      actual="$(json 'd.get("service", "")' <<<"$response")"
      if [[ "$actual" == "$expected" ]]; then
        echo "$response"
        return 0
      fi
    fi
    sleep 1
  done
  echo "Health check failed for $expected on localhost:$port (got '$actual')" >&2
  return 1
}
check_health coordinator "$COORDINATOR_PORT"
check_health wallet "$WALLET_PORT"
check_health catalog "$CATALOG_PORT"
check_health audit-ledger "$AUDIT_PORT"

# $1 local port, $2 PID of the kubectl port-forward that should own it. The
# forward must still be running once the port answers; otherwise something
# else (an earlier forward, a local PostgreSQL) holds the port.
wait_for_port() {
  for _ in $(seq 1 30); do
    if ! kill -0 "$2" 2>/dev/null; then
      echo "port-forward to localhost:$1 exited:" >&2
      cat "$WORKDIR/pf-$1.log" >&2 || true
      return 1
    fi
    python3 -c "import socket, sys; socket.create_connection(('127.0.0.1', int(sys.argv[1])), 1)" "$1" \
      2>/dev/null && kill -0 "$2" 2>/dev/null && return 0
    sleep 1
  done
  echo "port-forward on localhost:$1 did not open" >&2
  cat "$WORKDIR/pf-$1.log" >&2 || true
  return 1
}
wait_for_databases() {
  wait_for_port "$WALLET_DB_PORT" "${DB_PIDS[0]}"
  wait_for_port "$CATALOG_DB_PORT" "${DB_PIDS[1]}"
}
wait_for_databases

echo "== Seed (LM-006, insert-only, safe to repeat)"
PW="$(kc get secret -l app.kubernetes.io/component=db-credentials \
  -o jsonpath='{.items[0].data.POSTGRES_PASSWORD}' | base64 -d)"
seed() {
  (
    cd "$ROOT_DIR"
    LEDGERMESH_ENV=local \
      WALLET_DATABASE_URL="postgresql+psycopg://wallet:$PW@localhost:$WALLET_DB_PORT/wallet" \
      CATALOG_DATABASE_URL="postgresql+psycopg://catalog:$PW@localhost:$CATALOG_DB_PORT/catalog" \
      uv run --quiet python -m infra.scripts.seed seed wallet catalog
  )
}
# A port-forward opened while a database pod was restarting can refuse
# connections; re-open the forwards and retry (seeding is insert-only).
seeded=0
for attempt in 1 2 3; do
  if seed 2>"$SEED_ERR"; then
    seeded=1
    break
  fi
  echo "seed attempt $attempt failed; re-opening database port-forwards" >&2
  tail -n 1 "$SEED_ERR" >&2
  forward_databases
  wait_for_databases
  sleep 3
done
if [[ "$seeded" != 1 ]]; then
  cat "$SEED_ERR" >&2
  exit 1
fi

balance() {
  curl --fail --silent --show-error "http://localhost:$WALLET_PORT/v1/wallets/$USER_ID" |
    json 'd["available_points"]'
}
reward_field() {
  curl --fail --silent --show-error "http://localhost:$CATALOG_PORT/v1/rewards/$REWARD_ID" |
    json "d[\"$1\"]"
}

diagnose() {
  local id="$1"
  echo "-- transitions for $id" >&2
  curl --silent "http://localhost:$COORDINATOR_PORT/internal/v1/redemptions/$id/transitions" >&2 || true
  echo >&2
  for deploy in coordinator coordinator-saga wallet-saga catalog-saga audit-consumer; do
    echo "-- logs deployment/$deploy (last 40 lines)" >&2
    kc logs "deployment/$deploy" --tail=40 >&2 || true
  done
}

RESULTS=()
run_mode() {
  local mode="$1" expected="$2" terminal="$3"
  echo "== $mode redemption"
  local before_points before_inventory cost
  before_points="$(balance)"
  before_inventory="$(reward_field available_inventory)"
  cost="$(reward_field point_cost)"
  echo "before: $USER_ID available_points=$before_points; $REWARD_ID available_inventory=$before_inventory point_cost=$cost"

  local key="k8s-smoke-$mode-$(date +%s)-$RANDOM"
  local body response code id
  body="{\"user_id\":\"$USER_ID\",\"reward_id\":\"$REWARD_ID\",\"quantity\":1,\"coordination_mode\":\"$mode\"}"
  response="$(curl --silent --show-error -w '\n%{http_code}' -X POST \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
    -d "$body" "http://localhost:$COORDINATOR_PORT/v1/redemptions")"
  code="${response##*$'\n'}"
  response="${response%$'\n'*}"
  if [[ "$code" != 202 ]]; then
    echo "POST /v1/redemptions returned $code: $response" >&2
    exit 1
  fi
  id="$(json 'd["transaction_id"]' <<<"$response")"
  echo "accepted transaction_id=$id status=$(json 'd["status"]' <<<"$response")"

  if [[ "$mode" == two_phase_commit && "$SMOKE_EXECUTE_2PC" == 1 ]]; then
    curl --silent --show-error -X POST \
      "http://localhost:$COORDINATOR_PORT/internal/v1/redemptions/$id/execute-2pc" >/dev/null || true
  fi

  local status="" deadline=$((SECONDS + SMOKE_TIMEOUT_SECONDS))
  while ((SECONDS < deadline)); do
    status="$(curl --fail --silent "http://localhost:$COORDINATOR_PORT/v1/redemptions/$id" |
      json 'd["status"]' || true)"
    if [[ " $terminal " == *" $status "* ]]; then
      break
    fi
    sleep 2
  done
  echo "final status: $status"
  if [[ "$status" != "$expected" ]]; then
    echo "$mode redemption $id ended as '$status', expected $expected" >&2
    diagnose "$id"
    exit 1
  fi

  local after_points after_inventory
  after_points="$(balance)"
  after_inventory="$(reward_field available_inventory)"
  echo "after:  $USER_ID available_points=$after_points; $REWARD_ID available_inventory=$after_inventory"
  if ((after_points != before_points - cost)); then
    echo "balance moved from $before_points to $after_points, expected -$cost" >&2
    exit 1
  fi
  if ((after_inventory != before_inventory - 1)); then
    echo "inventory moved from $before_inventory to $after_inventory, expected -1" >&2
    exit 1
  fi

  # 2PC writes redemption.<status> with payload.status (repository.py:309-345);
  # the Saga writes coordinator.transitioned with payload.to_status (saga.py:86-106).
  local events found="0 -"
  deadline=$((SECONDS + SMOKE_TIMEOUT_SECONDS))
  while ((SECONDS < deadline)); do
    events="$(curl --fail --silent "http://localhost:$AUDIT_PORT/v1/transactions/$id/events" || echo '[]')"
    found="$(EXPECTED="$expected" python3 -c '
import json, os, sys
events = json.load(sys.stdin)
want = os.environ["EXPECTED"]
hits = [e["envelope"]["event_type"] for e in events
        if want in (e["envelope"]["payload"].get("status"), e["envelope"]["payload"].get("to_status"))]
print(len(events), hits[0] if hits else "-")
' <<<"$events")"
    if [[ "${found#* }" != "-" ]]; then
      break
    fi
    sleep 2
  done
  if [[ "${found#* }" == "-" ]]; then
    echo "audit ledger has no $expected event for $id (events: ${found%% *})" >&2
    diagnose "$id"
    exit 1
  fi
  echo "audit: ${found%% *} event(s), terminal event ${found#* }"
  RESULTS+=("$mode=$status")
}

run_mode two_phase_commit COMMITTED "COMMITTED ABORTED"
run_mode saga COMPLETED "COMPLETED FAILED COMPENSATED"

echo "LedgerMesh Kubernetes smoke test passed (2PC=COMMITTED, Saga=COMPLETED)."
