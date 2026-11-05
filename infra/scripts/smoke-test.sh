#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="$ROOT_DIR/infra/.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

COMPOSE=(docker compose -f "$ROOT_DIR/infra/compose.yaml")

check_health() {
  local expected_service="$1"
  local port="$2"
  local endpoint="http://localhost:${port}/health"
  local response
  local actual_service

  if ! response="$(curl --fail --silent --show-error "$endpoint")"; then
    echo "Health check failed for $expected_service at $endpoint" >&2
    echo "Another application may be using port $port." >&2
    return 1
  fi

  if ! actual_service="$(
    python3 -c \
      'import json, sys; print(json.load(sys.stdin).get("service", ""))' \
      <<<"$response"
  )"; then
    echo "Invalid JSON returned by $endpoint: $response" >&2
    return 1
  fi

  if [[ "$actual_service" != "$expected_service" ]]; then
    echo "Expected $expected_service at $endpoint, but response identified '$actual_service'." >&2
    echo "Another application may be using port $port." >&2
    return 1
  fi

  echo "$response"
}

echo "Checking Compose service state..."
"${COMPOSE[@]}" ps

echo "Checking HTTP health endpoints..."
check_health "coordinator" "${COORDINATOR_PORT:-8000}"
check_health "wallet" "${WALLET_PORT:-8001}"
check_health "catalog" "${CATALOG_PORT:-8002}"
check_health "audit-ledger" "${AUDIT_LEDGER_PORT:-8003}"

echo "Checking Kafka broker..."
"${COMPOSE[@]}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list >/dev/null

echo "Checking PostgreSQL readiness and prepared-transaction support..."
for entry in \
  "coordinator-db coordinator coordinator" \
  "wallet-db wallet wallet" \
  "catalog-db catalog catalog" \
  "audit-ledger-db audit_ledger audit_ledger"
do
  read -r service user database <<<"$entry"
  "${COMPOSE[@]}" exec -T "$service" pg_isready -U "$user" -d "$database" >/dev/null
  max_prepared="$("${COMPOSE[@]}" exec -T "$service" \
    psql -U "$user" -d "$database" -Atqc "SHOW max_prepared_transactions")"
  if [[ ! "$max_prepared" =~ ^[1-9][0-9]*$ ]]; then
    echo "$service has invalid max_prepared_transactions=$max_prepared" >&2
    exit 1
  fi
done

echo "LedgerMesh infrastructure smoke test passed."

