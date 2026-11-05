#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT_DIR/infra/compose.yaml")

echo "Checking Compose service state..."
"${COMPOSE[@]}" ps

echo "Checking HTTP health endpoints..."
for endpoint in \
  "http://localhost:${COORDINATOR_PORT:-8000}/health" \
  "http://localhost:${WALLET_PORT:-8001}/health" \
  "http://localhost:${CATALOG_PORT:-8002}/health" \
  "http://localhost:${AUDIT_LEDGER_PORT:-8003}/health"
do
  curl --fail --silent --show-error "$endpoint"
  echo
done

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

