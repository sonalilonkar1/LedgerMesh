#!/usr/bin/env bash
# Create (or reuse) the kind cluster "lm041", build and load the four service
# images, apply the kind overlay and wait for everything to be ready (LM-041).
#
# The kubeconfig is written to a separate file, so your default kubectl context
# is never changed. Override with KIND_CLUSTER, KUBECONFIG_LM041 or KUBE_CONTEXT,
# and pick the node image with KIND_NODE_IMAGE (e.g. kindest/node:v1.35.0).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OVERLAY="$ROOT_DIR/infra/k8s/overlays/kind"
CLUSTER="${KIND_CLUSTER:-lm041}"
export KUBECONFIG="${KUBECONFIG_LM041:-/tmp/${CLUSTER}.kubeconfig}"
KCTX="${KUBE_CONTEXT:-kind-${CLUSTER}}"
NS=ledgermesh
kc() { kubectl --context "$KCTX" "$@"; }

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "Creating kind cluster $CLUSTER (kubeconfig: $KUBECONFIG)..."
  kind create cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" \
    --config "$OVERLAY/kind-cluster.yaml" --wait 120s \
    ${KIND_NODE_IMAGE:+--image "$KIND_NODE_IMAGE"}
else
  kind export kubeconfig --name "$CLUSTER" --kubeconfig "$KUBECONFIG"
fi

if [[ "${SKIP_IMAGE_BUILD:-0}" != 1 ]]; then
  echo "Building service images (make images-build)..."
  make -C "$ROOT_DIR" images-build
fi
REVISION="$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
for name in coordinator wallet catalog audit-ledger; do
  docker tag "ledgermesh-$name:$REVISION" "ledgermesh-$name:local"
  kind load docker-image "ledgermesh-$name:local" --name "$CLUSTER"
done

if [[ ! -f "$OVERLAY/.env.secrets" ]]; then
  echo "Generating $OVERLAY/.env.secrets (gitignored)..."
  umask 077
  printf 'POSTGRES_PASSWORD=%s\n' "$(openssl rand -hex 24)" >"$OVERLAY/.env.secrets"
fi

# A Job's pod template is immutable; the bootstrap is idempotent, so re-run it.
kc -n "$NS" delete job kafka-topics --ignore-not-found
kc apply -k "$OVERLAY"

echo "Waiting for Kafka and PostgreSQL..."
for sts in kafka coordinator-db wallet-db catalog-db audit-ledger-db; do
  kc -n "$NS" rollout status "statefulset/$sts" --timeout=300s
done
kc -n "$NS" wait --for=condition=complete job/kafka-topics --timeout=300s
echo "Waiting for APIs and workers..."
for deploy in coordinator wallet catalog audit-ledger \
  coordinator-saga wallet-saga catalog-saga audit-consumer; do
  kc -n "$NS" rollout status "deployment/$deploy" --timeout=300s
done
kc -n "$NS" get pods -o wide
echo "LedgerMesh is running in kind cluster $CLUSTER. Next: make k8s-smoke"
