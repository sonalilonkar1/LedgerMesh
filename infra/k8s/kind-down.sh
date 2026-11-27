#!/usr/bin/env bash
# Delete only the LedgerMesh kind cluster (LM-041). Other clusters and your
# default kubectl context are left alone.
set -euo pipefail
CLUSTER="${KIND_CLUSTER:-lm041}"
KUBECONFIG_FILE="${KUBECONFIG_LM041:-/tmp/${CLUSTER}.kubeconfig}"
kind delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE"
rm -f "$KUBECONFIG_FILE"
