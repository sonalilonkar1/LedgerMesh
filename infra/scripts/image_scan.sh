#!/usr/bin/env bash
# Scan a built service image with the same Trivy version and policy as CI (LM-042).
#
#   bash infra/scripts/image_scan.sh IMAGE
#
# Policy (from .github/workflows/image-build.yml): fail on HIGH or CRITICAL
# findings that have a fix, with exceptions only in .trivyignore. The image is
# saved to a tarball first, because Trivy cannot always read a cross-platform
# image straight from the Docker daemon.
set -euo pipefail

image="$1"
# v0.70.0 is what aquasecurity/trivy-action v0.36.0 (pinned in CI) installs.
TRIVY_IMAGE="${TRIVY_IMAGE:-aquasec/trivy:0.70.0@sha256:be1190afcb28352bfddc4ddeb71470835d16462af68d310f9f4bca710961a41e}"
TRIVY_CACHE_VOLUME="${TRIVY_CACHE_VOLUME:-ledgermesh-trivy-cache}"

root="$(git rev-parse --show-toplevel)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

docker save -o "$work/image.tar" "$image"
cp "$root/.trivyignore" "$work/.trivyignore"

docker run --rm \
  -v "$TRIVY_CACHE_VOLUME:/root/.cache" \
  -v "$work:/work:ro" \
  "$TRIVY_IMAGE" image --skip-version-check --input /work/image.tar \
  --severity HIGH,CRITICAL --ignore-unfixed --ignorefile /work/.trivyignore \
  --exit-code 1 --format table
