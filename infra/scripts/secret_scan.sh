#!/usr/bin/env bash
# Scan every commit on every ref for committed secrets (LM-042).
#
#   bash infra/scripts/secret_scan.sh
#
# Runs gitleaks from its container image, pinned by digest, against a bare
# mirror of this repository. The mirror works the same from a normal clone, a
# git worktree (whose .git is only a pointer file) and CI. Findings are printed
# redacted; the script exits non-zero if there are any.
set -euo pipefail

# v8.30.1, digest checked against ghcr.io on 2026-11-27 (docs/security.md).
GITLEAKS_IMAGE="${GITLEAKS_IMAGE:-ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f}"

root="$(git rev-parse --show-toplevel)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

git clone --quiet --mirror "$root" "$work/repo.git"
echo "scanning $(git -C "$work/repo.git" rev-list --all | wc -l | tr -d ' ') commits on all refs"

config=()
if [[ -f "$root/.gitleaks.toml" ]]; then
  cp "$root/.gitleaks.toml" "$work/gitleaks.toml"
  config=(--config /work/gitleaks.toml)
fi

# Same uid as the files, so git inside the container trusts the repository.
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$work:/work:ro" \
  "$GITLEAKS_IMAGE" git /work/repo.git \
  --log-opts=--all --redact --no-banner --exit-code 1 ${config[@]+"${config[@]}"}
