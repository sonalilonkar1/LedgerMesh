#!/usr/bin/env bash
# Scan commits for committed secrets (LM-042).
#
#   bash infra/scripts/secret_scan.sh                      # every commit on every ref
#   SCAN_RANGE=<base-sha>..<head-sha> bash infra/scripts/secret_scan.sh
#
# By default every commit on every ref is scanned (push to main, the weekly run,
# `make security-secrets`). SCAN_RANGE limits the scan to one revision range;
# CI sets it on pull requests so a PR is judged only on its own commits, not on
# other branches that happen to be fetched.
#
# A reviewed historical finding can be suppressed by its fingerprint in a
# .gitleaksignore file at the repository root (gitleaks prints the fingerprint).
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

scan_range="${SCAN_RANGE:-}"
if [[ -n "$scan_range" && ! "$scan_range" =~ ^[0-9a-f]{7,40}\.\.[0-9a-f]{7,40}$ ]]; then
  echo "SCAN_RANGE must be <sha>..<sha>, got: $scan_range" >&2
  exit 2
fi

git clone --quiet --mirror "$root" "$work/repo.git"
if [[ -n "$scan_range" ]]; then
  log_opts="$scan_range"
  echo "scanning $(git -C "$work/repo.git" rev-list "$scan_range" | wc -l | tr -d ' ') commits in $scan_range"
else
  log_opts="--all"
  echo "scanning $(git -C "$work/repo.git" rev-list --all | wc -l | tr -d ' ') commits on all refs"
fi

config=()
if [[ -f "$root/.gitleaks.toml" ]]; then
  cp "$root/.gitleaks.toml" "$work/gitleaks.toml"
  config+=(--config /work/gitleaks.toml)
fi
# The mirror has no working tree, so gitleaks cannot find .gitleaksignore itself.
if [[ -f "$root/.gitleaksignore" ]]; then
  cp "$root/.gitleaksignore" "$work/gitleaksignore"
  config+=(--gitleaks-ignore-path /work/gitleaksignore)
fi

# Same uid as the files, so git inside the container trusts the repository.
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$work:/work:ro" \
  "$GITLEAKS_IMAGE" git /work/repo.git \
  --log-opts="$log_opts" --redact --verbose --no-banner --exit-code 1 ${config[@]+"${config[@]}"}
