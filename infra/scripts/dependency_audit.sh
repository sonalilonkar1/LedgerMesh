#!/usr/bin/env bash
# Audit every locked Python dependency for known vulnerabilities (LM-042).
#
#   bash infra/scripts/dependency_audit.sh
#
# Exports both lockfiles (the root uv.lock, which the images install, and
# libs/messaging/uv.lock, which its own CI uses) with their hashes, and checks
# them with pip-audit against the PyPI advisory database and OSV. Exits
# non-zero on any known vulnerability.
set -euo pipefail

PIP_AUDIT_VERSION="${PIP_AUDIT_VERSION:-2.10.1}"

root="$(git rev-parse --show-toplevel)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# pip-audit --require-hashes rejects editable entries; the messaging library is
# the only one, and its own lockfile is audited separately below.
(cd "$root" && uv export --python 3.12 --locked --all-groups --no-emit-project --format requirements-txt) \
  | grep -v '^-e ' >"$work/root.txt"
(cd "$root/libs/messaging" && uv export --python 3.12 --locked --all-groups --no-emit-project \
  --format requirements-txt) | grep -v '^-e ' >"$work/messaging.txt"

status=0
for lock in root messaging; do
  for source in pypi osv; do
    echo "== ${lock} lockfile, ${source} advisories"
    uvx --python 3.12 "pip-audit==${PIP_AUDIT_VERSION}" -r "$work/${lock}.txt" \
      --require-hashes --disable-pip --progress-spinner off -s "$source" || status=1
  done
done
exit "$status"
