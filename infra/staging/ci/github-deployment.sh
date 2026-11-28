#!/usr/bin/env bash
# Record staging deployments in GitHub for the exact commit deployed (LM-049).
#
#   github-deployment.sh create <sha> <description>     prints the deployment id
#   github-deployment.sh status <id> <state> <description>
#
# The workflows use the GitHub environment `staging` with `deployment: false`
# (for its variables, branch rule and OIDC), and record deployments here
# instead. An automatic environment deployment would carry the workflow's own
# commit, which for workflow_run and rollbacks is not the commit deployed.
#
# Environment: GH_TOKEN (deployments: write), GITHUB_REPOSITORY, and for
# `status` STAGING_URL (environment_url) and RUN_URL (log_url).
# <state>: in_progress | success | failure | error | inactive.
# A `success` status marks earlier staging deployments inactive, so the
# environment shows which commit is live.
set -euo pipefail

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

die() {
  echo "::error::github-deployment: $*" >&2
  exit 1
}

usage="usage: github-deployment.sh create <sha> <description> | status <id> <state> <description>"
[[ $# -ge 1 ]] || die "$usage"
description="${!#}"
[[ ${#description} -le 140 ]] || die "description is longer than 140 characters"

case "$1" in
  create)
    [[ $# -eq 3 ]] || die "$usage"
    [[ "$2" =~ ^[0-9a-f]{40}$ ]] || die "invalid sha"
    # create_deployment <ref> <sha> <description>: prints the id; gh's error
    # goes to $err_file.
    err_file="$(mktemp)"
    trap 'rm -f "$err_file"' EXIT
    create_deployment() {
      local body
      body="$(python3 -c '
import json, sys
print(json.dumps({
    "ref": sys.argv[1],
    "environment": "staging",
    "description": sys.argv[3],
    # The exact revision, whatever ref GitHub accepted.
    "payload": {"revision": sys.argv[2]},
    "auto_merge": False,
    # The commit was already gated by core-ci and the image workflow.
    "required_contexts": [],
    "production_environment": False,
    "transient_environment": False,
}))' "$1" "$2" "$3")"
      gh api --method POST "repos/$GITHUB_REPOSITORY/deployments" --input - --jq .id \
        <<<"$body" 2>"$err_file"
    }
    # The commit itself first, so GitHub shows exactly what was deployed. The
    # environment's deployment branch rule (main only) can reject a bare SHA
    # ("... is not allowed to deploy to staging due to environment protection
    # rules"); then record it against `main`, whose branch rule it passes, and
    # carry the exact SHA in the description and payload.revision. Any other
    # error fails.
    status=0
    out="$(create_deployment "$2" "$2" "$3")" || status=$?
    if [[ "$status" -ne 0 ]]; then
      grep -q "not allowed to deploy" "$err_file" \
        || die "could not create the deployment: $(cat "$err_file")"
      echo "::notice::The staging environment's branch rule rejected ref $2; recording the deployment against main with revision $2" >&2
      out="$(create_deployment main "$2" "$3")" \
        || die "could not create the deployment: $(cat "$err_file")"
    fi
    [[ "$out" =~ ^[0-9]+$ ]] || die "unexpected deployment id"
    echo "$out"
    ;;
  status)
    [[ $# -eq 4 ]] || die "$usage"
    [[ "$2" =~ ^[0-9]+$ ]] || die "invalid deployment id"
    case "$3" in in_progress | success | failure | error | inactive) ;; *) die "invalid state" ;; esac
    : "${STAGING_URL:?STAGING_URL is required}"
    : "${RUN_URL:?RUN_URL is required}"
    body="$(python3 -c '
import json, sys
print(json.dumps({
    "state": sys.argv[1],
    "description": sys.argv[2],
    "environment_url": sys.argv[3],
    "log_url": sys.argv[4],
    "auto_inactive": True,
}))' "$3" "$4" "$STAGING_URL" "$RUN_URL")"
    gh api --method POST "repos/$GITHUB_REPOSITORY/deployments/$2/statuses" --input - --silent <<<"$body" \
      || die "could not set deployment $2 to $3"
    ;;
  *) die "unknown command: $1" ;;
esac
