#!/usr/bin/env bash
# Decide whether a deploy-staging run may deploy, and what (LM-049).
#
# Runs in the `gate` job of .github/workflows/deploy-staging.yml, from a
# checkout of main with full history. Everything comes from the environment,
# never from interpolation into the script:
#
#   EVENT            github.event_name (workflow_run | workflow_dispatch)
#   GITHUB_REPOSITORY, GITHUB_REF, GITHUB_OUTPUT, GH_TOKEN (actions: read)
#   GHCR_USER, GHCR_TOKEN   for infra/scripts/ghcr_digest.sh (packages: read)
#   workflow_run:     RUN_ID, RUN_CONCLUSION, RUN_EVENT, RUN_BRANCH, RUN_REPO,
#                     RUN_TITLE (the triggering "Service images" run; its
#                     display title, set by `run-name` in images.yml, is
#                     "Service images <sha of the commit it built>")
#   workflow_dispatch: INPUT_SHA
#
# Writes deploy=true|false, sha and digests to $GITHUB_OUTPUT.
#
# A "Service images" run that did not publish (core-ci failed, so the publish
# job was skipped but the run still concluded success; or a pull request)
# is not an error: the run ends green with deploy=false and a notice. Anything
# that looks wrong about the commit or its images fails the job.
#
# The revision comes from RUN_TITLE, never from the run's head_sha: a
# workflow_run-triggered run's head_sha is main's tip when it was queued,
# not the commit core-ci tested and images.yml built
# (github.event.workflow_run.head_sha in images.yml), so after quick merges
# it names a newer commit whose images may not exist yet.
#
# A workflow_run deploys only main's current tip. Image runs for consecutive
# commits run in parallel and finish in any order; a run for an older commit
# skips with a notice instead of deploying over a newer one. The tip's own
# run deploys it (core-ci and images.yml run for every push to main). A
# manual workflow_dispatch may still deploy any published commit on main,
# for recovery.
#
# LEDGERMESH_GHCR_DIGEST overrides the digest lookup command (tests only).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
GHCR_DIGEST="${LEDGERMESH_GHCR_DIGEST:-$ROOT/infra/scripts/ghcr_digest.sh}"
IMAGES=(coordinator wallet catalog audit-ledger)
SHA_RE='^[0-9a-f]{40}$'

: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
: "${EVENT:?EVENT is required}"

fail() {
  echo "::error::$*" >&2
  exit 1
}

skip() {
  echo "::notice::Not deploying: $*"
  echo "deploy=false" >>"$GITHUB_OUTPUT"
  exit 0
}

case "$EVENT" in
  workflow_run)
    # These come from the triggering run. The workflow's `branches: [main]`
    # filter matches head_branch only, which a fork's pull request can also
    # call "main", so check the event and the head repository too.
    [[ "${RUN_CONCLUSION:-}" == "success" ]] || skip "Service images concluded '${RUN_CONCLUSION:-}'"
    [[ "${RUN_EVENT:-}" == "workflow_run" ]] || skip "Service images ran for '${RUN_EVENT:-}', not for core-ci on main"
    [[ "${RUN_BRANCH:-}" == "main" ]] || skip "Service images ran for branch '${RUN_BRANCH:-}'"
    [[ "${RUN_REPO:-}" == "$GITHUB_REPOSITORY" ]] || skip "Service images ran for repository '${RUN_REPO:-}'"
    [[ "${RUN_ID:-}" =~ ^[0-9]+$ ]] || fail "invalid run id"
    title_re='^Service images ([0-9a-f]{40})$'
    [[ "${RUN_TITLE:-}" =~ $title_re ]] \
      || fail "Service images run $RUN_ID does not name the commit it built (title '${RUN_TITLE:-}'); deploy it with workflow_dispatch"
    sha="${BASH_REMATCH[1]}"

    # The publish jobs are named by image-build.yml ("publish <image>") under
    # the caller job "publish" in images.yml. If core-ci failed they are skipped.
    jobs="$(gh api --paginate "repos/$GITHUB_REPOSITORY/actions/runs/$RUN_ID/jobs" \
      --jq '.jobs[] | "\(.name)\t\(.conclusion)"')" || fail "could not list the jobs of run $RUN_ID"
    published=0
    for image in "${IMAGES[@]}"; do
      conclusion="$(awk -F '\t' -v n="publish / publish ledgermesh-$image" '$1 == n { print $2; exit }' <<<"$jobs")"
      case "$conclusion" in
        success) published=$((published + 1)) ;;
        "" | skipped) ;;
        *) fail "publish ledgermesh-$image concluded '$conclusion' in run $RUN_ID" ;;
      esac
    done
    [[ "$published" -gt 0 ]] || skip "run $RUN_ID published no images (core-ci did not pass for $sha)"
    [[ "$published" -eq ${#IMAGES[@]} ]] || fail "run $RUN_ID published only $published of ${#IMAGES[@]} images"
    ;;
  workflow_dispatch)
    [[ "${GITHUB_REF:-}" == "refs/heads/main" ]] || fail "staging deploys only run from main (this run is on ${GITHUB_REF:-?})"
    sha="${INPUT_SHA:-}"
    [[ "$sha" =~ $SHA_RE ]] || fail "sha must be a full 40-character lowercase commit SHA"
    ;;
  *)
    fail "deploy-staging does not handle the '$EVENT' event"
    ;;
esac

# Only commits that are on main, and that carry the deploy tooling.
git cat-file -e "$sha^{commit}" 2>/dev/null || fail "$sha is not a commit in this repository"
git merge-base --is-ancestor "$sha" origin/main || fail "$sha is not on main"
if [[ "$EVENT" == "workflow_run" ]]; then
  tip="$(git rev-parse --verify origin/main^{commit})" || fail "could not resolve origin/main"
  [[ "$sha" == "$tip" ]] \
    || skip "$sha is no longer main's tip ($tip is newer and its own run deploys it); staging never moves back to an older commit automatically"
fi
git cat-file -e "$sha:infra/staging/deploy.sh" 2>/dev/null \
  || fail "$sha has no infra/staging/deploy.sh (it predates LM-049); deploy a newer commit"

# The registry digests, so the VM can check it pulled exactly these.
owner="$(tr '[:upper:]' '[:lower:]' <<<"${GITHUB_REPOSITORY%%/*}")"
digests=""
for image in "${IMAGES[@]}"; do
  status=0
  digest="$(bash "$GHCR_DIGEST" "$owner/ledgermesh-$image" "$sha")" || status=$?
  case "$status" in
    0) ;;
    3) fail "ghcr.io/$owner/ledgermesh-$image:$sha was never published" ;;
    *) fail "could not look up ghcr.io/$owner/ledgermesh-$image:$sha" ;;
  esac
  [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "invalid digest for ledgermesh-$image"
  digests="${digests:+$digests,}$image=$digest"
  echo "- \`ghcr.io/$owner/ledgermesh-$image:$sha\` = \`$digest\`" >>"${GITHUB_STEP_SUMMARY:-/dev/null}"
done

{
  echo "deploy=true"
  echo "sha=$sha"
  echo "digests=$digests"
} >>"$GITHUB_OUTPUT"
echo "Deploying $sha"
