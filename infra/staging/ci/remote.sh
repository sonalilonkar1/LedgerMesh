#!/usr/bin/env bash
# Run deploy.sh on the staging VM over IAP SSH (LM-049).
#
#   remote.sh [--project <id>] [--zone <zone>] [--vm <name>] <command> [args]
#
# Commands (each runs `sudo bash deploy.sh <command> …` on the VM)
#   deploy <sha> [--actor <login>] [--run-url <url>] [--digests <list>] [--no-promote]
#       Uploads infra/staging exactly as committed at <sha> (git archive), so
#       the VM runs that commit's Compose file, Caddyfile and scripts.
#   promote <sha>
#   rollback <previous|last-known-good|sha> [--after-failed <sha>]
#   resolve <previous|last-known-good|sha>
#   status [--expect <sha>]
#   logs <local-out.tgz> [--sha <sha>]
#       Writes the redacted log bundle on the VM and copies it here.
#   fixtures <run_id>
#       Create the hosted smoke suite's rows for one run (LM-050) in the
#       running release; run_id is 6-40 lowercase letters and digits.
#
# Every command except deploy uploads the deploy tooling (infra/staging) of
# the checked-out commit (HEAD), which is main in the workflows. The upload is
# checked with sha256 on the VM before anything runs. The exit status is
# deploy.sh's (see its header): 10, 20, 21 and 22 mean different things.
#
# Defaults come from GCP_PROJECT_ID, GCP_ZONE and STAGING_VM_NAME. Google
# credentials come from the environment (google-github-actions/auth in the
# workflows, or `gcloud auth login` for an admin). SSH goes only through IAP
# with OS Login; the key gcloud creates for it expires after an hour.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROJECT="${GCP_PROJECT_ID:-deven-cloud-lab}"
ZONE="${GCP_ZONE:-us-west1-b}"
VM="${STAGING_VM_NAME:-ledgermesh-staging}"
SHA_RE='^[0-9a-f]{40}$'

die() {
  echo "remote: $*" >&2
  exit 2
}

usage() {
  sed -n '2,28p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) [[ $# -ge 2 ]] || die "$1 needs a value"; PROJECT="$2"; shift 2 ;;
    --zone) [[ $# -ge 2 ]] || die "$1 needs a value"; ZONE="$2"; shift 2 ;;
    --vm) [[ $# -ge 2 ]] || die "$1 needs a value"; VM="$2"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    *) ARGS+=("$1"); shift ;;
  esac
done
[[ ${#ARGS[@]} -ge 1 ]] || { usage >&2; exit 2; }
COMMAND="${ARGS[0]}"
REST=()
if [[ ${#ARGS[@]} -gt 1 ]]; then
  REST=("${ARGS[@]:1}")
fi

[[ "$PROJECT" =~ ^[a-z][a-z0-9-]{4,28}[a-z0-9]$ ]] || die "invalid project: $PROJECT"
[[ "$ZONE" =~ ^[a-z]+-[a-z]+[0-9]-[a-z]$ ]] || die "invalid zone: $ZONE"
[[ "$VM" =~ ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$ ]] || die "invalid VM name: $VM"

# The remote command line is built from these values, so every one is checked
# against a strict pattern before use and then shell-quoted as well.
check_args() {
  local i=0 arg next
  while [[ $i -lt ${#REST[@]} ]]; do
    arg="${REST[$i]}"
    next="${REST[$((i + 1))]:-}"
    case "$arg" in
      --actor)
        [[ "$next" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,38}(\[bot\])?$ ]] || die "invalid --actor"
        i=$((i + 2)) ;;
      --run-url)
        [[ "$next" =~ ^https://[A-Za-z0-9./_-]+$ ]] || die "invalid --run-url"
        i=$((i + 2)) ;;
      --digests)
        [[ "$next" =~ ^([a-z-]+=sha256:[0-9a-f]{64},?)+$ ]] || die "invalid --digests"
        i=$((i + 2)) ;;
      --after-failed | --expect | --sha)
        [[ "$next" =~ $SHA_RE ]] || die "invalid $arg"
        i=$((i + 2)) ;;
      --no-promote) i=$((i + 1)) ;;
      previous | last-known-good) i=$((i + 1)) ;;
      *)
        [[ "$arg" =~ $SHA_RE ]] || die "unexpected argument: $arg"
        i=$((i + 1)) ;;
    esac
  done
}

GCLOUD_SSH_FLAGS=(--project "$PROJECT" --zone "$ZONE" --tunnel-through-iap --quiet
  --ssh-key-expire-after 1h)

quote() {
  local out="" arg
  for arg in "$@"; do
    out+="$(printf '%q' "$arg") "
  done
  echo "${out% }"
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
NONCE="$(date -u +%Y%m%d%H%M%S)-$$"
REMOTE_BUNDLE="ledgermesh-bundle-$NONCE.tgz"

upload_bundle() {
  # upload_bundle <git revision>: infra/staging as committed at that revision,
  # plus .bundle-revision naming that commit (deploy.sh checks it).
  local revision commit
  revision="$1"
  commit="$(git -C "$ROOT" rev-parse --verify "$revision^{commit}")" || die "unknown revision $revision"
  mkdir -p "$WORK/bundle"
  git -C "$ROOT" archive --format=tar "$commit:infra/staging" | tar -xf - -C "$WORK/bundle" \
    || die "could not archive infra/staging at $commit"
  echo "$commit" >"$WORK/bundle/.bundle-revision"
  COPYFILE_DISABLE=1 tar -czf "$WORK/bundle.tgz" -C "$WORK/bundle" . || die "could not pack the bundle"
  BUNDLE_SUM="$(cd "$WORK" && { sha256sum bundle.tgz 2>/dev/null || shasum -a 256 bundle.tgz; } | awk '{ print $1 }')"
  [[ "$BUNDLE_SUM" =~ ^[0-9a-f]{64}$ ]] || die "could not checksum the bundle"
  gcloud compute scp "${GCLOUD_SSH_FLAGS[@]}" "$WORK/bundle.tgz" "$VM:$REMOTE_BUNDLE"
}

run_remote() {
  # run_remote <deploy.sh args…>: check, unpack and run the uploaded bundle.
  local script
  script="set -euo pipefail
f=\"\$HOME/$REMOTE_BUNDLE\"
echo \"$BUNDLE_SUM  \$f\" | sha256sum --check --quiet
d=\"\$(mktemp -d)\"
trap 'rm -rf \"\$d\" \"\$f\"' EXIT
tar -xzf \"\$f\" -C \"\$d\"
status=0
sudo bash \"\$d/deploy.sh\" $(quote "$@") || status=\$?
exit \$status"
  gcloud compute ssh "$VM" "${GCLOUD_SSH_FLAGS[@]}" --command "bash -c $(printf '%q' "$script")"
}

case "$COMMAND" in
  deploy)
    check_args
    sha="${REST[0]:-}"
    [[ "$sha" =~ $SHA_RE ]] || die "deploy needs a full commit SHA"
    git -C "$ROOT" cat-file -e "$sha^{commit}" || die "$sha is not in this checkout (fetch full history)"
    upload_bundle "$sha"
    run_remote deploy "${REST[@]}"
    ;;
  promote | rollback | resolve | status)
    check_args
    upload_bundle HEAD
    run_remote "$COMMAND" ${REST[@]+"${REST[@]}"}
    ;;
  fixtures)
    [[ ${#REST[@]} -eq 1 && "${REST[0]}" =~ ^[a-z0-9]{6,40}$ ]] \
      || die "fixtures needs exactly one run id of 6-40 lowercase letters and digits"
    upload_bundle HEAD
    run_remote fixtures "${REST[0]}"
    ;;
  logs)
    out="${REST[0]:-}"
    [[ -n "$out" ]] || die "logs needs a local output path"
    if [[ ${#REST[@]} -gt 1 ]]; then
      REST=("${REST[@]:1}")
    else
      REST=()
    fi
    check_args
    upload_bundle HEAD
    remote_out="ledgermesh-logs-$NONCE.tgz"
    # deploy.sh runs as root; --owner hands the bundle to the SSH user.
    gcloud compute ssh "$VM" "${GCLOUD_SSH_FLAGS[@]}" --command "bash -c $(printf '%q' "set -euo pipefail
f=\"\$HOME/$REMOTE_BUNDLE\"
echo \"$BUNDLE_SUM  \$f\" | sha256sum --check --quiet
d=\"\$(mktemp -d)\"
trap 'rm -rf \"\$d\" \"\$f\"' EXIT
tar -xzf \"\$f\" -C \"\$d\"
sudo bash \"\$d/deploy.sh\" logs \"\$HOME/$remote_out\" --owner \"\$(id -u):\$(id -g)\" $(quote ${REST[@]+"${REST[@]}"})")"
    gcloud compute scp "${GCLOUD_SSH_FLAGS[@]}" "$VM:$remote_out" "$out"
    gcloud compute ssh "$VM" "${GCLOUD_SSH_FLAGS[@]}" --command "rm -f \"\$HOME/$remote_out\""
    echo "remote: wrote $out"
    ;;
  *) usage >&2; die "unknown command: $COMMAND" ;;
esac
