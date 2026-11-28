#!/usr/bin/env bash
# Deploy, promote and roll back hosted staging releases (LM-049).
#
#   deploy.sh [options] <command> [args]
#
# Runs on the staging VM as root (the deploy workflow calls it through
# infra/staging/ci/remote.sh over IAP SSH). It wraps stack.sh (LM-048), which
# does the actual bring-up, and keeps a release record for every commit.
#
# Commands
#   deploy <sha> [--actor <login>] [--run-url <url>] [--digests <list>] [--no-promote]
#       1. Check that all four images exist for <sha> (registry: pull them and
#          read their digests; local: they must already be tagged). Nothing has
#          changed yet if this fails: exit 10, the running release keeps running.
#       2. Record the previous release and copy this bundle (infra/staging at
#          <sha>) to releases/<sha>/bundle.
#       3. Render the env file, then run that bundle's `stack.sh up`: data tier,
#          prepared-transaction check, migrations, LM-018 topic bootstrap, APIs
#          and Caddy (bounded `compose up --wait`), workers, every check.
#       4. Check every container runs exactly the recorded image and revision.
#       5. Mark the release healthy and, unless --no-promote, last-known-good.
#       On any failure after step 1: save a redacted log bundle, then restore
#       the last-known-good release from its own bundle and recorded image
#       digests (no rebuild, no migrations). Exit 20 when that worked, 21 when
#       the rollback failed too, 22 when there was no last-known-good release.
#       --digests coordinator=sha256:…,wallet=…,catalog=…,audit-ledger=…
#       (from the registry, looked up by the workflow) must match what was pulled.
#   promote <sha>
#       Mark the running, healthy release last-known-good (the deploy workflow
#       does this after the hosted smoke suite passes).
#   rollback <previous|last-known-good|sha> [--after-failed <sha>]
#       Bring back a release that was last-known-good before, from its own
#       bundle and image digests. Never runs migrations or the topic
#       bootstrap. `previous` is the newest promoted release other than the
#       one running now. --after-failed marks that release's record as rolled
#       back. A target that is already running and correct is left alone.
#   resolve <previous|last-known-good|sha>
#       Print the SHA a rollback target refers to.
#   status [--expect <sha>]
#       Print the current and last-known-good releases, every container's
#       image and revision, and the prepared-transaction check. With --expect,
#       fail unless <sha> is current and every container runs its images.
#   logs <out.tgz> [--sha <sha>] [--owner <uid:gid>]
#       Write a log bundle: current container logs, the release record and
#       history, and the deploy logs and failure bundles of <sha> (default the
#       current release). Every database password is redacted and the result
#       is checked before it is written.
#
# Options
#   --env-file <path>        Rendered env file (default /etc/ledgermesh/staging.env).
#   --secrets-source <src>   gcp (default) | file:<path> | none. Passed to
#                            render-env.sh before every bring-up; none keeps
#                            the existing env file.
#   --image-source <src>     registry (default: pull from GHCR and pin digests)
#                            | local (images already tagged on this host;
#                            local validation only).
#   --data-dir <path>        Absolute data directory (default /var/lib/ledgermesh).
#                            Release records live in <data-dir>/releases and
#                            deploy logs in <data-dir>/deploy-logs.
#   --project-name <name>    Compose project (default ledgermesh-staging).
#   --current-link <path>    Symlink kept pointing at the running release's
#                            bundle (default /opt/ledgermesh/staging).
#   --public-url <url>       Recorded in the release (default https://<site address>).
#   --wait-timeout <secs>    Passed to stack.sh (default 300).
#   --https-port, --ca-file, --worker-window
#                            Passed to stack.sh.
#   --keep <n>               Keep the images of the newest <n> releases plus the
#                            current and last-known-good ones (default 3,
#                            registry mode only).
#   --lock-timeout <secs>    How long to wait for another deploy.sh on this
#                            host to finish before failing (default 300).
#
# Exit status: 0 ok, 1 error, 2 usage, 3 no such rollback target, 10 images
# missing or wrong (nothing changed), 20 deploy failed and was rolled back,
# 21 deploy or rollback failed and staging may be unhealthy, 22 deploy failed
# and there was no last-known-good release to restore.
#
# Compatible with bash 3.2 (macOS) as well as the VM's bash. Needs python3 for
# the JSON release records.
set -euo pipefail

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTRY="ghcr.io/sonalilonkar1"
IMAGE_SERVICES=(coordinator wallet catalog audit-ledger)
# Every long-running container and the image it runs (compose.yaml).
CONTAINER_IMAGES=(
  "coordinator:coordinator" "wallet:wallet" "catalog:catalog" "audit-ledger:audit-ledger"
  "coordinator-saga:coordinator" "wallet-saga:wallet" "catalog-saga:catalog" "audit-consumer:audit-ledger"
)
PASSWORD_KEYS=(COORDINATOR_DB_PASSWORD WALLET_DB_PASSWORD CATALOG_DB_PASSWORD AUDIT_LEDGER_DB_PASSWORD)

ENV_FILE="/etc/ledgermesh/staging.env"
SECRETS_SOURCE="gcp"
IMAGE_SOURCE="registry"
DATA_DIR="/var/lib/ledgermesh"
PROJECT="ledgermesh-staging"
CURRENT_LINK="/opt/ledgermesh/staging"
PUBLIC_URL=""
WAIT_TIMEOUT=300
HTTPS_PORT=""
CA_FILE=""
WORKER_WINDOW=""
KEEP=3
LOCK_TIMEOUT=300

ACTOR="unknown"
RUN_URL=""
DIGESTS=""
PROMOTE=1
AFTER_FAILED=""
EXPECT=""
LOG_SHA=""
OWNER=""

SHA_RE='^[0-9a-f]{40}$'
# Only LEDGERMESH_REVISION from an image's or container's environment: the rest
# of a container's environment holds secrets and never leaves Docker.
REVISION_TEMPLATE='{{range .Config.Env}}{{if eq (index (split . "=") 0) "LEDGERMESH_REVISION"}}{{index (split . "=") 1}}{{end}}{{end}}'
DIGEST_RE='^sha256:[0-9a-f]{64}$'

LOG=""

log() {
  echo "deploy: $*" >&2
  if [[ -n "$LOG" ]]; then
    echo "$(utc) deploy: $*" >>"$LOG"
  fi
}

die() {
  log "$*"
  exit 1
}

die_code() {
  local code="$1"
  shift
  log "$*"
  exit "$code"
}

usage() {
  sed -n '2,79p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

utc() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

# ---- arguments --------------------------------------------------------------------

ORIG_OPTS=()
ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file | --secrets-source | --image-source | --data-dir | --project-name | \
      --current-link | --public-url | --wait-timeout | --https-port | --ca-file | \
      --worker-window | --keep | --lock-timeout)
      [[ $# -ge 2 ]] || { echo "deploy: $1 needs a value" >&2; exit 2; }
      ORIG_OPTS+=("$1" "$2")
      case "$1" in
        --env-file) ENV_FILE="$2" ;;
        --secrets-source) SECRETS_SOURCE="$2" ;;
        --image-source) IMAGE_SOURCE="$2" ;;
        --data-dir) DATA_DIR="$2" ;;
        --project-name) PROJECT="$2" ;;
        --current-link) CURRENT_LINK="$2" ;;
        --public-url) PUBLIC_URL="$2" ;;
        --wait-timeout) WAIT_TIMEOUT="$2" ;;
        --https-port) HTTPS_PORT="$2" ;;
        --ca-file) CA_FILE="$2" ;;
        --worker-window) WORKER_WINDOW="$2" ;;
        --keep) KEEP="$2" ;;
        --lock-timeout) LOCK_TIMEOUT="$2" ;;
      esac
      shift 2
      ;;
    --actor | --run-url | --digests | --after-failed | --expect | --sha | --owner)
      [[ $# -ge 2 ]] || { echo "deploy: $1 needs a value" >&2; exit 2; }
      case "$1" in
        --actor) ACTOR="$2" ;;
        --run-url) RUN_URL="$2" ;;
        --digests) DIGESTS="$2" ;;
        --after-failed) AFTER_FAILED="$2" ;;
        --expect) EXPECT="$2" ;;
        --sha) LOG_SHA="$2" ;;
        --owner) OWNER="$2" ;;
      esac
      shift 2
      ;;
    --no-promote) PROMOTE=0; shift ;;
    -h | --help) usage; exit 0 ;;
    *) ARGS+=("$1"); shift ;;
  esac
done
[[ ${#ARGS[@]} -ge 1 ]] || { usage >&2; exit 2; }
COMMAND="${ARGS[0]}"

case "$SECRETS_SOURCE" in gcp | none | file:?*) ;; *) die "--secrets-source must be gcp, none or file:<path>" ;; esac
case "$IMAGE_SOURCE" in registry | local) ;; *) die "--image-source must be registry or local" ;; esac
[[ "$PROJECT" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || die "invalid --project-name: $PROJECT"
[[ "$DATA_DIR" == /* ]] || die "--data-dir must be an absolute path"
[[ "$CURRENT_LINK" == /* ]] || die "--current-link must be an absolute path"
for number in "$WAIT_TIMEOUT" "$KEEP" "$LOCK_TIMEOUT" ${HTTPS_PORT:+"$HTTPS_PORT"} ${WORKER_WINDOW:+"$WORKER_WINDOW"}; do
  [[ "$number" =~ ^[0-9]+$ ]] || die "expected a whole number, got: $number"
done
# Recorded in JSON and shown in logs, so only plain values.
[[ "$ACTOR" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,38}(\[bot\])?$ ]] || die "invalid --actor: $ACTOR"
if [[ -n "$RUN_URL" && ! "$RUN_URL" =~ ^https://[A-Za-z0-9./_-]+$ ]]; then
  die "invalid --run-url"
fi
if [[ -n "$PUBLIC_URL" && ! "$PUBLIC_URL" =~ ^https://[A-Za-z0-9.-]+(:[0-9]+)?/?$ ]]; then
  die "invalid --public-url"
fi
for value in "$AFTER_FAILED" "$EXPECT" "$LOG_SHA"; do
  if [[ -n "$value" && ! "$value" =~ $SHA_RE ]]; then
    die "expected a full 40-character lowercase commit SHA, got: $value"
  fi
done
if [[ -n "$OWNER" && ! "$OWNER" =~ ^[0-9]+:[0-9]+$ ]]; then
  die "--owner must be <uid>:<gid>"
fi

RELEASES="$DATA_DIR/releases"
DEPLOY_LOGS="$DATA_DIR/deploy-logs"

command -v python3 >/dev/null || die "python3 is required (vm-startup.sh installs it)"

# ---- release records (JSON, written atomically) ------------------------------------

RECORD_PY='
import json, os, sys

op, path = sys.argv[1], sys.argv[2]
rest = sys.argv[3:]


def load(p):
    try:
        with open(p) as handle:
            return json.load(handle)
    except FileNotFoundError:
        return {}


def parse(item):
    key, _, value = item.partition("=")
    if key.endswith(":int"):
        return key[:-4], int(value)
    if key.endswith(":null"):
        return key[:-5], None
    return key, value


if op == "set":
    data = load(path)
    for item in rest:
        key, value = parse(item)
        target = data
        parts = key.split(".")
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = value
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(tmp, path)
elif op == "get":
    value = load(path)
    for part in rest[0].split("."):
        value = value.get(part) if isinstance(value, dict) else None
    if value is not None:
        print(value)
elif op == "append":
    entry = dict(parse(item) for item in rest)
    with open(path, "a") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")
elif op == "previous":
    # Newest promoted release that is not excluded.
    try:
        lines = open(path).read().splitlines()
    except FileNotFoundError:
        lines = []
    for line in reversed(lines):
        entry = json.loads(line)
        if entry.get("event") == "promoted" and entry.get("sha") not in rest:
            print(entry["sha"])
            break
elif op == "was-promoted":
    try:
        lines = open(path).read().splitlines()
    except FileNotFoundError:
        lines = []
    found = any(
        json.loads(line).get("event") == "promoted" and json.loads(line).get("sha") == rest[0]
        for line in lines
    )
    sys.exit(0 if found else 1)
elif op == "recent":
    # Distinct SHAs of the newest deploys, newest first.
    try:
        lines = open(path).read().splitlines()
    except FileNotFoundError:
        lines = []
    seen = []
    for line in reversed(lines):
        sha = json.loads(line).get("sha")
        if sha and sha not in seen:
            seen.append(sha)
    print("\n".join(seen[: int(rest[0])]))
else:
    sys.exit(f"unknown record operation {op}")
'

record() {
  python3 -I -S -c "$RECORD_PY" "$@"
}

release_json() {
  echo "$RELEASES/$1/release.json"
}

record_set() {
  local sha="$1"
  shift
  record set "$(release_json "$sha")" "$@"
}

record_get() {
  record get "$(release_json "$1")" "$2"
}

history_add() {
  record append "$RELEASES/history.jsonl" "at=$(utc)" "$@"
}

read_pointer() {
  local file="$RELEASES/$1" value=""
  if [[ -f "$file" ]]; then
    value="$(cat "$file")"
    [[ "$value" =~ $SHA_RE ]] || die "$file does not hold a commit SHA"
  fi
  echo "$value"
}

set_pointer() {
  local file="$RELEASES/$1"
  echo "$2" >"$file.tmp"
  mv -f "$file.tmp" "$file"
}

# ---- lock ---------------------------------------------------------------------------

LOCK_DIR=""
release_lock() {
  if [[ -n "$LOCK_DIR" ]]; then
    rm -rf "$LOCK_DIR"
    LOCK_DIR=""
  fi
}

# Waits (bounded, backing off up to 15 s) for another deploy.sh on this host,
# so short operations such as `fixtures` or `promote` queue behind each other
# instead of failing. A lock whose pid is gone, or that never got a pid within
# a minute, is stale and removed.
acquire_lock() {
  local dir="$RELEASES/.lock" owner="" deadline=$((SECONDS + LOCK_TIMEOUT)) delay=1 waiting=0
  mkdir -p "$RELEASES" "$DEPLOY_LOGS"
  until mkdir "$dir" 2>/dev/null; do
    owner="$(cat "$dir/pid" 2>/dev/null)" || owner=""
    if [[ "$owner" =~ ^[0-9]+$ ]] && ! kill -0 "$owner" 2>/dev/null; then
      log "removing a stale lock (pid $owner is gone)"
      rm -rf "$dir"
      continue
    fi
    if [[ -z "$owner" && -n "$(find "$dir" -maxdepth 0 -mmin +1 2>/dev/null)" ]]; then
      log "removing a stale lock (no pid after a minute)"
      rm -rf "$dir"
      continue
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      die "another deploy.sh (pid ${owner:-starting}) is still running after ${LOCK_TIMEOUT}s; try again when it has finished"
    fi
    if [[ "$waiting" == 0 ]]; then
      log "another deploy.sh (pid ${owner:-starting}) is running; waiting up to ${LOCK_TIMEOUT}s"
      waiting=1
    fi
    sleep "$delay"
    delay=$((delay * 2 > 15 ? 15 : delay * 2))
  done
  echo "$$" >"$dir/pid"
  LOCK_DIR="$dir"
  trap release_lock EXIT
}

start_log() {
  # start_log <name>: also append this run's own messages and the bring-up
  # output to deploy-logs/<name>-<timestamp>.log (secret-free, like stack.sh's output).
  mkdir -p "$DEPLOY_LOGS"
  LOG="$DEPLOY_LOGS/$1-$(date -u +%Y%m%dT%H%M%SZ).log"
  : >"$LOG"
}

# ---- images -------------------------------------------------------------------------

image_var() {
  case "$1" in
    coordinator) echo LEDGERMESH_COORDINATOR_IMAGE ;;
    wallet) echo LEDGERMESH_WALLET_IMAGE ;;
    catalog) echo LEDGERMESH_CATALOG_IMAGE ;;
    audit-ledger) echo LEDGERMESH_AUDIT_LEDGER_IMAGE ;;
    *) return 1 ;;
  esac
}

expected_digest() {
  # The digest the workflow read from the registry for <service>, if given.
  local item
  [[ -n "$DIGESTS" ]] || return 0
  for item in $(echo "$DIGESTS" | tr ',' ' '); do
    if [[ "${item%%=*}" == "$1" ]]; then
      echo "${item#*=}"
      return 0
    fi
  done
}

image_revision() {
  docker inspect -f "$REVISION_TEMPLATE" "$1"
}

resolve_images() {
  # resolve_images <sha> <images.env out> <record args out>
  # Exits 10 on any problem: nothing has changed yet.
  local sha="$1" out="$2" fields="$3" service repo ref digests digest expected id revision var
  : >"$out"
  : >"$fields"
  if [[ -n "$DIGESTS" ]]; then
    for service in "${IMAGE_SERVICES[@]}"; do
      [[ "$(expected_digest "$service")" =~ $DIGEST_RE ]] \
        || die_code 10 "--digests has no valid digest for $service"
    done
  fi
  for service in "${IMAGE_SERVICES[@]}"; do
    repo="$REGISTRY/ledgermesh-$service"
    ref="$repo:$sha"
    var="$(image_var "$service")"
    if [[ "$IMAGE_SOURCE" == "registry" ]]; then
      log "pulling $ref"
      if ! docker pull --quiet "$ref" >/dev/null; then
        die_code 10 "could not pull $ref: not published for this commit, or the registry is unreachable. Nothing was changed."
      fi
      digests="$(docker image inspect -f '{{range .RepoDigests}}{{println .}}{{end}}' "$ref")" \
        || die_code 10 "could not inspect $ref"
      digest="$(sed -n "s|^$repo@||p" <<<"$digests" | sed -n 1p)"
      [[ "$digest" =~ $DIGEST_RE ]] || die_code 10 "$ref has no registry digest"
      expected="$(expected_digest "$service")"
      if [[ -n "$expected" && "$expected" != "$digest" ]]; then
        die_code 10 "$ref is $digest but the registry lookup said $expected. Nothing was changed."
      fi
      echo "$var=$repo@$digest" >>"$out"
      echo "digests.$service=$digest" >>"$fields"
    else
      if ! docker image inspect "$ref" >/dev/null 2>&1; then
        die_code 10 "image $ref is not on this host (--image-source local). Nothing was changed."
      fi
      id="$(docker image inspect -f '{{.Id}}' "$ref")" || die_code 10 "could not inspect $ref"
      echo "$var=$ref" >>"$out"
      echo "image_ids.$service=$id" >>"$fields"
    fi
    revision="$(image_revision "$ref")" || die_code 10 "could not read the revision of $ref"
    [[ "$revision" == "$sha" ]] \
      || die_code 10 "$ref was built from revision '${revision:-none}', not $sha. Nothing was changed."
    echo "images.$service=$(sed -n "s/^$var=//p" "$out")" >>"$fields"
    log "ok  $service $(sed -n "s/^$var=//p" "$out")"
  done
}

load_images() {
  # Export the four LEDGERMESH_*_IMAGE variables of a release.
  local file="$RELEASES/$1/images.env" line key value service
  [[ -f "$file" ]] || die "release $1 has no recorded images ($file)"
  for service in "${IMAGE_SERVICES[@]}"; do
    key="$(image_var "$service")"
    line="$(grep "^$key=" "$file")" || die "$file has no $key"
    value="${line#*=}"
    [[ "$value" =~ ^$REGISTRY/ledgermesh-$service(@sha256:[0-9a-f]{64}|:[0-9a-f]{40})$ ]] \
      || die "$file has an unexpected image reference for $service"
    export "$key=$value"
  done
}

ensure_images() {
  # The images of a recorded release are on this host, re-pulled by digest if
  # they were pruned; locally built tags must still be the recorded image.
  local sha="$1" service var ref recorded id
  load_images "$sha"
  for service in "${IMAGE_SERVICES[@]}"; do
    var="$(image_var "$service")"
    ref="${!var}"
    if [[ "$ref" == *@sha256:* ]]; then
      if ! docker image inspect "$ref" >/dev/null 2>&1; then
        log "pulling $ref"
        docker pull --quiet "$ref" >/dev/null || die "could not pull $ref"
      fi
    else
      docker image inspect "$ref" >/dev/null 2>&1 || die "image $ref is no longer on this host"
      recorded="$(record_get "$sha" "image_ids.$service")"
      id="$(docker image inspect -f '{{.Id}}' "$ref")"
      [[ -z "$recorded" || "$recorded" == "$id" ]] \
        || die "$ref now points at $id, not the recorded $recorded; re-tag the recorded image"
    fi
  done
}

# ---- what is running -----------------------------------------------------------------

container_for() {
  local names
  names="$(docker ps -a --filter "label=com.docker.compose.project=$PROJECT" \
    --filter "label=com.docker.compose.service=$1" --format '{{.Names}}')" || return 1
  # One-off `compose run` containers are not the service.
  awk '!/-run-/ { print; exit }' <<<"$names"
}

check_running() {
  # check_running <sha> [quiet]: every API and worker container is running the
  # release's recorded image and revision. Returns 1 with the reasons otherwise.
  local sha="$1" quiet="${2:-}" ok=0 spec name image_service var want names states line image state revision
  if [[ ! -f "$RELEASES/$sha/images.env" ]]; then
    echo "release $sha has no recorded images"
    return 1
  fi
  names="$(docker ps -a --filter "label=com.docker.compose.project=$PROJECT" --format '{{.Names}}')" || return 1
  names="$(awk '!/-run-/' <<<"$names")"
  states=""
  if [[ -n "$names" ]]; then
    # One line per container: service image status revision.
    # shellcheck disable=SC2086 # one container name per word
    states="$(docker inspect -f "{{index .Config.Labels \"com.docker.compose.service\"}} {{.Config.Image}} {{.State.Status}} $REVISION_TEMPLATE" $names)" || return 1
  fi
  for spec in "${CONTAINER_IMAGES[@]}"; do
    name="${spec%%:*}"
    image_service="${spec#*:}"
    var="$(image_var "$image_service")"
    want="$(sed -n "s/^$var=//p" "$RELEASES/$sha/images.env")"
    line="$(awk -v n="$name" '$1 == n { print; exit }' <<<"$states")"
    if [[ -z "$line" ]]; then
      echo "FAIL $name: no container"
      ok=1
      continue
    fi
    read -r _ image state revision <<<"$line"
    if [[ "$image" != "$want" || "${revision:-}" != "$sha" || "$state" != "running" ]]; then
      echo "FAIL $name: $state, image $image, revision ${revision:-none} (want $want, $sha)"
      ok=1
    elif [[ -z "$quiet" ]]; then
      echo "ok  $name $state $image revision $revision"
    fi
  done
  return "$ok"
}

# ---- bring-up ------------------------------------------------------------------------

stack_args() {
  # The options every stack.sh call gets.
  STACK_ARGS=(--env-file "$ENV_FILE" --data-dir "$DATA_DIR" --project-name "$PROJECT"
    --wait-timeout "$WAIT_TIMEOUT")
  if [[ -n "$HTTPS_PORT" ]]; then STACK_ARGS+=(--https-port "$HTTPS_PORT"); fi
  if [[ -n "$CA_FILE" ]]; then STACK_ARGS+=(--ca-file "$CA_FILE"); fi
  if [[ -n "$WORKER_WINDOW" ]]; then STACK_ARGS+=(--worker-window "$WORKER_WINDOW"); fi
}

apply_child() {
  # Internal (`__apply <sha> deploy|rollback`), run as its own process so that
  # `set -e` applies to every step and the parent can react to the outcome.
  local sha="$1" mode="$2" rel="$RELEASES/$1"
  local phase_file="$rel/phase"
  echo "env" >"$phase_file"
  if [[ "$SECRETS_SOURCE" != "none" ]]; then
    bash "$SCRIPT_DIR/render-env.sh" --source "$SECRETS_SOURCE" --out "$ENV_FILE"
  fi
  [[ -f "$ENV_FILE" ]] || die "env file $ENV_FILE is missing"
  echo "images" >"$phase_file"
  ensure_images "$sha"
  stack_args
  STACK_ARGS+=(--phase-file "$phase_file")
  if [[ "$mode" == "rollback" ]]; then
    STACK_ARGS+=(--skip-migrations --skip-topics)
  fi
  log "bringing up $sha ($mode) with $rel/bundle/stack.sh"
  bash "$rel/bundle/stack.sh" "${STACK_ARGS[@]}" up
  echo "revision" >"$phase_file"
  check_running "$sha"
  echo "complete" >"$phase_file"
}

apply_release() {
  # apply_release <sha> <mode>: returns the child's exit status; its output
  # also goes to this run's deploy log.
  local status
  set +e
  bash "$SCRIPT_DIR/deploy.sh" ${ORIG_OPTS[@]+"${ORIG_OPTS[@]}"} __apply "$1" "$2" 2>&1 | tee -a "$LOG" >&2
  status="${PIPESTATUS[0]}"
  set -e
  return "$status"
}

install_bundle() {
  # Copy this directory (infra/staging at <sha>) into the release, so a later
  # rollback uses exactly this Compose file, Caddyfile and stack.sh.
  local sha="$1" dest="$RELEASES/$1/bundle" tmp
  if [[ -d "$dest" && "$(cd -P "$dest" && pwd)" == "$SCRIPT_DIR" ]]; then
    return 0
  fi
  tmp="$RELEASES/$sha/bundle.new.$$"
  rm -rf "$tmp"
  mkdir -p "$tmp"
  cp -R "$SCRIPT_DIR/." "$tmp/"
  # Never keep a local secrets file or bytecode in a release.
  rm -f "$tmp/.env.local"
  find "$tmp" -name __pycache__ -type d -prune -exec rm -rf {} +
  if [[ -d "$dest" ]] && diff -r -q "$dest" "$tmp" >/dev/null 2>&1; then
    rm -rf "$tmp"
    return 0
  fi
  rm -rf "$dest"
  mv "$tmp" "$dest"
}

update_link() {
  local target="$RELEASES/$1/bundle"
  if [[ -e "$CURRENT_LINK" && ! -L "$CURRENT_LINK" ]]; then
    local aside
    aside="$CURRENT_LINK.manual-$(date -u +%Y%m%dT%H%M%SZ)"
    log "moving the hand-copied $CURRENT_LINK aside to $aside"
    mv "$CURRENT_LINK" "$aside"
  fi
  mkdir -p "$(dirname "$CURRENT_LINK")"
  ln -sfn "$target" "$CURRENT_LINK"
}

collect_failure_logs() {
  # Keep the failing containers' logs before the rollback replaces them.
  local sha="$1" out
  stack_args
  out="$DEPLOY_LOGS/$sha-$(date -u +%Y%m%dT%H%M%SZ)-failed.tgz"
  if bash "$RELEASES/$sha/bundle/stack.sh" "${STACK_ARGS[@]}" collect-logs "$out" >>"$LOG" 2>&1; then
    log "saved the failing release's logs to $out"
  else
    log "WARNING: could not save the failing release's logs (see $LOG); rolling back anyway"
  fi
}

prune_images() {
  # Remove the images of releases outside the newest $KEEP plus the current
  # and last-known-good ones. Rollback re-pulls them by digest if needed.
  [[ "$IMAGE_SOURCE" == "registry" ]] || return 0
  local keep sha dir ref pinned service var current lkg
  current="$(read_pointer current)"
  lkg="$(read_pointer last-known-good)"
  keep="$(record recent "$RELEASES/history.jsonl" "$KEEP" | tr '\n' ' ') $current $lkg"
  for dir in "$RELEASES"/*/; do
    sha="$(basename "$dir")"
    [[ "$sha" =~ $SHA_RE ]] || continue
    [[ " $keep " == *" $sha "* ]] && continue
    [[ "$(record_get "$sha" images_pruned)" == "true" ]] && continue
    [[ -f "$dir/images.env" ]] || continue
    for service in "${IMAGE_SERVICES[@]}"; do
      var="$(image_var "$service")"
      pinned="$(sed -n "s/^$var=//p" "$dir/images.env")"
      for ref in "$pinned" "$REGISTRY/ledgermesh-$service:$sha"; do
        if docker image inspect "$ref" >/dev/null 2>&1; then
          if ! docker image rm "$ref" >/dev/null; then
            log "WARNING: could not remove image $ref"
          fi
        fi
      done
    done
    record_set "$sha" images_pruned=true
    log "removed the images of old release $sha"
  done
}

# ---- commands --------------------------------------------------------------------------

do_promote() {
  local sha="$1"
  set_pointer last-known-good "$sha"
  record_set "$sha" status=promoted "promoted_at=$(utc)"
  history_add event=promoted "sha=$sha" "actor=$ACTOR"
  log "promoted $sha to last-known-good"
}

rollback_to() {
  # rollback_to <target> [failed sha]: 0 when the target is up and verified.
  local target="$1" failed="${2:-}"
  log "restoring $target (no migrations, no topic bootstrap)"
  history_add event=rollback_started "sha=$target" "from=${failed:-none}" "actor=$ACTOR"
  # Called as a condition, where `set -e` does not apply: every step checks.
  if apply_release "$target" rollback; then
    set_pointer current "$target" || return 1
    update_link "$target" || return 1
    set_pointer last-known-good "$target" || return 1
    record_set "$target" "last_restored_at=$(utc)" || return 1
    history_add event=rolled_back "sha=$target" "from=${failed:-none}" "actor=$ACTOR" || return 1
    if [[ -n "$failed" && -f "$(release_json "$failed")" ]]; then
      record_set "$failed" status=failed_rolled_back "rolled_back_to=$target" "finished_at=$(utc)" \
        || return 1
    fi
    log "restored $target"
    return 0
  fi
  history_add event=rollback_failed "sha=$target" "from=${failed:-none}" "actor=$ACTOR"
  if [[ -n "$failed" && -f "$(release_json "$failed")" ]]; then
    record_set "$failed" status=rollback_failed "rolled_back_to=$target" "finished_at=$(utc)"
  fi
  log "ROLLBACK TO $target FAILED: staging may be unhealthy. See $LOG and docs/deployment/staging.md"
  return 1
}

cmd_deploy() {
  local sha="${1:-}" rel previous current attempt started tmp_images tmp_fields phase url
  [[ "$sha" =~ $SHA_RE ]] || die "deploy needs a full 40-character lowercase commit SHA"
  # This directory becomes the release's bundle, so it must be infra/staging
  # at <sha>: remote.sh marks its upload with .bundle-revision, and an older
  # release's installed bundle (/opt/ledgermesh/staging) must not be reused.
  if [[ -f "$SCRIPT_DIR/.bundle-revision" && "$(cat "$SCRIPT_DIR/.bundle-revision")" != "$sha" ]]; then
    die "this bundle is infra/staging at $(cat "$SCRIPT_DIR/.bundle-revision"), not $sha; use ci/remote.sh deploy $sha"
  fi
  case "$SCRIPT_DIR/" in
    "$(cd -P "$DATA_DIR" 2>/dev/null && pwd)/releases/$sha/bundle/") ;;
    "$(cd -P "$DATA_DIR" 2>/dev/null && pwd)/releases/"*)
      die "this is another release's bundle; deploy $sha with ci/remote.sh deploy $sha (it uploads infra/staging at $sha)" ;;
  esac
  acquire_lock
  start_log "$sha"
  started="$(utc)"
  log "deploying $sha (actor $ACTOR, images from $IMAGE_SOURCE)"

  # 1. Images. Nothing changes before this succeeds.
  tmp_images="$(mktemp "$RELEASES/.images.XXXXXX")"
  tmp_fields="$(mktemp "$RELEASES/.fields.XXXXXX")"
  # shellcheck disable=SC2064 # expand now
  trap "rm -f '$tmp_images' '$tmp_fields'; release_lock" EXIT
  resolve_images "$sha" "$tmp_images" "$tmp_fields"

  # 2. Record.
  previous="$(read_pointer last-known-good)"
  current="$(read_pointer current)"
  rel="$RELEASES/$sha"
  mkdir -p "$rel"
  install_bundle "$sha"
  mv -f "$tmp_images" "$rel/images.env"
  attempt="$(record_get "$sha" attempts)"
  attempt=$((${attempt:-0} + 1))
  url="$PUBLIC_URL"
  if [[ -z "$url" && -f "$ENV_FILE" ]]; then
    url="https://$(sed -n 's/^LEDGERMESH_SITE_ADDRESS=//p' "$ENV_FILE" | tail -n 1)"
  fi
  local fields=()
  while IFS= read -r line; do
    [[ -n "$line" ]] && fields+=("$line")
  done <"$tmp_fields"
  record_set "$sha" "sha=$sha" status=deploying "started_at=$started" finished_at:null= \
    healthy_at:null= "actor=$ACTOR" "run_url=$RUN_URL" "url=$url" "image_source=$IMAGE_SOURCE" \
    "previous_last_known_good=$previous" "previous_current=$current" "attempts:int=$attempt" \
    failed_phase:null= rolled_back_to:null= ${fields[@]+"${fields[@]}"}
  history_add event=deploy_started "sha=$sha" "actor=$ACTOR" "attempt:int=$attempt" "run_url=$RUN_URL"
  rm -f "$rel/phase"

  # 3 and 4. Bring-up and revision check.
  if apply_release "$sha" deploy; then
    if [[ -z "$url" ]]; then
      url="https://$(sed -n 's/^LEDGERMESH_SITE_ADDRESS=//p' "$ENV_FILE" | tail -n 1)"
    fi
    record_set "$sha" status=healthy "healthy_at=$(utc)" "finished_at=$(utc)" "url=$url"
    set_pointer current "$sha"
    update_link "$sha"
    history_add event=healthy "sha=$sha" "actor=$ACTOR"
    if [[ "$PROMOTE" == 1 ]]; then
      do_promote "$sha"
    else
      log "healthy; not promoted yet (--no-promote): run 'deploy.sh promote $sha' after the smoke tests"
    fi
    prune_images
    log "deployed $sha; release record $(release_json "$sha")"
    return 0
  fi

  phase="unknown"
  if [[ -f "$rel/phase" ]]; then
    phase="$(cat "$rel/phase")"
  fi
  log "deploy of $sha FAILED in phase '$phase'"
  record_set "$sha" status=failed "failed_phase=$phase" "finished_at=$(utc)"
  history_add event=deploy_failed "sha=$sha" "phase=$phase" "actor=$ACTOR"
  collect_failure_logs "$sha"

  if [[ -z "$previous" ]]; then
    record_set "$sha" status=failed_no_rollback
    die_code 22 "there is no last-known-good release to restore; staging is left as the failed deploy left it"
  fi
  if rollback_to "$previous" "$sha"; then
    die_code 20 "deploy of $sha failed in '$phase'; restored last-known-good $previous"
  fi
  die_code 21 "deploy of $sha failed in '$phase' and restoring $previous failed too"
}

resolve_target() {
  local target="$1" sha current
  current="$(read_pointer current)"
  case "$target" in
    last-known-good) sha="$(read_pointer last-known-good)" ;;
    previous) sha="$(record previous "$RELEASES/history.jsonl" "$current" "$AFTER_FAILED")" ;;
    *)
      [[ "$target" =~ $SHA_RE ]] || die "rollback target must be previous, last-known-good or a full commit SHA"
      record was-promoted "$RELEASES/history.jsonl" "$target" \
        || die_code 3 "$target was never a last-known-good release here; only those can be restored"
      sha="$target"
      ;;
  esac
  [[ -n "$sha" ]] || die_code 3 "there is no '$target' release to roll back to"
  [[ -f "$RELEASES/$sha/images.env" && -f "$RELEASES/$sha/bundle/stack.sh" ]] \
    || die_code 3 "release $sha has no recorded images or bundle"
  echo "$sha"
}

cmd_rollback() {
  local target="${1:-}" sha current
  [[ -n "$target" ]] || die "rollback needs a target: previous, last-known-good or a SHA"
  acquire_lock
  start_log "rollback"
  sha="$(resolve_target "$target")"
  current="$(read_pointer current)"
  if [[ -n "$AFTER_FAILED" && "$AFTER_FAILED" == "$sha" ]]; then
    # An idempotent redeploy of the last-known-good release failed the smoke
    # suite: there is nothing older to restore under this target. Say so
    # instead of reporting a restore; an older SHA can be restored with
    # rollback-staging.yml.
    log "the failed release $sha is itself the $target release; nothing older is restored and $sha stays live"
    history_add event=smoke_failed_on_last_known_good "sha=$sha" "target=$target" "actor=$ACTOR"
  fi
  if [[ "$current" == "$sha" ]] && check_running "$sha" quiet >>"$LOG"; then
    log "$sha is already running and verified; nothing to do"
    if [[ -n "$AFTER_FAILED" && "$AFTER_FAILED" != "$sha" && -f "$(release_json "$AFTER_FAILED")" ]]; then
      case "$(record_get "$AFTER_FAILED" status)" in
        failed_rolled_back | rollback_failed | failed_no_rollback) ;;
        *) record_set "$AFTER_FAILED" status=smoke_failed_rolled_back "rolled_back_to=$sha" "finished_at=$(utc)" ;;
      esac
    fi
    return 0
  fi
  if ! rollback_to "$sha" "$AFTER_FAILED"; then
    exit 21
  fi
  if [[ -n "$AFTER_FAILED" && "$AFTER_FAILED" != "$sha" && -f "$(release_json "$AFTER_FAILED")" ]]; then
    # It reached healthy but failed later (the hosted smoke suite).
    case "$(record_get "$AFTER_FAILED" healthy_at)" in
      "") ;;
      *) record_set "$AFTER_FAILED" status=smoke_failed_rolled_back ;;
    esac
  fi
  history_add event=promoted "sha=$sha" "actor=$ACTOR" reason=rollback
  log "rolled back to $sha; it is now current and last-known-good"
}

cmd_promote() {
  local sha="${1:-}" status
  [[ "$sha" =~ $SHA_RE ]] || die "promote needs a full commit SHA"
  acquire_lock
  start_log "promote-$sha"
  status="$(record_get "$sha" status)"
  case "$status" in
    healthy | promoted) ;;
    *) die "release $sha is '${status:-unknown}', not healthy; refusing to promote" ;;
  esac
  [[ "$(read_pointer current)" == "$sha" ]] || die "$sha is not the current release"
  check_running "$sha" >&2 || die "$sha is not running as recorded; refusing to promote"
  do_promote "$sha"
}

cmd_status() {
  local current lkg spec name container status=0
  current="$(read_pointer current)"
  lkg="$(read_pointer last-known-good)"
  echo "current_sha=$current"
  echo "last_known_good_sha=$lkg"
  if [[ -n "$current" ]]; then
    echo "current_status=$(record_get "$current" status)"
    if ! check_running "$current"; then
      status=1
    fi
    stack_args
    load_images "$current"
    if ! bash "$RELEASES/$current/bundle/stack.sh" "${STACK_ARGS[@]}" verify prepared-transactions; then
      status=1
    fi
  else
    for spec in "${CONTAINER_IMAGES[@]}"; do
      name="${spec%%:*}"
      container="$(container_for "$name")"
      echo "$name: ${container:-no container}"
    done
  fi
  if [[ -n "$EXPECT" ]]; then
    [[ "$current" == "$EXPECT" ]] || die "expected $EXPECT to be current, found ${current:-nothing}"
    [[ "$status" == 0 ]] || die "$EXPECT is current but not running as recorded"
    echo "ok  $EXPECT is current and every container runs its recorded image"
  fi
  return "$status"
}

cmd_logs() {
  local out="${1:-}" sha work key value file
  [[ -n "$out" ]] || die "logs needs an output path"
  sha="${LOG_SHA:-$(read_pointer current)}"
  umask 077
  work="$(mktemp -d)"
  # shellcheck disable=SC2064 # expand now
  trap "rm -rf '$work'" EXIT
  mkdir -p "$work/releases" "$work/deploy-logs"
  {
    echo "generated_at=$(utc)"
    echo "current_sha=$(read_pointer current)"
    echo "last_known_good_sha=$(read_pointer last-known-good)"
    echo "requested_sha=$sha"
  } >"$work/summary.txt"
  if [[ -f "$RELEASES/history.jsonl" ]]; then
    tail -n 200 "$RELEASES/history.jsonl" >"$work/releases/history.jsonl"
  fi
  local current
  current="$(read_pointer current)"
  for file in "$sha" "$current"; do
    if [[ -n "$file" && -f "$(release_json "$file")" ]]; then
      cp "$(release_json "$file")" "$work/releases/$file.json"
    fi
  done
  if [[ -n "$sha" ]]; then
    # Everything about this release, plus the three newest rollback logs.
    # Names start with <sha>-, promote-<sha>- or rollback- and then a UTC timestamp.
    local files
    files="$(
      for file in "$DEPLOY_LOGS/$sha"-* "$DEPLOY_LOGS/promote-$sha"-*; do
        [[ -f "$file" ]] || continue
        basename "$file"
      done
      for file in "$DEPLOY_LOGS"/rollback-*.log; do
        [[ -f "$file" ]] || continue
        basename "$file"
      done | sort -r | sed -n 1,3p
    )"
    for file in $files; do
      if [[ "$file" == *.tgz ]]; then
        mkdir -p "$work/deploy-logs/${file%.tgz}"
        tar -xzf "$DEPLOY_LOGS/$file" -C "$work/deploy-logs/${file%.tgz}"
      else
        cp "$DEPLOY_LOGS/$file" "$work/deploy-logs/"
      fi
    done
  fi
  # The containers as they are now.
  if [[ -n "$current" && -f "$RELEASES/$current/bundle/stack.sh" ]]; then
    stack_args
    load_images "$current"
    if bash "$RELEASES/$current/bundle/stack.sh" "${STACK_ARGS[@]}" collect-logs "$work/now.tgz" >"$work/collect-logs.txt" 2>&1; then
      mkdir -p "$work/now"
      tar -xzf "$work/now.tgz" -C "$work/now"
      rm -f "$work/now.tgz"
    else
      echo "collect-logs failed; see collect-logs.txt" >>"$work/summary.txt"
    fi
  fi
  if [[ -f "$ENV_FILE" ]]; then
    for key in "${PASSWORD_KEYS[@]}"; do
      value="$(sed -n "s/^$key=//p" "$ENV_FILE" | tail -n 1)"
      [[ -n "$value" ]] || continue
      LM_REDACT="$value" find "$work" -type f -exec perl -pi -e 's/\Q$ENV{LM_REDACT}\E/[REDACTED]/g' {} +
      if grep -rqF -- "$value" "$work"; then
        die "redaction failed for $key; the bundle was not written"
      fi
    done
  else
    echo "no env file at $ENV_FILE: nothing to redact (no secrets were rendered)" >>"$work/summary.txt"
  fi
  COPYFILE_DISABLE=1 tar -czf "$out" -C "$work" .
  if [[ -n "$OWNER" ]]; then
    chown "$OWNER" "$out"
  fi
  echo "deploy: wrote $out (passwords redacted and checked)" >&2
}

case "$COMMAND" in
  deploy) cmd_deploy "${ARGS[1]:-}" ;;
  promote) cmd_promote "${ARGS[1]:-}" ;;
  rollback) cmd_rollback "${ARGS[1]:-}" ;;
  resolve) resolve_target "${ARGS[1]:-}" ;;
  status) cmd_status ;;
  logs) cmd_logs "${ARGS[1]:-}" ;;
  __apply)
    [[ "${ARGS[1]:-}" =~ $SHA_RE ]] || die "__apply needs a SHA"
    case "${ARGS[2]:-}" in deploy | rollback) ;; *) die "__apply mode must be deploy or rollback" ;; esac
    apply_child "${ARGS[1]}" "${ARGS[2]}"
    ;;
  *) usage >&2; die "unknown command: $COMMAND" ;;
esac
