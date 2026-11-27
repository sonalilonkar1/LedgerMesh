#!/usr/bin/env bash
# Operate the hosted staging Compose stack (LM-048).
#
#   stack.sh [options] <command> [args]
#
# Commands
#   up                  Start the data tier, verify prepared transactions, run
#                       migrations and the LM-018 topic bootstrap, start
#                       everything, then run every check. Needs --sha or the
#                       LEDGERMESH_*_IMAGE variables.
#   migrate             Run `alembic upgrade head` for the four services.
#   topics              Run the LM-018 topic bootstrap (idempotent).
#   verify <check>      prepared-transactions | topics | ports | db-isolation |
#                       health | workers | all
#   ps                  Show the containers.
#   logs [service...]   Print recent logs (--tail, --follow).
#   collect-logs <out.tgz>
#                       Write a secret-free log bundle for debugging.
#   restart             Restart every container (data is kept).
#   down                Stop and remove the containers. Data on --data-dir is
#                       kept; this script never deletes data.
#
# Options
#   --env-file <path>       Rendered env file (default /etc/ledgermesh/staging.env,
#                           see render-env.sh). Must not be group/other readable.
#   --data-dir <path>       Absolute data directory (default /var/lib/ledgermesh).
#   --project-name <name>   Compose project (default ledgermesh-staging).
#   --sha <40-hex sha>      Use ghcr.io/sonalilonkar1/ledgermesh-<service>:<sha>.
#                           Without it, LEDGERMESH_*_IMAGE from the environment
#                           are used, or else the images of the running stack.
#   --wait-timeout <secs>   Bound for every wait (default 300).
#   --ca-file <path>        CA bundle for the HTTPS checks. Defaults to Caddy's
#                           local root CA when LEDGERMESH_TLS=internal.
#   --https-port <port>     Host HTTPS port for the checks (default
#                           LEDGERMESH_HTTPS_PORT or 443).
#   --tail <n>              Log lines per service for logs/collect-logs (default 2000).
#   --follow                Follow logs.
#   --prepared-max-age <s>  Fail when a prepared transaction is older (default 300).
#   --worker-window <s>     How long workers must stay up without restarting (default 20).
#
# Compatible with bash 3.2 (macOS) as well as the VM's bash.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/compose.yaml"
REGISTRY="ghcr.io/sonalilonkar1"

# The apache/kafka image runs as this UID (checked with
# `docker run --rm --entrypoint id apache/kafka:4.4.0`); vm-startup.sh uses the same value.
KAFKA_UID=1000

ENV_FILE="/etc/ledgermesh/staging.env"
DATA_DIR="/var/lib/ledgermesh"
PROJECT="ledgermesh-staging"
SHA=""
WAIT_TIMEOUT=300
CA_FILE=""
HTTPS_PORT="${LEDGERMESH_HTTPS_PORT:-443}"
TAIL=2000
FOLLOW=0
PREPARED_MAX_AGE=300
WORKER_WINDOW=20

# service:module:port:db-service:db-user:db-name:image-variable
SERVICES=(
  "coordinator:coordinator:8000:coordinator-db:coordinator:coordinator:LEDGERMESH_COORDINATOR_IMAGE"
  "wallet:wallet:8001:wallet-db:wallet:wallet:LEDGERMESH_WALLET_IMAGE"
  "catalog:catalog:8002:catalog-db:catalog:catalog:LEDGERMESH_CATALOG_IMAGE"
  "audit-ledger:audit_ledger:8003:audit-ledger-db:audit_ledger:audit_ledger:LEDGERMESH_AUDIT_LEDGER_IMAGE"
)
WORKERS=(coordinator-saga wallet-saga catalog-saga audit-consumer)
DATA_TIER=(kafka coordinator-db wallet-db catalog-db audit-ledger-db)
PASSWORD_KEYS=(COORDINATOR_DB_PASSWORD WALLET_DB_PASSWORD CATALOG_DB_PASSWORD AUDIT_LEDGER_DB_PASSWORD)

die() {
  echo "stack: $*" >&2
  exit 1
}

log() {
  echo "stack: $*" >&2
}

usage() {
  sed -n '2,42p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

field() {
  # field <spec> <index>: one ':'-separated field of a SERVICES entry.
  echo "$1" | cut -d: -f"$2"
}

# ---- argument parsing -----------------------------------------------------------

ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) [[ $# -ge 2 ]] || die "$1 needs a value"; ENV_FILE="$2"; shift 2 ;;
    --data-dir) [[ $# -ge 2 ]] || die "$1 needs a value"; DATA_DIR="$2"; shift 2 ;;
    --project-name) [[ $# -ge 2 ]] || die "$1 needs a value"; PROJECT="$2"; shift 2 ;;
    --sha) [[ $# -ge 2 ]] || die "$1 needs a value"; SHA="$2"; shift 2 ;;
    --wait-timeout) [[ $# -ge 2 ]] || die "$1 needs a value"; WAIT_TIMEOUT="$2"; shift 2 ;;
    --ca-file) [[ $# -ge 2 ]] || die "$1 needs a value"; CA_FILE="$2"; shift 2 ;;
    --https-port) [[ $# -ge 2 ]] || die "$1 needs a value"; HTTPS_PORT="$2"; shift 2 ;;
    --tail) [[ $# -ge 2 ]] || die "$1 needs a value"; TAIL="$2"; shift 2 ;;
    --follow) FOLLOW=1; shift ;;
    --prepared-max-age) [[ $# -ge 2 ]] || die "$1 needs a value"; PREPARED_MAX_AGE="$2"; shift 2 ;;
    --worker-window) [[ $# -ge 2 ]] || die "$1 needs a value"; WORKER_WINDOW="$2"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    *) ARGS+=("$1"); shift ;;
  esac
done
[[ ${#ARGS[@]} -ge 1 ]] || { usage >&2; exit 2; }
COMMAND="${ARGS[0]}"
CMD_ARGS=()
if [[ ${#ARGS[@]} -gt 1 ]]; then
  CMD_ARGS=("${ARGS[@]:1}")
fi

[[ "$PROJECT" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || die "invalid --project-name: $PROJECT"
[[ "$DATA_DIR" == /* ]] || die "--data-dir must be an absolute path (Compose resolves relative paths against infra/staging/)"
for number in "$WAIT_TIMEOUT" "$TAIL" "$PREPARED_MAX_AGE" "$WORKER_WINDOW" "$HTTPS_PORT"; do
  [[ "$number" =~ ^[0-9]+$ ]] || die "expected a whole number, got: $number"
done
if [[ -n "$SHA" && ! "$SHA" =~ ^[0-9a-f]{40}$ ]]; then
  die "--sha must be a full 40-character lowercase commit SHA"
fi

# ---- environment -----------------------------------------------------------------

file_mode() {
  stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"
}

[[ -f "$ENV_FILE" ]] || die "env file not found: $ENV_FILE (render it with render-env.sh)"
mode="$(file_mode "$ENV_FILE")"
[[ "$mode" =~ ^[0-7]?[0-7]00$ ]] || die "$ENV_FILE has mode $mode; it contains secrets and must be 0600"

env_value() {
  # The last KEY=VALUE line for KEY; render-env.sh writes plain values.
  sed -n "s/^$1=//p" "$ENV_FILE" | tail -n 1
}

export LEDGERMESH_DATA_DIR="$DATA_DIR"
export LEDGERMESH_HTTPS_PORT="$HTTPS_PORT"

running_image() {
  docker ps -a \
    --filter "label=com.docker.compose.project=$PROJECT" \
    --filter "label=com.docker.compose.service=$1" \
    --format '{{.Image}}' | head -n 1
}

resolve_images() {
  # $1 = required|optional. Sets the four LEDGERMESH_*_IMAGE variables.
  local need="$1" spec service var current
  for spec in "${SERVICES[@]}"; do
    service="$(field "$spec" 1)"
    var="$(field "$spec" 7)"
    if [[ -n "$SHA" ]]; then
      export "$var=$REGISTRY/ledgermesh-$service:$SHA"
      continue
    fi
    current="${!var:-}"
    if [[ -z "$current" ]]; then
      current="$(running_image "$service")"
    fi
    if [[ -z "$current" ]]; then
      [[ "$need" == "optional" ]] || die "no image for $service: pass --sha or set $var"
      # Only to let Compose parse the file for ps/logs/down; nothing is pulled.
      current="not-deployed"
    fi
    export "$var=$current"
  done
}

COMPOSE=(docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --env-file "$ENV_FILE")

compose() {
  "${COMPOSE[@]}" "$@"
}

prepare_dirs() {
  local dir
  for dir in kafka postgres/coordinator postgres/wallet postgres/catalog postgres/audit-ledger caddy/data caddy/config; do
    mkdir -p "$DATA_DIR/$dir"
  done
  if [[ "$(id -u)" == "0" ]]; then
    chown "$KAFKA_UID:$KAFKA_UID" "$DATA_DIR/kafka"
  fi
}

# ---- one-off steps -----------------------------------------------------------------

run_migrations() {
  local spec service module
  for spec in "${SERVICES[@]}"; do
    service="$(field "$spec" 1)"
    module="$(field "$spec" 2)"
    log "migrating $service"
    compose run --rm --no-deps -T "$service" \
      alembic -c "services/$module/alembic.ini" upgrade head
  done
}

run_topics() {
  log "creating Kafka topics (LM-018 bootstrap)"
  compose run --rm --no-deps -T coordinator \
    python -m ledgermesh_messaging.topics --bootstrap-servers kafka:19092 --replication-factor 1
}

# ---- checks ------------------------------------------------------------------------

verify_prepared_transactions() {
  local spec db user name value stats count age
  for spec in "${SERVICES[@]}"; do
    db="$(field "$spec" 4)"
    user="$(field "$spec" 5)"
    name="$(field "$spec" 6)"
    value="$(compose exec -T "$db" psql -U "$user" -d "$name" -Atqc "SHOW max_prepared_transactions")"
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
      die "$db: max_prepared_transactions=$value (must be > 0 for 2PC)"
    fi
    stats="$(compose exec -T "$db" psql -U "$user" -d "$name" -Atqc \
      "SELECT count(*), coalesce(floor(extract(epoch FROM now() - min(prepared))), 0)::bigint FROM pg_prepared_xacts")"
    count="${stats%%|*}"
    age="${stats##*|}"
    [[ "$count" =~ ^[0-9]+$ && "$age" =~ ^[0-9]+$ ]] || die "$db: unexpected pg_prepared_xacts result"
    echo "ok  $db max_prepared_transactions=$value prepared_now=$count oldest_age_s=$age"
    if [[ "$age" -gt "$PREPARED_MAX_AGE" ]]; then
      die "$db: a prepared transaction is ${age}s old (limit ${PREPARED_MAX_AGE}s); inspect it before it blocks rows (infra/README.md)"
    fi
  done
}

verify_topics() {
  local expected actual config name partitions found
  expected="$(compose run --rm --no-deps -T coordinator python -c \
    'from ledgermesh_messaging.topics import ALL_TOPICS
for t in ALL_TOPICS: print(t.name, t.partitions)')"
  [[ -n "$expected" ]] || die "could not read the LM-018 topic definitions"
  actual="$(compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:19092 --describe --exclude-internal \
    | awk '$1 == "Topic:" { for (i = 1; i <= NF; i++) if ($i == "PartitionCount:") print $2, $(i + 1) }')"
  while read -r name partitions; do
    found="$(echo "$actual" | awk -v n="$name" '$1 == n { print $2 }')"
    [[ -n "$found" ]] || die "topic $name is missing"
    [[ "$found" == "$partitions" ]] || die "topic $name has $found partitions, expected $partitions"
  done <<<"$expected"
  while read -r name partitions; do
    [[ -z "$name" ]] && continue
    echo "$expected" | awk -v n="$name" '$1 == n { f = 1 } END { exit !f }' \
      || die "unexpected topic $name (auto-created or not defined by LM-018)"
  done <<<"$actual"
  echo "ok  $(echo "$expected" | wc -l | tr -d ' ') topics match the LM-018 definitions"
  config="$(compose exec -T kafka /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server localhost:19092 --entity-type brokers --entity-name 1 --describe --all)"
  echo "$config" | grep -q 'auto.create.topics.enable=false' \
    || die "auto.create.topics.enable is not false on the broker"
  echo "ok  auto.create.topics.enable=false"
}

project_containers() {
  docker ps -a --filter "label=com.docker.compose.project=$PROJECT" --format '{{.Names}}'
}

verify_ports() {
  local name service bindings mode count=0
  while read -r name; do
    [[ -z "$name" ]] && continue
    count=$((count + 1))
    service="$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.service" }}' "$name")"
    bindings="$(docker inspect -f '{{ range $port, $b := .HostConfig.PortBindings }}{{ $port }} {{ end }}' "$name")"
    bindings="$(echo "$bindings" | tr ' ' '\n' | sed '/^$/d' | sort | tr '\n' ' ' | sed 's/ $//')"
    mode="$(docker inspect -f '{{ .HostConfig.NetworkMode }}' "$name")"
    [[ "$mode" != "host" ]] || die "$name uses host networking"
    if [[ "$service" == "caddy" ]]; then
      [[ "$bindings" == "443/tcp 80/tcp" ]] || die "caddy publishes '$bindings', expected only 80/tcp and 443/tcp"
    elif [[ -n "$bindings" ]]; then
      die "$name publishes host ports ($bindings); only caddy may"
    fi
  done <<<"$(project_containers)"
  [[ "$count" -gt 0 ]] || die "no containers found for project $PROJECT"
  echo "ok  only caddy publishes host ports (80/tcp, 443/tcp) across $count containers"
}

# Runs inside each API container with that service's own DATABASE_URL, so no
# password is ever passed on a command line. Own database: must connect.
# Every other service's database: must be rejected by authentication.
DB_ISOLATION_PY='
import os, sys
import psycopg
from sqlalchemy.engine import make_url

url = make_url(os.environ["DATABASE_URL"])
own = dict(user=url.username, password=url.password, connect_timeout=5)
with psycopg.connect(host=url.host, port=url.port or 5432, dbname=url.database, **own):
    pass
failed = False
for target in sys.argv[1:]:
    host, dbname = target.split("/")
    try:
        psycopg.connect(host=host, port=5432, dbname=dbname, **own).close()
    except psycopg.OperationalError as error:
        text = str(error)
        if "password authentication failed" in text or "does not exist" in text:
            print(f"ok  {url.username} rejected by {host}")
            continue
        print(f"FAIL {url.username} -> {host}: not an authentication rejection: {text}")
        failed = True
    else:
        print(f"FAIL {url.username} could log in to {host}/{dbname}")
        failed = True
sys.exit(1 if failed else 0)
'

verify_db_isolation() {
  local spec other service targets
  for spec in "${SERVICES[@]}"; do
    service="$(field "$spec" 1)"
    targets=()
    for other in "${SERVICES[@]}"; do
      [[ "$other" == "$spec" ]] && continue
      targets+=("$(field "$other" 4)/$(field "$other" 6)")
    done
    compose exec -T "$service" python -c "$DB_ISOLATION_PY" "${targets[@]}" \
      || die "$service: database isolation check failed"
  done
}

container_health() {
  docker inspect -f '{{ if .State.Health }}{{ .State.Health.Status }}{{ else }}none{{ end }}' "$1"
}

verify_health() {
  local name service health site tls ca path expected body deadline delay curl_args
  while read -r name; do
    [[ -z "$name" ]] && continue
    service="$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.service" }}' "$name")"
    [[ "$name" == *-run-* ]] && continue
    health="$(container_health "$name")"
    case "$service" in
      coordinator-saga | wallet-saga | catalog-saga | audit-consumer) continue ;;
    esac
    [[ "$health" == "healthy" ]] || die "$service is $health"
    echo "ok  $service healthy"
  done <<<"$(project_containers)"

  site="$(env_value LEDGERMESH_SITE_ADDRESS)"
  tls="$(env_value LEDGERMESH_TLS)"
  ca="$CA_FILE"
  if [[ -z "$ca" && "$tls" == "internal" ]]; then
    ca="$DATA_DIR/caddy/data/caddy/pki/authorities/local/root.crt"
  fi
  curl_args=(--fail --silent --show-error --max-time 10 --resolve "$site:$HTTPS_PORT:127.0.0.1")
  if [[ -n "$ca" ]]; then
    curl_args+=(--cacert "$ca")
  fi
  deadline=$(($(date +%s) + WAIT_TIMEOUT))
  for path in /health /health/coordinator /health/wallet /health/catalog /health/audit-ledger; do
    expected="${path#/health/}"
    [[ "$path" == "/health" ]] && expected="coordinator"
    delay=1
    while true; do
      # Retried until the deadline: right after the first start the
      # certificate may still be being issued.
      body=""
      if [[ -z "$ca" || -f "$ca" ]]; then
        if ! body="$(curl "${curl_args[@]}" "https://$site:$HTTPS_PORT$path" 2>&1)"; then
          log "waiting for https://$site$path: $body"
          body=""
        fi
      fi
      if echo "$body" | grep -Eq "\"service\": ?\"$expected\""; then
        echo "ok  https://$site$path -> $expected"
        break
      fi
      [[ "$(date +%s)" -lt "$deadline" ]] || die "https://$site:$HTTPS_PORT$path did not answer for $expected within ${WAIT_TIMEOUT}s"
      sleep "$delay"
      delay=$((delay * 2 > 10 ? 10 : delay * 2))
    done
  done
}

worker_state() {
  local name
  name="$(docker ps -a --filter "label=com.docker.compose.project=$PROJECT" \
    --filter "label=com.docker.compose.service=$1" --format '{{.Names}}' | grep -v -- '-run-' | head -n 1)"
  [[ -n "$name" ]] || { echo "missing"; return 0; }
  docker inspect -f '{{ .State.Status }} {{ .RestartCount }} {{ .State.StartedAt }}' "$name"
}

verify_workers() {
  local worker before after
  local states=()
  for worker in "${WORKERS[@]}"; do
    before="$(worker_state "$worker")"
    [[ "$before" == running\ * ]] || die "$worker is not running ($before)"
    states+=("$before")
  done
  sleep "$WORKER_WINDOW"
  local index=0
  for worker in "${WORKERS[@]}"; do
    after="$(worker_state "$worker")"
    [[ "$after" == "${states[$index]}" ]] \
      || die "$worker restarted or stopped during ${WORKER_WINDOW}s (before: ${states[$index]}, after: $after)"
    echo "ok  $worker running, no restarts in ${WORKER_WINDOW}s"
    index=$((index + 1))
  done
}

verify() {
  local check="${1:-}"
  case "$check" in
    prepared-transactions) verify_prepared_transactions ;;
    topics) verify_topics ;;
    ports) verify_ports ;;
    db-isolation) verify_db_isolation ;;
    health) verify_health ;;
    workers) verify_workers ;;
    all)
      verify_health
      verify_ports
      verify_prepared_transactions
      verify_topics
      verify_db_isolation
      verify_workers
      ;;
    *) die "verify needs one of: prepared-transactions topics ports db-isolation health workers all" ;;
  esac
}

# ---- logs ----------------------------------------------------------------------------

collect_logs() {
  local out="$1" work service key value
  [[ -n "$out" ]] || die "collect-logs needs an output path"
  umask 077
  work="$(mktemp -d)"
  # shellcheck disable=SC2064 # expand $work now
  trap "rm -rf '$work'" EXIT
  mkdir -p "$work/logs"
  compose ps -a >"$work/ps.txt"
  for service in $(compose config --services); do
    compose logs --no-color --timestamps --tail "$TAIL" "$service" >"$work/logs/$service.log" 2>&1
  done
  # Container state only; `docker inspect` in full would include the environment.
  while read -r name; do
    [[ -z "$name" ]] && continue
    docker inspect -f '{{ .Name }} image={{ .Config.Image }} status={{ .State.Status }} exit={{ .State.ExitCode }} restarts={{ .RestartCount }} started={{ .State.StartedAt }} health={{ if .State.Health }}{{ .State.Health.Status }}{{ else }}none{{ end }}' "$name"
  done <<<"$(project_containers)" >"$work/containers.txt"

  for key in "${PASSWORD_KEYS[@]}"; do
    value="$(env_value "$key")"
    [[ -n "$value" ]] || die "$key is missing from $ENV_FILE"
    LM_REDACT="$value" find "$work" -type f -exec perl -pi -e 's/\Q$ENV{LM_REDACT}\E/[REDACTED]/g' {} +
    if grep -rqF -- "$value" "$work"; then
      die "redaction failed for $key; the bundle was not written"
    fi
  done
  # COPYFILE_DISABLE stops macOS tar adding AppleDouble files; ignored on Linux.
  COPYFILE_DISABLE=1 tar -czf "$out" -C "$work" .
  echo "stack: wrote $out (passwords redacted and checked)"
}

# ---- commands ------------------------------------------------------------------------

case "$COMMAND" in
  up)
    resolve_images required
    prepare_dirs
    log "starting the data tier"
    compose up -d --wait --wait-timeout "$WAIT_TIMEOUT" "${DATA_TIER[@]}"
    verify_prepared_transactions
    run_migrations
    run_topics
    verify_topics
    log "starting every service"
    compose up -d --wait --wait-timeout "$WAIT_TIMEOUT" --remove-orphans
    verify all
    log "stack is up and verified"
    ;;
  migrate) resolve_images required; run_migrations ;;
  topics) resolve_images required; run_topics ;;
  verify)
    resolve_images optional
    verify "${CMD_ARGS[0]:-}"
    ;;
  ps) resolve_images optional; compose ps -a ;;
  logs)
    resolve_images optional
    if [[ "$FOLLOW" == 1 ]]; then
      compose logs --timestamps --tail "$TAIL" --follow ${CMD_ARGS[@]+"${CMD_ARGS[@]}"}
    else
      compose logs --timestamps --tail "$TAIL" ${CMD_ARGS[@]+"${CMD_ARGS[@]}"}
    fi
    ;;
  collect-logs) resolve_images optional; collect_logs "${CMD_ARGS[0]:-}" ;;
  restart) resolve_images optional; compose restart ;;
  down) resolve_images optional; compose down --remove-orphans ;;
  *) usage >&2; die "unknown command: $COMMAND" ;;
esac
