#!/usr/bin/env bash
# Render the hosted staging environment file (LM-048).
#
#   render-env.sh [--source gcp|file:<path>] [--out <path>] [--project <id>]
#                 [--secret-prefix <prefix>]
#
# --source gcp (default, on the VM)
#     Reads the four database passwords from Secret Manager with the VM's
#     service account (gcloud uses the metadata server credentials), and the
#     non-secret site address and ACME email from instance metadata
#     (ledgermesh-site-address, ledgermesh-acme-email) set by provision.sh.
# --source file:<path> (local validation)
#     Reads the same keys from a KEY=VALUE file such as infra/staging/.env.local.
#
# The output (default /etc/ledgermesh/staging.env) is written atomically with
# mode 0600, owned by root when run as root. Values are validated and never
# printed. Docker Compose reads this file with --env-file; it is never
# committed, copied into a release record or included in a log bundle.
#
# Compatible with bash 3.2 (macOS) as well as the VM's bash.
set -euo pipefail

SOURCE="gcp"
OUT="/etc/ledgermesh/staging.env"
PROJECT=""
SECRET_PREFIX="ledgermesh-staging"
METADATA_URL="http://metadata.google.internal/computeMetadata/v1"

# Environment key -> Secret Manager secret suffix.
PASSWORD_KEYS=(COORDINATOR_DB_PASSWORD WALLET_DB_PASSWORD CATALOG_DB_PASSWORD AUDIT_LEDGER_DB_PASSWORD)
secret_suffix() {
  case "$1" in
    COORDINATOR_DB_PASSWORD) echo "coordinator-db-password" ;;
    WALLET_DB_PASSWORD) echo "wallet-db-password" ;;
    CATALOG_DB_PASSWORD) echo "catalog-db-password" ;;
    AUDIT_LEDGER_DB_PASSWORD) echo "audit-ledger-db-password" ;;
    *) return 1 ;;
  esac
}

die() {
  echo "render-env: $*" >&2
  exit 1
}

usage() {
  sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) [[ $# -ge 2 ]] || die "--source needs a value"; SOURCE="$2"; shift 2 ;;
    --out) [[ $# -ge 2 ]] || die "--out needs a value"; OUT="$2"; shift 2 ;;
    --project) [[ $# -ge 2 ]] || die "--project needs a value"; PROJECT="$2"; shift 2 ;;
    --secret-prefix) [[ $# -ge 2 ]] || die "--secret-prefix needs a value"; SECRET_PREFIX="$2"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

# Passwords are embedded in DATABASE_URL, so only URL-safe characters.
valid_password() { [[ "$1" =~ ^[A-Za-z0-9._~-]{16,128}$ ]]; }
valid_site() { [[ "$1" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$ ]]; }
valid_tls() { [[ "$1" == "internal" || "$1" =~ ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$ ]]; }

# Values live in V_<KEY> variables (bash 3.2 has no associative arrays).
set_value() { printf -v "V_$1" '%s' "$2"; }
get_value() {
  local name="V_$1"
  printf '%s' "${!name:-}"
}

read_file_source() {
  local path="$1" line key value
  [[ -f "$path" ]] || die "source file not found: $path"
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    [[ "$line" == *=* ]] || die "malformed line in $path (expected KEY=VALUE)"
    key="${line%%=*}"
    value="${line#*=}"
    case "$key" in
      LEDGERMESH_SITE_ADDRESS | LEDGERMESH_TLS | COORDINATOR_DB_PASSWORD | WALLET_DB_PASSWORD | CATALOG_DB_PASSWORD | AUDIT_LEDGER_DB_PASSWORD)
        set_value "$key" "$value" ;;
      *) die "unexpected key in $path: $key" ;;
    esac
  done <"$path"
}

metadata_attribute() {
  curl --fail --silent --show-error --max-time 5 \
    -H "Metadata-Flavor: Google" "$METADATA_URL/instance/attributes/$1"
}

read_gcp_source() {
  command -v gcloud >/dev/null || die "gcloud is not installed"
  local key name value
  local cmd=(gcloud secrets versions access latest)
  if [[ -n "$PROJECT" ]]; then
    cmd+=(--project "$PROJECT")
  fi
  for key in "${PASSWORD_KEYS[@]}"; do
    name="${SECRET_PREFIX}-$(secret_suffix "$key")"
    if ! value="$("${cmd[@]}" --secret="$name")"; then
      die "could not read secret $name"
    fi
    set_value "$key" "$value"
  done
  value="$(metadata_attribute ledgermesh-site-address)" \
    || die "instance metadata ledgermesh-site-address is missing"
  set_value LEDGERMESH_SITE_ADDRESS "$value"
  value="$(metadata_attribute ledgermesh-acme-email)" \
    || die "instance metadata ledgermesh-acme-email is missing"
  set_value LEDGERMESH_TLS "$value"
}

case "$SOURCE" in
  gcp) read_gcp_source ;;
  file:?*) read_file_source "${SOURCE#file:}" ;;
  *) die "--source must be gcp or file:<path>" ;;
esac

for key in "${PASSWORD_KEYS[@]}"; do
  [[ -n "$(get_value "$key")" ]] || die "$key is missing"
  valid_password "$(get_value "$key")" || die "$key must be 16-128 characters of [A-Za-z0-9._~-]"
done
for key in "${PASSWORD_KEYS[@]}"; do
  for other in "${PASSWORD_KEYS[@]}"; do
    if [[ "$key" < "$other" && "$(get_value "$key")" == "$(get_value "$other")" ]]; then
      die "$key and $other must differ: each service has its own credentials"
    fi
  done
done
valid_site "$(get_value LEDGERMESH_SITE_ADDRESS)" || die "LEDGERMESH_SITE_ADDRESS is missing or not a hostname"
valid_tls "$(get_value LEDGERMESH_TLS)" || die "LEDGERMESH_TLS must be 'internal' or an email address"

out_dir="$(dirname "$OUT")"
if [[ ! -d "$out_dir" ]]; then
  mkdir -p "$out_dir"
  chmod 0700 "$out_dir"
fi

umask 077
tmp="$(mktemp "$out_dir/.staging.env.XXXXXX")"
trap 'rm -f "$tmp"' EXIT
{
  echo "# Rendered by infra/staging/render-env.sh. Contains secrets: do not copy or commit."
  for key in LEDGERMESH_SITE_ADDRESS LEDGERMESH_TLS "${PASSWORD_KEYS[@]}"; do
    printf '%s=%s\n' "$key" "$(get_value "$key")"
  done
} >"$tmp"
chmod 0600 "$tmp"
if [[ "$(id -u)" == "0" ]]; then
  chown root:root "$tmp"
fi
mv -f "$tmp" "$OUT"
trap - EXIT
echo "render-env: wrote $OUT (mode 0600, ${#PASSWORD_KEYS[@]} passwords, values not shown)"
