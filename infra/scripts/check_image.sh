#!/usr/bin/env bash
# Verify a built LedgerMesh service image (LM-047).
#
#   bash infra/scripts/check_image.sh IMAGE EXPECTED_SERVICE PORT
#
# Checks that the image starts on its own (no database needed for /health),
# answers its own /health, runs as a non-root user, and contains no .env files,
# git metadata, private keys, credentials or tokens.
set -euo pipefail

image="$1"
expected_service="$2"
port="$3"
name="lm047-check-${expected_service}-$$"

cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$name" -p "127.0.0.1::${port}" "$image" >/dev/null
host_port="$(docker port "$name" "${port}/tcp" | head -1 | sed 's/.*://')"

body=""
for _ in $(seq 1 30); do
  if body="$(curl -fsS "http://127.0.0.1:${host_port}/health" 2>/dev/null)"; then
    break
  fi
  sleep 1
done
if [[ -z "$body" ]]; then
  echo "FAIL: ${image} did not answer /health" >&2
  docker logs "$name" >&2 || true
  exit 1
fi
python3 -c '
import json, sys
body = json.loads(sys.argv[1])
expected = {"status": "ok", "service": sys.argv[2]}
assert body == expected, f"unexpected /health body {body!r}, expected {expected!r}"
' "$body" "$expected_service"
echo "ok: /health -> ${body}"

uid="$(docker exec "$name" id -u)"
gid="$(docker exec "$name" id -g)"
if [[ "$uid" == "0" || "$gid" == "0" ]]; then
  echo "FAIL: container runs as uid=${uid} gid=${gid}" >&2
  exit 1
fi
echo "ok: runs as uid=${uid} gid=${gid}"

# The forbidden-file scan, run inside the container as root so directories the
# service user cannot read are covered too. $1 is the directory to scan ("/").
#
# It prints every forbidden path, and exits 0 only if the scan COMPLETED; any
# other exit means it did not, and the check fails instead of reading the empty
# output as "clean":
#   - any *.key file, .env*, .git, .git-credentials or .netrc is forbidden;
#   - a *.pem is forbidden only if it holds a private key, because public CA
#     bundles (e.g. certifi's cacert.pem) are legitimately present. grep exits
#     1 for "no private key", which is clean; 2 or more is a scan failure.
read -r -d '' SCAN <<'EOF' || true
set -u
root="$1"
find "$root" -xdev \( -name ".env" -o -name ".env.*" -o -name ".git" \
   -o -name ".git-credentials" -o -name ".netrc" -o -name "*.key" \) -print \
   || exit 10
pems="$(find "$root" -xdev -type f -name "*.pem" -print)" || exit 11
[ -n "$pems" ] || exit 0
printf '%s\n' "$pems" | while IFS= read -r pem; do
  status=0
  grep -l "PRIVATE KEY" "$pem" || status=$?
  [ "$status" -le 1 ] || exit 12
done
EOF

scan_status=0
found="$(docker exec -u 0 "$name" sh -c "$SCAN" scan /)" || scan_status=$?
if [[ "$scan_status" -ne 0 ]]; then
  echo "FAIL: the forbidden-file scan did not complete (exit ${scan_status})" >&2
  exit 1
fi
if [[ -n "$found" ]]; then
  echo "FAIL: forbidden files in image:" >&2
  echo "$found" >&2
  exit 1
fi

# Read the environment first, so a failed `docker exec` fails the check instead
# of looking like "no credential variables".
env_status=0
env_output="$(docker exec "$name" env)" || env_status=$?
if [[ "$env_status" -ne 0 ]]; then
  echo "FAIL: could not read the container environment (exit ${env_status})" >&2
  exit 1
fi
match_status=0
grep -iqE '^(GITHUB_TOKEN|GH_TOKEN|.*_SECRET|.*PASSWORD)=' <<<"$env_output" || match_status=$?
case "$match_status" in
  0)
    echo "FAIL: credentials present in the image environment" >&2
    exit 1
    ;;
  1) ;;
  *)
    echo "FAIL: could not check the container environment (grep exit ${match_status})" >&2
    exit 1
    ;;
esac
echo "ok: no .env files, git metadata, private keys or credentials"

# Read the logs once: piping straight into `grep -q` can SIGPIPE docker logs,
# which pipefail would report as a missing match.
logs="$(docker logs "$name" 2>&1)"
revision="$(printf '%s\n' "$logs" | sed -n 's/.*\(revision=[^ ]*\).*/\1/p' | sed -n 1p)"
if [[ -z "$revision" ]]; then
  echo "FAIL: startup log does not report the image revision" >&2
  printf '%s\n' "$logs" >&2
  exit 1
fi
echo "ok: startup log reports ${revision}"
