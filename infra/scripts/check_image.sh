#!/usr/bin/env bash
# Verify a built LedgerMesh service image (LM-047).
#
#   bash infra/scripts/check_image.sh IMAGE EXPECTED_SERVICE PORT
#
# Checks that the image starts on its own (no database needed for /health),
# answers its own /health, runs as a non-root user, and contains no .env files,
# git metadata, credentials or tokens.
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

found="$(docker exec "$name" sh -c \
  'find / -xdev \( -name ".env" -o -name ".env.*" -o -name ".git" \
     -o -name ".git-credentials" -o -name ".netrc" \) -print 2>/dev/null' || true)"
if [[ -n "$found" ]]; then
  echo "FAIL: forbidden files in image:" >&2
  echo "$found" >&2
  exit 1
fi
if docker exec "$name" env | grep -iE '^(GITHUB_TOKEN|GH_TOKEN|.*_SECRET|.*PASSWORD)=' >/dev/null; then
  echo "FAIL: credentials present in the image environment" >&2
  exit 1
fi
echo "ok: no .env files, git metadata or credentials"

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
