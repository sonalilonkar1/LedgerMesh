#!/usr/bin/env bash
# Look up an image tag on GHCR (LM-047).
#
#   GHCR_USER=... GHCR_TOKEN=... bash infra/scripts/ghcr_digest.sh [--config] OWNER/IMAGE TAG
#
# Prints the tag's manifest digest, or with --config the digest of the image's
# config (for a multi-platform index, the linux/amd64 image's config). The
# config digest is what a pushed image keeps from the local image it came from,
# so it is what to compare with a locally built candidate.
#
# Exit status is the whole contract, so callers can tell "not published" apart
# from "could not find out":
#
#   0  the tag exists; the requested digest (sha256:...) is printed on stdout
#   3  the registry CONFIRMED the tag does not exist: HTTP 404 with the
#      MANIFEST_UNKNOWN or NAME_UNKNOWN error code
#   1  anything else: authentication, network, server errors, or a response
#      that cannot be parsed. Publishing must stop, never assume "not found".
#
# It talks to the registry API directly instead of parsing CLI error text.
set -uo pipefail

NOT_FOUND=3
PLATFORM_OS=linux
PLATFORM_ARCH=amd64
ACCEPT="application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json"
DIGEST_RE='^sha256:[0-9a-f]{64}$'

fail() {
  echo "ghcr_digest: $*" >&2
  exit 1
}

mode=manifest
if [[ "${1:-}" == "--config" ]]; then
  mode=config
  shift
fi
[[ $# -eq 2 ]] || fail "usage: ghcr_digest.sh [--config] OWNER/IMAGE TAG"
repo="$1"
tag="$2"
[[ -n "${GHCR_USER:-}" && -n "${GHCR_TOKEN:-}" ]] || fail "GHCR_USER and GHCR_TOKEN must be set"

workdir="$(mktemp -d)" || fail "cannot create a temporary directory"
trap 'rm -rf "$workdir"' EXIT

# 1. Exchange the workflow token for a registry bearer token for this repository.
token_status="$(curl -sS -o "$workdir/token.json" -w '%{http_code}' \
  -u "${GHCR_USER}:${GHCR_TOKEN}" \
  "https://ghcr.io/token?service=ghcr.io&scope=repository:${repo}:pull")" \
  || fail "token request for ${repo} did not complete"
[[ "$token_status" == "200" ]] || fail "token request for ${repo} returned HTTP ${token_status}"
bearer="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["token"])' \
  "$workdir/token.json" 2>/dev/null)" || fail "token response for ${repo} has no token"
[[ -n "$bearer" ]] || fail "token response for ${repo} has an empty token"

# fetch REF: GET a manifest into $workdir/body and $workdir/headers; prints the HTTP status.
fetch() {
  curl -sS -o "$workdir/body" -D "$workdir/headers" -w '%{http_code}' \
    -H "Authorization: Bearer ${bearer}" \
    -H "Accept: ${ACCEPT}" \
    "https://ghcr.io/v2/${repo}/manifests/$1"
}

# manifest_info platform|config: read the fetched body.
#   platform: print the linux/amd64 entry of an index (exit 0), exit 2 if the
#             body is a single image manifest, exit 4 if the index has no such
#             entry;
#   config:   print the image manifest's config digest.
# Exit 1 if the body cannot be parsed.
manifest_info() {
  python3 - "$workdir/body" "$1" "$PLATFORM_OS" "$PLATFORM_ARCH" <<'PY'
import json
import sys

path, want, os_name, arch = sys.argv[1:5]
try:
    with open(path) as handle:
        manifest = json.load(handle)
except (OSError, ValueError):
    sys.exit(1)
if not isinstance(manifest, dict):
    sys.exit(1)

if want == "platform":
    entries = manifest.get("manifests")
    if entries is None:
        sys.exit(2)
    for entry in entries if isinstance(entries, list) else []:
        platform = entry.get("platform") or {}
        if platform.get("os") == os_name and platform.get("architecture") == arch:
            print(entry.get("digest", ""))
            sys.exit(0)
    sys.exit(4)

digest = (manifest.get("config") or {}).get("digest")
if not isinstance(digest, str):
    sys.exit(1)
print(digest)
PY
}

# 2. Ask for the tag's manifest.
status="$(fetch "$tag")" || fail "manifest request for ${repo}:${tag} did not complete"

case "$status" in
  200) ;;
  404)
    if grep -qE '"(MANIFEST_UNKNOWN|NAME_UNKNOWN)"' "$workdir/body"; then
      exit "$NOT_FOUND"
    fi
    fail "${repo}:${tag} returned 404 without a not-found error code"
    ;;
  *)
    fail "manifest request for ${repo}:${tag} returned HTTP ${status}"
    ;;
esac

if [[ "$mode" == "manifest" ]]; then
  digest="$(grep -i '^docker-content-digest:' "$workdir/headers" | tail -1 \
    | cut -d: -f2- | tr -d ' \r')"
  [[ "$digest" =~ $DIGEST_RE ]] \
    || fail "${repo}:${tag} returned 200 without a valid Docker-Content-Digest"
  echo "$digest"
  exit 0
fi

# --config: a multi-platform index first points at the platform's own manifest.
info_status=0
child="$(manifest_info platform)" || info_status=$?
case "$info_status" in
  0)
    [[ "$child" =~ $DIGEST_RE ]] \
      || fail "${repo}:${tag} index has an invalid ${PLATFORM_OS}/${PLATFORM_ARCH} digest"
    status="$(fetch "$child")" || fail "manifest request for ${repo}@${child} did not complete"
    [[ "$status" == "200" ]] || fail "manifest ${repo}@${child} returned HTTP ${status}"
    ;;
  2) ;;
  4) fail "${repo}:${tag} is an index without a ${PLATFORM_OS}/${PLATFORM_ARCH} image" ;;
  *) fail "${repo}:${tag} returned a manifest that cannot be parsed" ;;
esac

config="$(manifest_info config)" || fail "${repo}:${tag} manifest has no config digest"
[[ "$config" =~ $DIGEST_RE ]] || fail "${repo}:${tag} has an invalid config digest"
echo "$config"
