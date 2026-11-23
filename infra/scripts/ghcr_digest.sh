#!/usr/bin/env bash
# Look up the manifest digest of an image tag on GHCR (LM-047).
#
#   GHCR_USER=... GHCR_TOKEN=... bash infra/scripts/ghcr_digest.sh OWNER/IMAGE TAG
#
# Exit status is the whole contract, so callers can tell "not published" apart
# from "could not find out":
#
#   0  the tag exists; its digest (sha256:...) is printed on stdout
#   3  the registry CONFIRMED the tag does not exist: HTTP 404 with the
#      MANIFEST_UNKNOWN or NAME_UNKNOWN error code
#   1  anything else: authentication, network, server errors, or a response
#      that cannot be parsed. Publishing must stop, never assume "not found".
#
# It talks to the registry API directly instead of parsing CLI error text.
set -uo pipefail

NOT_FOUND=3
ACCEPT="application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json"

fail() {
  echo "ghcr_digest: $*" >&2
  exit 1
}

[[ $# -eq 2 ]] || fail "usage: ghcr_digest.sh OWNER/IMAGE TAG"
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

# 2. Ask for the tag's manifest.
status="$(curl -sS -o "$workdir/body" -D "$workdir/headers" -w '%{http_code}' \
  -H "Authorization: Bearer ${bearer}" \
  -H "Accept: ${ACCEPT}" \
  "https://ghcr.io/v2/${repo}/manifests/${tag}")" \
  || fail "manifest request for ${repo}:${tag} did not complete"

case "$status" in
  200)
    digest="$(grep -i '^docker-content-digest:' "$workdir/headers" | tail -1 \
      | cut -d: -f2- | tr -d ' \r')"
    [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] \
      || fail "${repo}:${tag} returned 200 without a valid Docker-Content-Digest"
    echo "$digest"
    ;;
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
