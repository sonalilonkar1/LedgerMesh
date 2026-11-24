#!/usr/bin/env bash
# Decide whether an already-published tag is the candidate that was just
# checked and scanned (LM-047).
#
#   GHCR_USER=... GHCR_TOKEN=... bash infra/scripts/verify_existing_tag.sh \
#     OWNER/IMAGE TAG CANDIDATE_CONFIG_DIGEST
#
# Compares the registry image's config digest (ghcr_digest.sh --config) with
# the candidate's (image_config_digest.py). The config digest survives a push
# unchanged, so equal digests mean the tag already holds this exact image.
#
#   0  the existing tag is this candidate; nothing needs to be published
#   1  it is a different image, or the comparison could not be made. SHA tags
#      are never replaced, so publishing stops.
set -uo pipefail

DIGEST_RE='^sha256:[0-9a-f]{64}$'

fail() {
  echo "verify_existing_tag: $*" >&2
  exit 1
}

[[ $# -eq 3 ]] || fail "usage: verify_existing_tag.sh OWNER/IMAGE TAG CANDIDATE_CONFIG_DIGEST"
repo="$1"
tag="$2"
candidate="$3"
[[ "$candidate" =~ $DIGEST_RE ]] || fail "candidate config digest '${candidate}' is not sha256:<64 hex>"

existing="$(bash "$(dirname "$0")/ghcr_digest.sh" --config "$repo" "$tag")" \
  || fail "could not read the config of ${repo}:${tag} to compare it with the candidate"

if [[ "$existing" != "$candidate" ]]; then
  fail "${repo}:${tag} is a different image (config ${existing}, candidate ${candidate})"
fi
echo "${repo}:${tag} is the checked candidate (config ${existing})"
