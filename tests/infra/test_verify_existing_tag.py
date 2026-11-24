"""infra/scripts/verify_existing_tag.sh: an existing SHA tag must BE the candidate (LM-047).

When the SHA tag already exists, the publish job accepts it only if the
registry image's config digest equals the candidate that was just checked and
scanned. A different image, or any failure to compare, stops the job.
"""

import json

import pytest

from .conftest import digest, index_body, manifest_body, not_found_body

SCRIPT = "verify_existing_tag.sh"
REPO, TAG = "owner/ledgermesh-wallet", "abc123"
CANDIDATE = digest("c1")


def verify(registry, manifests, candidate=CANDIDATE, env=None):
    return registry(
        SCRIPT,
        REPO,
        TAG,
        candidate,
        env={"FAKE_MANIFESTS": json.dumps(manifests), **(env or {})},
    )


def test_existing_tag_matching_the_candidate_is_accepted(registry):
    result, _ = verify(registry, {TAG: {"body": manifest_body(CANDIDATE)}})

    assert result.returncode == 0
    assert "is the checked candidate" in result.stdout


def test_existing_index_whose_amd64_image_matches_is_accepted(registry):
    amd = digest("a2")
    manifests = {
        TAG: {"body": index_body([(amd, "linux", "amd64")])},
        amd: {"body": manifest_body(CANDIDATE)},
    }

    result, _ = verify(registry, manifests)

    assert result.returncode == 0


def test_existing_tag_holding_a_different_image_is_rejected(registry):
    result, _ = verify(registry, {TAG: {"body": manifest_body(digest("d9"))}})

    assert result.returncode == 1
    assert "is a different image" in result.stderr


@pytest.mark.parametrize(
    "manifests",
    [
        {TAG: {"status": 500}},
        {TAG: {"status": 401, "body": not_found_body("UNAUTHORIZED")}},
        # the tag vanished between the existence check and the comparison
        {TAG: {"status": 404, "body": not_found_body("MANIFEST_UNKNOWN")}},
        {TAG: {"body": "not json"}},
        {TAG: {"body": index_body([(digest("a1"), "linux", "arm64")])}},
    ],
)
def test_a_comparison_that_cannot_be_made_is_rejected(registry, manifests):
    result, _ = verify(registry, manifests)

    assert result.returncode == 1
    assert "could not read the config" in result.stderr


@pytest.mark.parametrize("candidate", ["", "sha256:short", "md5:" + "a" * 32])
def test_an_invalid_candidate_digest_is_rejected_before_any_request(registry, candidate):
    result, calls = verify(registry, {TAG: {"body": manifest_body(CANDIDATE)}}, candidate)

    assert result.returncode == 1
    assert calls == []
