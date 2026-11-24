"""infra/scripts/ghcr_digest.sh: tell "not published" apart from "lookup failed" (LM-047).

The publish and release jobs only push or create a tag when this script exits
3, the registry-confirmed "not found". A fake ``curl`` (see conftest.py) plays
the registry, so every response and a failed request are checked offline.
"""

import json

import pytest

from .conftest import digest, index_body, manifest_body, not_found_body

SCRIPT = "ghcr_digest.sh"
REPO, TAG = "owner/ledgermesh-wallet", "abc123"
MANIFEST = digest("ab")
NOT_FOUND = 3


def lookup(registry, env=None, *flags):
    return registry(SCRIPT, *flags, REPO, TAG, env=env)


def test_existing_tag_prints_its_digest(registry):
    result, calls = lookup(registry, {"FAKE_MANIFEST_STATUS": "200", "FAKE_DIGEST": MANIFEST})

    assert result.returncode == 0
    assert result.stdout.strip() == MANIFEST
    manifest = next(call for call in calls if call["kind"] == "manifest")
    assert "Authorization: Bearer registry-bearer" in manifest["args"]
    assert manifest["url"] == f"https://ghcr.io/v2/{REPO}/manifests/{TAG}"


@pytest.mark.parametrize("code", ["MANIFEST_UNKNOWN", "NAME_UNKNOWN"])
@pytest.mark.parametrize("flags", [(), ("--config",)])
def test_confirmed_not_found_exits_3(registry, code, flags):
    result, _ = lookup(
        registry,
        {"FAKE_MANIFEST_STATUS": "404", "FAKE_MANIFEST_BODY": not_found_body(code)},
        *flags,
    )

    assert result.returncode == NOT_FOUND
    assert result.stdout == ""


@pytest.mark.parametrize(
    ("status", "body"),
    [
        ("404", "<html>Not Found</html>"),
        ("404", not_found_body("DENIED")),
        ("401", not_found_body("UNAUTHORIZED")),
        ("403", not_found_body("DENIED")),
        ("429", ""),
        ("500", ""),
        ("503", ""),
    ],
)
def test_any_other_manifest_response_is_a_lookup_failure(registry, status, body):
    result, _ = lookup(registry, {"FAKE_MANIFEST_STATUS": status, "FAKE_MANIFEST_BODY": body})

    assert result.returncode == 1
    assert result.stdout == ""


@pytest.mark.parametrize("curl_exit", [3, 6, 7, 28, 35])
def test_a_failed_manifest_request_is_a_lookup_failure_not_not_found(registry, curl_exit):
    # curl's own exit code 3 must never leak out as "not found".
    result, _ = lookup(registry, {"FAKE_EXIT_ON": "manifest", "FAKE_EXIT_CODE": str(curl_exit)})

    assert result.returncode == 1


@pytest.mark.parametrize(
    "token_env",
    [
        {"FAKE_TOKEN_STATUS": "401"},
        {"FAKE_TOKEN_STATUS": "500"},
        {"FAKE_TOKEN_BODY": "{}"},
        {"FAKE_TOKEN_BODY": "not json"},
        {"FAKE_TOKEN_BODY": json.dumps({"token": ""})},
        {"FAKE_EXIT_ON": "token", "FAKE_EXIT_CODE": "7"},
    ],
)
def test_a_failed_token_exchange_is_a_lookup_failure(registry, token_env):
    result, calls = lookup(registry, token_env)

    assert result.returncode == 1
    assert all(call["kind"] == "token" for call in calls)


def test_ok_without_a_valid_digest_is_a_lookup_failure(registry):
    result, _ = lookup(registry, {"FAKE_MANIFEST_STATUS": "200", "FAKE_DIGEST": "sha256:short"})

    assert result.returncode == 1


def test_missing_credentials_fail_before_any_request(registry):
    result, calls = lookup(registry, {"GHCR_TOKEN": ""})

    assert result.returncode == 1
    assert calls == []


def test_wrong_arguments_fail(registry):
    result, calls = registry(SCRIPT, "only-one-argument")

    assert result.returncode == 1
    assert calls == []


# --config: the digest to compare with a locally built candidate


def test_config_of_a_single_image_manifest(registry):
    result, _ = lookup(
        registry, {"FAKE_MANIFEST_BODY": manifest_body(digest("c1")), "FAKE_DIGEST": MANIFEST}, "--config"
    )

    assert result.returncode == 0
    assert result.stdout.strip() == digest("c1")


def test_config_of_an_index_follows_the_linux_amd64_image(registry):
    arm, amd = digest("a1"), digest("a2")
    manifests = {
        TAG: {"body": index_body([(arm, "linux", "arm64"), (amd, "linux", "amd64")])},
        amd: {"body": manifest_body(digest("c2"))},
        arm: {"body": manifest_body(digest("c9"))},
    }

    result, calls = lookup(registry, {"FAKE_MANIFESTS": json.dumps(manifests)}, "--config")

    assert result.returncode == 0
    assert result.stdout.strip() == digest("c2")
    assert [call["url"].rsplit("/", 1)[1] for call in calls if call["kind"] == "manifest"] == [
        TAG,
        amd,
    ]


@pytest.mark.parametrize(
    "manifests",
    [
        # an index without a linux/amd64 image
        {TAG: {"body": index_body([(digest("a1"), "linux", "arm64")])}},
        # the platform image cannot be fetched
        {
            TAG: {"body": index_body([(digest("a2"), "linux", "amd64")])},
            digest("a2"): {"status": 500},
        },
        # a manifest with no config
        {TAG: {"body": json.dumps({"schemaVersion": 2, "layers": []})}},
        # a body that is not JSON
        {TAG: {"body": "<html>"}},
        # a config digest that is not sha256:<64 hex>
        {TAG: {"body": manifest_body("sha256:short")}},
    ],
)
def test_config_lookup_fails_closed_on_anything_unexpected(registry, manifests):
    result, _ = lookup(registry, {"FAKE_MANIFESTS": json.dumps(manifests)}, "--config")

    assert result.returncode == 1
    assert result.stdout == ""
