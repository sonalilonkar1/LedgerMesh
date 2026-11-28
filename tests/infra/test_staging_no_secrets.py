"""No credentials are committed with the staging environment (LM-048).

Passwords exist only in Secret Manager and, on the VM, in the rendered
root-only env file. The repository may contain variable references and
obvious local placeholders, nothing else.
"""

import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
STAGING_FILES = sorted(
    path
    for path in [
        *(ROOT / "infra" / "staging").rglob("*"),
        ROOT / "docs" / "deployment" / "staging.md",
        ROOT / "docs" / "architecture" / "ADR-002-hosted-staging.md",
    ]
    if path.is_file()
)
# NAME=value (shell, env files) or NAME: value (YAML), for names that hold a credential.
ASSIGNMENT = re.compile(r"\b([A-Z_]*(?:PASSWORD|TOKEN|SECRET))(?:=|:\s+)([^\s#]+)")
ALLOWED_VALUE = re.compile(
    r"""^(
        \$\{?[A-Z_]+.*          # a variable reference
      | \$\(.*                  # a command substitution
      | "?\$[A-Za-z_].*         # a quoted reference
      | local-only-change-me-[a-z-]+   # the local example placeholders
      | <[^>]+>                 # a documented placeholder such as <value>
      | \[REDACTED\]
    )$""",
    re.VERBOSE,
)


def test_there_are_staging_files_to_scan():
    names = {path.name for path in STAGING_FILES}
    assert {"compose.yaml", "Caddyfile", ".env.local.example", "provision.sh"} <= names


@pytest.mark.parametrize("path", STAGING_FILES, ids=lambda path: str(path.relative_to(ROOT)))
def test_no_credential_values(path):
    text = path.read_text()
    assert "-----BEGIN" not in text
    for match in ASSIGNMENT.finditer(text):
        name, value = match.groups()
        value = value.rstrip("`,;)")
        if value.startswith(('"$', "'$")) or value in {"", '""', "''"}:
            continue
        assert ALLOWED_VALUE.match(value), f"{path.name}: {name} has a literal value"


def test_rendered_and_local_env_files_are_ignored_by_git():
    # `make staging-local-up` renders into artifacts/; the VM renders outside the checkout.
    for path in ("infra/staging/.env.local", "artifacts/staging/staging.env"):
        result = subprocess.run(["git", "check-ignore", "-q", path], cwd=ROOT, check=False)
        if result.returncode == 128:
            pytest.skip("not a git checkout")
        assert result.returncode == 0, f"{path} is not ignored by git"
    tracked = subprocess.run(
        ["git", "ls-files", "infra/staging"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    assert "infra/staging/.env.local" not in tracked


@pytest.mark.parametrize(
    "line",
    [
        "WALLET_DB_PASSWORD=9f86d081884c7d659a2feaa0c55ad015",  # gitleaks:allow
        "      POSTGRES_PASSWORD: hunter2-but-longer",
        "GHCR_TOKEN=ghp_exampleexampleexample",
    ],
)
def test_the_scan_catches_literal_credentials(line):
    name, value = ASSIGNMENT.search(line).groups()
    assert not ALLOWED_VALUE.match(value), name
