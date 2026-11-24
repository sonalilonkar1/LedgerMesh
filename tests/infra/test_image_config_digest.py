"""infra/scripts/image_config_digest.py: the candidate's config digest (LM-047).

A fake ``docker`` writes a ``docker save`` archive to stdout, in both the
Docker 25+ OCI layout and the older layout.
"""

import io
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from .conftest import SCRIPTS, install

HEX = "c1" * 32

FAKE_DOCKER = f"""#!{sys.executable}
import os, sys
if sys.argv[1:2] != ["save"]:
    sys.exit(64)
archive = os.environ.get("FAKE_SAVE_ARCHIVE")
if archive:
    with open(archive, "rb") as handle:
        sys.stdout.buffer.write(handle.read())
sys.exit(int(os.environ.get("FAKE_SAVE_EXIT", "0")))
"""


def save_archive(path: Path, members: dict[str, bytes]) -> Path:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    path.write_bytes(buffer.getvalue())
    return path


def manifest(config: str) -> bytes:
    return json.dumps([{"Config": config, "RepoTags": ["x:candidate"], "Layers": []}]).encode()


@pytest.fixture
def config_digest(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    install(bin_dir / "docker", FAKE_DOCKER)

    def run(env: dict[str, str]):
        return subprocess.run(
            [sys.executable, str(SCRIPTS / "image_config_digest.py"), "x:candidate"],
            env={**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", **env},
            capture_output=True,
            text=True,
            check=False,
        )

    return run


@pytest.mark.parametrize("config_entry", [f"blobs/sha256/{HEX}", f"{HEX}.json"])
def test_reads_the_config_digest_from_either_save_layout(config_digest, tmp_path, config_entry):
    archive = save_archive(
        tmp_path / "image.tar",
        {"oci-layout": b"{}", "manifest.json": manifest(config_entry), "index.json": b"{}"},
    )

    result = config_digest({"FAKE_SAVE_ARCHIVE": str(archive)})

    assert result.returncode == 0
    assert result.stdout.strip() == f"sha256:{HEX}"


@pytest.mark.parametrize(
    "members",
    [
        {"index.json": b"{}"},
        {"manifest.json": b"not json"},
        {"manifest.json": b"[]"},
        {"manifest.json": manifest("blobs/sha256/short")},
        {"manifest.json": json.dumps([{"Layers": []}]).encode()},
    ],
)
def test_an_unusable_archive_fails(config_digest, tmp_path, members):
    archive = save_archive(tmp_path / "image.tar", members)

    result = config_digest({"FAKE_SAVE_ARCHIVE": str(archive)})

    assert result.returncode == 1
    assert result.stdout == ""


def test_a_failed_docker_save_fails_even_with_a_manifest(config_digest, tmp_path):
    archive = save_archive(tmp_path / "image.tar", {"manifest.json": manifest(f"{HEX}.json")})

    result = config_digest({"FAKE_SAVE_ARCHIVE": str(archive), "FAKE_SAVE_EXIT": "1"})

    assert result.returncode == 1
    assert result.stdout == ""


def test_no_output_from_docker_save_fails(config_digest):
    result = config_digest({"FAKE_SAVE_EXIT": "1"})

    assert result.returncode == 1
