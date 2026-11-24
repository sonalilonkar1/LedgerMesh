"""Print the config digest of a local Docker image (LM-047).

    python3 infra/scripts/image_config_digest.py IMAGE

The config digest identifies an image's exact contents and settings, and it
is preserved when the image is pushed, so it can be compared with the image a
registry tag points to. It is read from the ``docker save`` archive, which
works whether Docker uses its classic or its containerd image store (in the
latter, ``docker image inspect .Id`` is a manifest digest, not the config).

Prints ``sha256:<hex>`` and exits 0, or explains the problem and exits 1.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tarfile

HEX = re.compile(r"^[0-9a-f]{64}$")


def config_digest_from_manifest(manifest: object) -> str:
    """Return the config digest named by a ``docker save`` manifest.json.

    Docker 25+ writes ``blobs/sha256/<hex>``; older versions write ``<hex>.json``.
    """
    if not isinstance(manifest, list) or len(manifest) != 1:
        raise ValueError("manifest.json must describe exactly one image")
    config = manifest[0].get("Config") if isinstance(manifest[0], dict) else None
    if not isinstance(config, str):
        raise ValueError("manifest.json has no Config entry")
    name = config.rsplit("/", 1)[-1].removesuffix(".json")
    if not HEX.match(name):
        raise ValueError(f"unexpected Config entry {config!r}")
    return f"sha256:{name}"


def image_config_digest(image: str) -> str:
    process = subprocess.Popen(["docker", "save", image], stdout=subprocess.PIPE)
    manifest_bytes = None
    try:
        with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
            for member in archive:
                if member.name.lstrip("./") == "manifest.json":
                    extracted = archive.extractfile(member)
                    if extracted is not None:
                        manifest_bytes = extracted.read()
    finally:
        returncode = process.wait()
    if returncode != 0:
        raise RuntimeError(f"docker save {image} exited {returncode}")
    if manifest_bytes is None:
        raise ValueError("docker save archive has no manifest.json")
    return config_digest_from_manifest(json.loads(manifest_bytes))


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: image_config_digest.py IMAGE", file=sys.stderr)
        return 1
    try:
        print(image_config_digest(argv[1]))
    except (OSError, RuntimeError, ValueError, tarfile.TarError) as error:
        print(f"image_config_digest: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
