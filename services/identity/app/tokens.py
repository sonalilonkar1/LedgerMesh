"""Random login tokens and session identifiers, and how they are stored (LM-052).

Both are 256-bit values from ``secrets.token_urlsafe(32)`` (43 URL-safe
characters). Only their SHA-256 hex digest is written to the database, so a
leaked table cannot be turned into a working link or cookie. A plain hash
(rather than a slow KDF) is enough because the input is random, not chosen by a
person, and cannot be guessed.
"""

import hashlib
import secrets

TOKEN_BYTES = 32


def new_secret() -> str:
    return secrets.token_urlsafe(TOKEN_BYTES)


def hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode("ascii", errors="replace")).hexdigest()
