"""Helpers for judging what a failed request tells us (LM-035)."""

from __future__ import annotations


def connection_never_established(error: BaseException | None) -> bool:
    """True when a request failed while connecting, so it cannot have been delivered.

    A refused or timed-out connection means nothing reached the Coordinator. A
    connection that dropped after the request was sent proves nothing, so it does
    not count. The class names are checked because requests wraps the urllib3 error.
    """
    if error is None:
        return False
    text = repr(error)
    return "NewConnectionError" in text or "ConnectTimeout" in text
