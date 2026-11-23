"""Repeatable fault triggers for the load harness (LM-035).

A fault is something we break on purpose while the load runs. Each one has a
trigger and a cleanup, both done with Docker Compose against the local stack.

    kill   docker compose kill <service>   -> docker compose start <service>
    pause  docker compose pause <service>  -> docker compose unpause <service>
    stop   docker compose stop <service>   -> docker compose start <service>

``kill`` is a crash, ``pause`` freezes the process so callers time out, and
``stop`` is a clean shutdown. A fault is written as ``kind:target@start+length``,
for example ``pause:wallet@10s+5s``: pause the Wallet 10 seconds into the load
and restore it 5 seconds later.

Run one by hand while the stack is up:

    uv run python -m tests.load.faults inject pause wallet
    uv run python -m tests.load.faults cleanup pause wallet
"""

from __future__ import annotations

import argparse
import json
import subprocess
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", str(REPO_ROOT / "infra" / "compose.yaml")]

# fault kind -> (docker compose action that causes it, action that undoes it)
KINDS = {
    "kill": ("kill", "start"),
    "pause": ("pause", "unpause"),
    "stop": ("stop", "start"),
}
HEALTH_PORTS = {"coordinator": 8000, "wallet": 8001, "catalog": 8002, "audit-ledger": 8003}
TARGETS = (*HEALTH_PORTS, "kafka")
RECOVERY_TIMEOUT_SECONDS = 60.0


class FaultError(ValueError):
    """Raised for a fault description that cannot be understood."""


def parse_seconds(text: str) -> float:
    """Turn ``10``, ``10s`` or ``2m`` into seconds."""
    text = text.strip()
    scale = 1.0
    if text.endswith("m"):
        scale, text = 60.0, text[:-1]
    elif text.endswith("s"):
        text = text[:-1]
    try:
        seconds = float(text) * scale
    except ValueError as error:
        raise FaultError(f"not a duration: {text!r}") from error
    if seconds < 0:
        raise FaultError("durations cannot be negative")
    return seconds


@dataclass(frozen=True)
class FaultSpec:
    kind: str
    target: str
    start: float  # seconds after the load starts
    length: float  # seconds the fault lasts before cleanup

    @classmethod
    def parse(cls, text: str) -> FaultSpec:
        try:
            what, when = text.split("@")
            kind, target = what.split(":")
            start, length = when.split("+")
        except ValueError as error:
            raise FaultError(f"expected kind:target@start+length, got {text!r}") from error
        if kind not in KINDS:
            raise FaultError(f"unknown fault {kind!r}; choose from {', '.join(KINDS)}")
        if target not in TARGETS:
            raise FaultError(f"unknown target {target!r}; choose from {', '.join(TARGETS)}")
        return cls(kind, target, parse_seconds(start), parse_seconds(length))

    def __str__(self) -> str:
        return f"{self.kind}:{self.target}@{self.start:g}s+{self.length:g}s"

    @property
    def end(self) -> float:
        return self.start + self.length


def validate_schedule(specs: list[FaultSpec]) -> None:
    """Reject two faults that would be active on the same service at once.

    Each fault undoes itself when it ends, so a second fault on the same service
    would be switched off early by the first one's cleanup.
    """
    by_target: dict[str, list[FaultSpec]] = {}
    for spec in specs:
        by_target.setdefault(spec.target, []).append(spec)
    for target, faults in by_target.items():
        ordered = sorted(faults, key=lambda spec: spec.start)
        for earlier, later in zip(ordered, ordered[1:], strict=False):
            if later.start < earlier.end:
                raise FaultError(
                    f"{earlier} and {later} overlap on {target}; "
                    "start the second fault after the first one ends"
                )


def compose(action: str, target: str) -> None:
    subprocess.run(
        [*COMPOSE, action, target], check=True, capture_output=True, text=True, timeout=120
    )


def inject(kind: str, target: str) -> None:
    compose(KINDS[kind][0], target)


def cleanup(kind: str, target: str) -> None:
    compose(KINDS[kind][1], target)


def seconds_until_healthy(target: str) -> float | None:
    """Seconds until the service answers /health again, or None if it never does."""
    port = HEALTH_PORTS.get(target)
    if port is None:  # Kafka has no HTTP health endpoint
        return None
    begin = time.monotonic()
    while time.monotonic() - begin < RECOVERY_TIMEOUT_SECONDS:
        try:
            with urllib.request.urlopen(f"http://localhost:{port}/health", timeout=2):
                return round(time.monotonic() - begin, 3)
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(0.25)
    return None


class FaultRunner:
    """Runs scheduled faults on background threads and logs every step."""

    def __init__(self, specs: list[FaultSpec], log_path: Path, load_started_at: float) -> None:
        self.specs = specs
        self.log_path = log_path
        self.t0 = load_started_at  # epoch seconds when the load began
        self.problems: list[str] = []  # anything that makes the experiment invalid
        self._abort = threading.Event()
        self._write_lock = threading.Lock()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        for spec in self.specs:
            thread = threading.Thread(target=self._run, args=(spec,), daemon=True)
            thread.start()
            self._threads.append(thread)

    def finish(self) -> None:
        """Stop waiting, undo any active fault, and wait for the threads."""
        self._abort.set()
        for thread in self._threads:
            thread.join()

    def _record(self, event: str, spec: FaultSpec, **details: object) -> None:
        now = time.time()
        line = {
            "time": datetime.fromtimestamp(now, UTC).isoformat(timespec="milliseconds"),
            "offset_seconds": round(now - self.t0, 3),
            "event": event,
            "fault": str(spec),
            **details,
        }
        with self._write_lock, self.log_path.open("a") as log:
            log.write(json.dumps(line) + "\n")

    def _problem(self, message: str) -> None:
        with self._write_lock:
            self.problems.append(message)

    def _run(self, spec: FaultSpec) -> None:
        delay = self.t0 + spec.start - time.time()
        if self._abort.wait(max(delay, 0)):
            self._record("skipped", spec, reason="run ended before the fault was due")
            self._problem(f"{spec} never ran: the load ended before it was due")
            return

        # "issued" is when the command was sent; the event itself is logged once
        # it returns, so a slow command (a graceful stop) is not mistaken for delay.
        issued = self._offset()
        inject_ok = False
        try:
            inject(spec.kind, spec.target)
            inject_ok = True
            self._record("inject", spec, ok=True, issued_offset_seconds=issued)
        except (subprocess.SubprocessError, OSError) as error:
            # The command may have taken effect before it failed or timed out, so
            # the service is treated as possibly disrupted and is restored below.
            self._record(
                "inject", spec, ok=False, issued_offset_seconds=issued, error=_describe(error)
            )
            self._problem(f"{spec} could not be injected: {_describe(error)}")

        try:
            if inject_ok and self._abort.wait(spec.length):
                self._problem(f"{spec} was cut short because the load ended")
        finally:
            self._clean(spec, after_failed_inject=not inject_ok)

    def _clean(self, spec: FaultSpec, *, after_failed_inject: bool) -> None:
        issued = self._offset()
        try:
            cleanup(spec.kind, spec.target)
        except (subprocess.SubprocessError, OSError) as error:
            self._record(
                "cleanup",
                spec,
                ok=False,
                issued_offset_seconds=issued,
                after_failed_inject=after_failed_inject,
                error=_describe(error),
            )
            # After a failed inject the service was probably untouched, so "not
            # paused" style errors are expected there and are not a problem.
            if not after_failed_inject:
                self._problem(f"{spec} could not be cleaned up: {_describe(error)}")
            return
        # The service counts as recovered once /health answers again, which can
        # be seconds after the cleanup command returns.
        healthy = seconds_until_healthy(spec.target)
        recovered = healthy is not None or spec.target not in HEALTH_PORTS
        self._record(
            "cleanup",
            spec,
            ok=True,
            issued_offset_seconds=issued,
            after_failed_inject=after_failed_inject,
            healthy_after_seconds=healthy,
            recovered=recovered,
        )
        if not recovered:
            self._problem(
                f"{spec.target} did not answer /health within {RECOVERY_TIMEOUT_SECONDS:g}s "
                "after cleanup"
            )

    def _offset(self) -> float:
        return round(time.time() - self.t0, 3)


def _describe(error: Exception) -> str:
    stderr = getattr(error, "stderr", None)
    return (stderr or str(error)).strip()[:300]


def main() -> None:
    parser = argparse.ArgumentParser(description="Cause or undo one fault by hand.")
    parser.add_argument("action", choices=["inject", "cleanup"])
    parser.add_argument("kind", choices=list(KINDS))
    parser.add_argument("target", choices=list(TARGETS))
    args = parser.parse_args()
    (inject if args.action == "inject" else cleanup)(args.kind, args.target)
    print(f"{args.action} {args.kind} {args.target}: done")


if __name__ == "__main__":
    main()
