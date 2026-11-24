"""The root-message causation rule shared by every ordering wrapper (LM-026).

ADR-001: ``causation_id`` is null only for the root message of a transaction.
Every other message names the message that caused it. Which messages a
consumer can legitimately receive as a root depends on its role, so each
wrapper states its policy explicitly with ``root_step``:

- ``root_step=1``: the consumer can receive the transaction's first command
  (a Saga participant receiving the step-1 command). Only a message exactly
  at that step may omit its cause. Every other step must name one: a later
  step answers an earlier message, and a step before the root cannot be a
  root at all, so an uncaused message there is malformed too.
- ``root_step=None``: the consumer never receives a root message, so every
  message must name its cause. The Coordinator's results always answer a
  command it sent, and an aggregate's events are always emitted in response
  to a command.

A message that breaks the rule is malformed, not late or duplicated, so it is
rejected with a ``PermanentError`` and dead-lettered by ``InboxConsumer``. It
is never applied and never recorded in the inbox as ignored.
"""

from __future__ import annotations

from .consumer import PermanentError
from .envelope import EventEnvelope


class MissingCausationError(PermanentError):
    """A message that is not a root message for this consumer has no causation_id."""

    def __init__(self, envelope: EventEnvelope, root_step: int | None) -> None:
        if root_step is None:
            rule = "this consumer receives no root messages"
        else:
            rule = f"only a root message exactly at workflow_step {root_step} may omit it"
        super().__init__(
            f"{envelope.event_type} {envelope.event_id} (workflow_step "
            f"{envelope.workflow_step}) has no causation_id; {rule}"
        )
        self.root_step = root_step


def is_root(envelope: EventEnvelope, *, root_step: int | None) -> bool:
    """True only if ``envelope`` is exactly at this consumer's root step."""
    return root_step is not None and envelope.workflow_step == root_step


def require_causation(envelope: EventEnvelope, *, root_step: int | None) -> None:
    """Raise ``MissingCausationError`` unless ``envelope`` has a cause or is a root."""
    if envelope.causation_id is None and not is_root(envelope, root_step=root_step):
        raise MissingCausationError(envelope, root_step)
