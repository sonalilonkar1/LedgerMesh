"""Store the trace context a redemption was created in (LM-033).

The Coordinator API writes a Saga redemption and its worker starts it later,
in another process. Keeping the request's W3C trace context on the row lets
the worker continue the same trace, so one redemption is one trace. Nullable:
untraced requests and rows created before this migration have none.

Revision ID: 0002_coordinator_trace_context
Revises: 0001_coordinator_initial
Create Date: 2026-11-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_coordinator_trace_context"
down_revision: str | None = "0001_coordinator_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "redemptions",
        sa.Column("trace_context", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("redemptions", "trace_context")
