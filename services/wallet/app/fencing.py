"""Durable Wallet-side fencing for stale Coordinator terms (LM-039)."""

from uuid import UUID

from sqlalchemy import Connection, func, select, text
from sqlalchemy.dialects.postgresql import insert as postgresql_insert

from services.wallet.app.db.schema import fencing_rejections, participant_fencing_state


def accept_fencing_token(
    connection: Connection,
    token: int,
    transaction_id: UUID,
    command_type: str,
    coordination_mode: str,
    *,
    acquire_lock: bool = True,
) -> tuple[bool, int]:
    """Advance the high-water mark or record a durable stale-token rejection."""
    if acquire_lock:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended('ledgermesh:fencing', 0))")
        )
    advanced = connection.execute(
        postgresql_insert(participant_fencing_state)
        .values(singleton_id=1, highest_token=token)
        .on_conflict_do_update(
            index_elements=[participant_fencing_state.c.singleton_id],
            set_={
                "highest_token": token,
                "updated_at": func.now(),
            },
            where=participant_fencing_state.c.highest_token <= token,
        )
        .returning(participant_fencing_state.c.highest_token)
    ).scalar_one_or_none()
    if advanced is not None:
        return True, int(advanced)

    highest = int(
        connection.execute(
            select(participant_fencing_state.c.highest_token).where(
                participant_fencing_state.c.singleton_id == 1
            )
        ).scalar_one()
    )
    connection.execute(
        fencing_rejections.insert().values(
            transaction_id=transaction_id,
            supplied_token=token,
            highest_token=highest,
            command_type=command_type,
            coordination_mode=coordination_mode,
        )
    )
    return False, highest
