from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from db.crud.telegram.sessions import (
    claim_inactive_session,
    expire_stale_session_leases,
    find_joinable_session,
    harvester_is_active,
    remove_lease_holder,
    upsert_lease_holder,
)
from db.schemas.telegram.harvester_session import SessionTypeSchema

lg = logging.getLogger(__name__)


class NoAvailableSessionError(RuntimeError):
    def __init__(self, *, harvester_id: UUID, session_type: SessionTypeSchema) -> None:
        super().__init__(f"No available {session_type!r} session for harvester id={harvester_id}")
        self.harvester_id = harvester_id
        self.session_type = session_type


async def claim_or_join_session(
    conn: Any,
    *,
    harvester_id: UUID,
    session_type: SessionTypeSchema,
    location_id: str,
    server_id: str,
) -> UUID:
    if not await harvester_is_active(conn, harvester_id):
        raise NoAvailableSessionError(harvester_id=harvester_id, session_type=session_type)

    # Make expired leases reclaimable even when the background sweeper is down.
    expired = await expire_stale_session_leases(conn)
    if expired:
        lg.info(
            "Lease expiry before claim harvester_id=%s stale_holders=%s sessions_released=%s",
            harvester_id,
            expired.stale_holders_removed,
            expired.sessions_released,
        )

    session_id = await find_joinable_session(
        conn,
        harvester_id=harvester_id,
        session_type=session_type,
        location_id=location_id,
        server_id=server_id,
    )
    if session_id is None:
        session_id = await claim_inactive_session(
            conn,
            harvester_id=harvester_id,
            session_type=session_type,
            location_id=location_id,
        )
    if session_id is None:
        raise NoAvailableSessionError(harvester_id=harvester_id, session_type=session_type)

    held = await upsert_lease_holder(
        conn,
        session_id=session_id,
        server_id=server_id,
        location_id=location_id,
    )

    if not held:
        await conn.commit()
        raise NoAvailableSessionError(harvester_id=harvester_id, session_type=session_type)

    await conn.commit()
    lg.info(
        "Session lease acquired session_id=%s harvester_id=%s type=%s location=%s server=%s",
        session_id,
        harvester_id,
        session_type,
        location_id,
        server_id,
    )
    return session_id


async def release_session_lease(
    conn: Any,
    *,
    session_id: UUID,
    server_id: str,
    joined_before: datetime | None = None,
) -> None:
    released = await remove_lease_holder(
        conn,
        session_id=session_id,
        server_id=server_id,
        joined_before=joined_before,
    )
    await conn.commit()
    if released:
        lg.info("Session released session_id=%s server=%s", session_id, server_id)


async def heartbeat_session_lease(
    conn: Any,
    *,
    session_id: UUID,
    harvester_id: UUID,
    location_id: str,
    server_id: str,
) -> bool:
    """Extend lease; return False if harvester deactivated or lease ownership lost."""
    if not await harvester_is_active(conn, harvester_id):
        await release_session_lease(conn, session_id=session_id, server_id=server_id)
        return False
    held = await upsert_lease_holder(
        conn,
        session_id=session_id,
        server_id=server_id,
        location_id=location_id,
    )
    if not held:
        # Lease expired or taken by another location — drop our holder row if any.
        await remove_lease_holder(conn, session_id=session_id, server_id=server_id)
        await conn.commit()
        lg.warning(
            "Session lease lost session_id=%s location=%s server=%s",
            session_id,
            location_id,
            server_id,
        )
        return False
    await conn.commit()
    return True
