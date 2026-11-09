from collections.abc import Sequence
from typing import TypeAlias, cast
from uuid import UUID
from datetime import datetime
from psycopg import AsyncConnection
from pydantic import BaseModel, ConfigDict
from db.schemas.telegram.harvester import TelegramPeerTypeSchema
from .queries import (
    DELETE_OTHER_INCOMPLETE_CURSORS_FOR_AFTER_SQL,
    LIST_PEER_ASSIGNMENTS_LIMITED_SQL,
    LIST_PEER_ASSIGNMENTS_PENDING_SQL,
    LIST_PEER_ASSIGNMENTS_SQL,
    LIST_PEER_ASSIGNMENTS_FOR_CATEGORIES_ASSIGNED_SQL,
    LIST_PEER_ASSIGNMENTS_FOR_CATEGORIES_SQL,
    LIST_PEER_ASSIGNMENTS_FOR_COMMUNITY_ID_ASSIGNED_SQL,
    LIST_PEER_ASSIGNMENTS_FOR_COMMUNITY_ID_SQL,
    LIST_PEER_ASSIGNMENTS_FOR_EXTERNAL_ID_ASSIGNED_SQL,
    LIST_PEER_ASSIGNMENTS_FOR_EXTERNAL_ID_SQL,
    LIST_PEER_ASSIGNMENTS_FOR_NAME_ASSIGNED_SQL,
    LIST_PEER_ASSIGNMENTS_FOR_NAME_SQL,
    LIST_UNASSIGNED_PEER_SELECTIONS_SQL,
    SELECT_CURSOR_SQL,
    SELECT_INCOMPLETE_CURSOR_FOR_AFTER_SQL,
    SELECT_PEER_USERNAME_SQL,
    UPSERT_CURSOR_SQL,
)


PeerSelectionSqlRow: TypeAlias = tuple[str, TelegramPeerTypeSchema, int | None, UUID | None, str | None]


class PeerSelectionRow(BaseModel):
    """Peer matched by a fetch selection query (harvester role may be missing)."""

    model_config = ConfigDict(frozen=True)

    peer_external_id: str
    peer_type: TelegramPeerTypeSchema
    access_hash: int | None
    harvester_id: UUID | None
    harvester_external_id: str | None


class PeerAssignmentRow(BaseModel):
    """Peer ready to fetch (always has an active harvester assignment)."""

    model_config = ConfigDict(frozen=True)

    peer_external_id: str
    peer_type: TelegramPeerTypeSchema
    access_hash: int | None
    harvester_id: UUID
    harvester_external_id: str


def peer_selection_skip_reason(row: PeerSelectionRow) -> str | None:
    """Return why a peer needs harvester resolution, or ``None`` if fetchable as-is."""
    if row.harvester_id is None or row.harvester_external_id is None:
        return "no telegram_harvester_peer_roles row (parent role, active harvester)"
    if row.peer_type in ("channel", "megagroup") and row.access_hash is None:
        return "missing telegram_harvester_peer_roles.access_hash"
    return None


def to_peer_assignment(row: PeerSelectionRow) -> PeerAssignmentRow:
    if row.harvester_id is None or row.harvester_external_id is None:
        raise ValueError("peer has no harvester assignment")
    return PeerAssignmentRow(
        peer_external_id=row.peer_external_id,
        peer_type=row.peer_type,
        access_hash=row.access_hash,
        harvester_id=row.harvester_id,
        harvester_external_id=row.harvester_external_id,
    )


class CursorState(BaseModel):
    model_config = ConfigDict(frozen=True)

    next_offset_id: int | None
    is_complete: bool


def _rows_to_selections(rows: list[tuple[object, ...]]) -> list[PeerSelectionRow]:
    typed_rows = cast(list[PeerSelectionSqlRow], rows)

    out: list[PeerSelectionRow] = []
    for peer_external_id, peer_type, access_hash, harvester_id, harvester_external_id in typed_rows:
        out.append(
            PeerSelectionRow(
                peer_external_id=str(peer_external_id),
                peer_type=peer_type,
                access_hash=access_hash,
                harvester_id=harvester_id,
                harvester_external_id=str(harvester_external_id) if harvester_external_id is not None else None,
            )
        )
    return out


def _require_workers(worker_harvester_ids: Sequence[UUID]) -> list[UUID]:
    worker_ids = list(worker_harvester_ids)
    if not worker_ids:
        raise ValueError("worker_harvester_ids must not be empty")
    return worker_ids


async def list_peer_assignments(
    conn: AsyncConnection,
    *,
    peer_types: list[TelegramPeerTypeSchema],
    assignee_harvester_id: UUID,
    worker_harvester_ids: Sequence[UUID],
    limit: int | None = None,
) -> list[PeerSelectionRow]:
    worker_ids = _require_workers(worker_harvester_ids)
    async with conn.cursor() as cur:
        if limit is not None and limit > 0:
            await cur.execute(
                LIST_PEER_ASSIGNMENTS_LIMITED_SQL,
                (peer_types, worker_ids, assignee_harvester_id, limit),
            )
        else:
            await cur.execute(
                LIST_PEER_ASSIGNMENTS_SQL,
                (peer_types, worker_ids, assignee_harvester_id),
            )
        rows = await cur.fetchall()
    return _rows_to_selections(rows)


async def list_peer_assignments_pending(
    conn: AsyncConnection,
    *,
    peer_types: list[TelegramPeerTypeSchema],
    window_after: datetime | None,
    window_before: datetime | None,
    catchup_slack_minutes: int,
    assignee_harvester_id: UUID,
    worker_harvester_ids: Sequence[UUID],
    limit: int,
) -> list[PeerSelectionRow]:
    """Peers with harvester roles that still need fetch work for the configured window.

    Capacity-aware: among ``worker_harvester_ids``, each peer is assigned to the
    least-loaded role-holder; results are filtered to ``assignee_harvester_id``.
    """
    worker_ids = _require_workers(worker_harvester_ids)
    async with conn.cursor() as cur:
        await cur.execute(
            LIST_PEER_ASSIGNMENTS_PENDING_SQL,
            (
                window_after,
                window_before,
                catchup_slack_minutes,
                peer_types,
                worker_ids,
                assignee_harvester_id,
                limit,
            ),
        )
        rows = await cur.fetchall()
    return _rows_to_selections(rows)


async def fetch_peer_username(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
) -> str | None:
    async with conn.cursor() as cur:
        await cur.execute(
            SELECT_PEER_USERNAME_SQL,
            (peer_external_id, peer_external_id),
        )
        row = await cur.fetchone()
    if not row or row[0] is None:
        return None
    username = str(row[0]).strip()
    return username or None


async def list_peer_assignments_for_external_id(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    assignee_harvester_id: UUID | None = None,
    worker_harvester_ids: Sequence[UUID] | None = None,
) -> list[PeerSelectionRow]:
    """List assignment(s) for one peer.

    With ``assignee_harvester_id`` + ``worker_harvester_ids``, returns the
    capacity-aware winner filtered to that assignee (or empty). Without them,
    returns the legacy LEFT JOIN (all roles + possible unassigned row) used by
    harvest_peer / fetch_dialogues / bootstrap resolve.
    """
    async with conn.cursor() as cur:
        if assignee_harvester_id is not None and worker_harvester_ids is not None:
            worker_ids = _require_workers(worker_harvester_ids)
            await cur.execute(
                LIST_PEER_ASSIGNMENTS_FOR_EXTERNAL_ID_ASSIGNED_SQL,
                (peer_external_id, worker_ids, assignee_harvester_id),
            )
        else:
            await cur.execute(LIST_PEER_ASSIGNMENTS_FOR_EXTERNAL_ID_SQL, (peer_external_id,))
        rows = await cur.fetchall()
    return _rows_to_selections(rows)


async def list_peer_assignments_for_name_query(
    conn: AsyncConnection,
    *,
    peer_name_query: str,
    assignee_harvester_id: UUID | None = None,
    worker_harvester_ids: Sequence[UUID] | None = None,
) -> list[PeerSelectionRow]:
    q = f"%{peer_name_query}%"
    async with conn.cursor() as cur:
        if assignee_harvester_id is not None and worker_harvester_ids is not None:
            worker_ids = _require_workers(worker_harvester_ids)
            await cur.execute(
                LIST_PEER_ASSIGNMENTS_FOR_NAME_ASSIGNED_SQL,
                (q, worker_ids, assignee_harvester_id),
            )
        else:
            await cur.execute(LIST_PEER_ASSIGNMENTS_FOR_NAME_SQL, (q,))
        rows = await cur.fetchall()
    return _rows_to_selections(rows)


async def list_peer_assignments_for_community_id(
    conn: AsyncConnection,
    *,
    community_id: UUID,
    assignee_harvester_id: UUID | None = None,
    worker_harvester_ids: Sequence[UUID] | None = None,
) -> list[PeerSelectionRow]:
    async with conn.cursor() as cur:
        if assignee_harvester_id is not None and worker_harvester_ids is not None:
            worker_ids = _require_workers(worker_harvester_ids)
            await cur.execute(
                LIST_PEER_ASSIGNMENTS_FOR_COMMUNITY_ID_ASSIGNED_SQL,
                (community_id, worker_ids, assignee_harvester_id),
            )
        else:
            await cur.execute(
                LIST_PEER_ASSIGNMENTS_FOR_COMMUNITY_ID_SQL,
                (community_id,),
            )
        rows = await cur.fetchall()
    return _rows_to_selections(rows)


async def list_peer_assignments_for_categories(
    conn: AsyncConnection,
    *,
    category_slugs: list[str],
    assignee_harvester_id: UUID | None = None,
    worker_harvester_ids: Sequence[UUID] | None = None,
) -> list[PeerSelectionRow]:
    async with conn.cursor() as cur:
        if assignee_harvester_id is not None and worker_harvester_ids is not None:
            worker_ids = _require_workers(worker_harvester_ids)
            await cur.execute(
                LIST_PEER_ASSIGNMENTS_FOR_CATEGORIES_ASSIGNED_SQL,
                (category_slugs, worker_ids, assignee_harvester_id),
            )
        else:
            await cur.execute(LIST_PEER_ASSIGNMENTS_FOR_CATEGORIES_SQL, (category_slugs,))
        rows = await cur.fetchall()
    return _rows_to_selections(rows)


async def list_unassigned_peer_selections(
    conn: AsyncConnection,
    *,
    peer_types: list[TelegramPeerTypeSchema],
    limit: int,
) -> list[PeerSelectionRow]:
    """Peers with no usable parent role (bootstrap resolve for all-communities mode)."""
    async with conn.cursor() as cur:
        await cur.execute(
            LIST_UNASSIGNED_PEER_SELECTIONS_SQL,
            (peer_types, limit),
        )
        rows = await cur.fetchall()
    return _rows_to_selections(rows)


async def fetch_cursor(
    conn: AsyncConnection,
    *,
    harvester_id: UUID,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    window_before: datetime,
    window_after: datetime | None,
    resume_open_ended: bool = False,
) -> CursorState:
    """Load paging cursor for a gap.

    When ``resume_open_ended`` is true (config has no fixed ``TELEGRAM_FETCH_BEFORE``),
    each run uses a fresh ``now`` as ``window_before``. Fall back to any incomplete
    cursor for the same ``window_after`` so we do not restart from the newest
    messages.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            SELECT_CURSOR_SQL,
            (harvester_id, peer_external_id, peer_type, window_before, window_after),
        )
        row = await cur.fetchone()
        if row:
            return CursorState(next_offset_id=row[0], is_complete=bool(row[1]))
        if not resume_open_ended:
            return CursorState(next_offset_id=None, is_complete=False)
        await cur.execute(
            SELECT_INCOMPLETE_CURSOR_FOR_AFTER_SQL,
            (harvester_id, peer_external_id, peer_type, window_after),
        )
        row = await cur.fetchone()
    if not row:
        return CursorState(next_offset_id=None, is_complete=False)
    return CursorState(next_offset_id=row[0], is_complete=bool(row[1]))


async def upsert_cursor(
    conn: AsyncConnection,
    *,
    harvester_id: UUID,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    window_before: datetime,
    window_after: datetime | None,
    next_offset_id: int | None,
    is_complete: bool,
    last_message_id_seen: int | None,
    last_message_date_seen: datetime | None,
    replace_open_ended_siblings: bool = False,
) -> None:
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                UPSERT_CURSOR_SQL,
                (
                    harvester_id,
                    peer_external_id,
                    peer_type,
                    window_before,
                    window_after,
                    next_offset_id,
                    is_complete,
                    last_message_id_seen,
                    last_message_date_seen,
                ),
            )
            if replace_open_ended_siblings:
                # Drop prior incomplete cursors keyed by older ``now`` values.
                await cur.execute(
                    DELETE_OTHER_INCOMPLETE_CURSORS_FOR_AFTER_SQL,
                    (
                        harvester_id,
                        peer_external_id,
                        peer_type,
                        window_after,
                        window_before,
                    ),
                )
        await conn.commit()
    except Exception:
        await conn.rollback()
        raise
