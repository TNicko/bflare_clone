"""Aggregated forward edges between Telegram communities.

Each row is a directed edge ``from_external_id -> to_external_id`` with a running
``forward_count`` (messages in ``from`` that forward content from ``to``).

- **forward_from** for peer X: ``WHERE from_external_id = X``
- **forward_to** for peer X: ``WHERE to_external_id = X``
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Sequence

from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict

from db.schemas.general.primitives import TelegramPeerTypeSchema
from telegram.utils import channel_id_from_signed_peer_id

_PEER_TYPES = frozenset({"channel", "megagroup", "chat"})


def _fwd_peer_signed_id_expr(field: str) -> str:
    """SQL: resolve ``fwd_from->'<field>'`` (a TL Peer) to a signed external_id, else NULL."""
    return f"""
CASE
  WHEN fwd_from->'{field}'->>'tl' = 'PeerChannel'
       AND (fwd_from->'{field}'->>'channel_id') ~ '^[0-9]+$'
    THEN (
      -(1000000000000::bigint + (fwd_from->'{field}'->>'channel_id')::bigint)
    )::text
  WHEN fwd_from->'{field}'->>'tl' = 'PeerChat'
       AND (fwd_from->'{field}'->>'chat_id') ~ '^[0-9]+$'
    THEN (
      CASE
        WHEN (fwd_from->'{field}'->>'chat_id')::bigint > 0
          THEN (-(fwd_from->'{field}'->>'chat_id')::bigint)::text
        ELSE (fwd_from->'{field}'->>'chat_id')
      END
    )
  ELSE NULL
END
"""


#: The original author of a forwarded message (survives re-forwards).
_FWD_FROM_ID_PEER_EXPR = _fwd_peer_signed_id_expr("from_id")
#: The peer a forward was immediately taken from (changes on each re-forward).
_FWD_SAVED_FROM_PEER_EXPR = _fwd_peer_signed_id_expr("saved_from_peer")
# Resolve signed peer id from fwd_from JSON (from_id, else saved_from_peer).
_FWD_TARGET_PEER_EXPR = f"COALESCE({_FWD_FROM_ID_PEER_EXPR}, {_FWD_SAVED_FROM_PEER_EXPR})"

AGGREGATE_FORWARD_SOURCES_SQL = f"""
WITH fwd AS (
  SELECT
    {_FWD_TARGET_PEER_EXPR} AS source_peer_id,
    NULLIF(BTRIM(fwd_from->>'from_name'), '') AS from_name
  FROM telegram_messages
  WHERE source_external_id = %s
    AND fwd_from IS NOT NULL
)
SELECT
  source_peer_id AS external_id,
  COUNT(*)::int AS forward_count,
  MAX(from_name) AS from_name
FROM fwd
WHERE source_peer_id IS NOT NULL
  AND source_peer_id <> %s
GROUP BY source_peer_id
ORDER BY forward_count DESC
"""

_FWD_MSGS_FOR_BACKFILL_CTE = f"""
fwd_msgs AS (
  SELECT
    m.source_external_id,
    {_FWD_TARGET_PEER_EXPR} AS target_peer_id
  FROM telegram_messages m
  WHERE m.fwd_from IS NOT NULL
    AND (%s::text IS NULL OR m.source_external_id = %s)
)
"""

_PENDING_OUTBOUND_EDGE_PEERS_CTE = f"""
{_FWD_MSGS_FOR_BACKFILL_CTE},
pending AS (
  SELECT DISTINCT f.source_external_id
  FROM fwd_msgs f
  WHERE f.target_peer_id IS NOT NULL
    AND f.target_peer_id <> f.source_external_id
    AND NOT EXISTS (
      SELECT 1
      FROM telegram_forward_edges e
      WHERE e.from_external_id = f.source_external_id
    )
)
"""

PEERS_PENDING_OUTBOUND_EDGE_BACKFILL_COUNT_SQL = f"""
WITH {_PENDING_OUTBOUND_EDGE_PEERS_CTE}
SELECT COUNT(*)::int
FROM pending
"""

PEERS_PENDING_OUTBOUND_EDGE_BACKFILL_LIST_SQL = f"""
WITH {_PENDING_OUTBOUND_EDGE_PEERS_CTE}
SELECT source_external_id
FROM pending
ORDER BY source_external_id
OFFSET %s
LIMIT %s
"""

PEERS_PENDING_OUTBOUND_EDGE_BACKFILL_ALL_IDS_SQL = f"""
WITH {_PENDING_OUTBOUND_EDGE_PEERS_CTE}
SELECT source_external_id
FROM pending
ORDER BY source_external_id
"""

RESOLVE_PEER_TYPES_FOR_IDS_SQL = """
SELECT external_id, peer_type
FROM (
  SELECT external_id, 'megagroup'::text AS peer_type
  FROM telegram_megagroups
  WHERE external_id = ANY(%s)
  UNION ALL
  SELECT external_id, 'chat'::text AS peer_type
  FROM telegram_chats
  WHERE external_id = ANY(%s)
  UNION ALL
  SELECT external_id, 'channel'::text AS peer_type
  FROM telegram_channels
  WHERE external_id = ANY(%s)
) resolved
"""

UPSERT_FORWARD_EDGE_SQL = """
INSERT INTO telegram_forward_edges (
  from_external_id,
  from_peer_type,
  to_external_id,
  to_peer_type,
  forward_count,
  last_from_name,
  first_seen_at,
  last_seen_at,
  created_at,
  updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, now(), now(), now(), now())
ON CONFLICT (from_external_id, to_external_id) DO UPDATE SET
  forward_count = telegram_forward_edges.forward_count + EXCLUDED.forward_count,
  last_from_name = COALESCE(EXCLUDED.last_from_name, telegram_forward_edges.last_from_name),
  to_peer_type = EXCLUDED.to_peer_type,
  last_seen_at = now(),
  updated_at = now()
"""

DELETE_OUTBOUND_EDGES_SQL = """
DELETE FROM telegram_forward_edges
WHERE from_external_id = %s
"""

INSERT_FORWARD_EDGE_EXACT_SQL = """
INSERT INTO telegram_forward_edges (
  from_external_id,
  from_peer_type,
  to_external_id,
  to_peer_type,
  forward_count,
  last_from_name,
  first_seen_at,
  last_seen_at,
  created_at,
  updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, now(), now(), now(), now())
ON CONFLICT (from_external_id, to_external_id) DO UPDATE SET
  forward_count = EXCLUDED.forward_count,
  last_from_name = COALESCE(EXCLUDED.last_from_name, telegram_forward_edges.last_from_name),
  to_peer_type = EXCLUDED.to_peer_type,
  last_seen_at = now(),
  updated_at = now()
"""

OUTBOUND_EDGES_FOR_PEERS_SQL = """
SELECT
  from_external_id,
  to_external_id,
  to_peer_type,
  forward_count,
  last_from_name
FROM telegram_forward_edges
WHERE from_external_id = ANY(%s)
ORDER BY from_external_id, forward_count DESC
"""

INBOUND_EDGES_FOR_PEER_SQL = """
SELECT
  from_external_id,
  from_peer_type,
  forward_count,
  last_seen_at
FROM telegram_forward_edges
WHERE to_external_id = %s
ORDER BY forward_count DESC
LIMIT %s
"""

OUTBOUND_EDGES_FOR_PEER_SQL = """
SELECT
  to_external_id,
  to_peer_type,
  forward_count,
  last_from_name
FROM telegram_forward_edges
WHERE from_external_id = %s
ORDER BY forward_count DESC
LIMIT %s
"""

#: Outbound + inbound neighbors for one peer, top-N per direction, plus totals.
PEER_FORWARD_NEIGHBORS_SQL = """
WITH edges AS (
  SELECT
    'outbound'::text AS direction,
    to_external_id AS other_external_id,
    to_peer_type AS other_peer_type,
    forward_count,
    last_from_name
  FROM telegram_forward_edges
  WHERE from_external_id = %s
  UNION ALL
  SELECT
    'inbound'::text,
    from_external_id,
    from_peer_type,
    forward_count,
    last_from_name
  FROM telegram_forward_edges
  WHERE to_external_id = %s
),
ranked AS (
  SELECT
    direction,
    other_external_id,
    other_peer_type,
    forward_count,
    last_from_name,
    ROW_NUMBER() OVER (PARTITION BY direction ORDER BY forward_count DESC) AS rn,
    COUNT(*) OVER (PARTITION BY direction)::int AS direction_total
  FROM edges
)
SELECT
  direction,
  other_external_id,
  other_peer_type,
  forward_count,
  last_from_name,
  direction_total
FROM ranked
WHERE rn <= %s
"""

TOP_OUTBOUND_SOURCE_PEERS_SQL = """
SELECT
  from_external_id,
  from_peer_type,
  COUNT(*)::int AS result_count
FROM telegram_forward_edges
GROUP BY from_external_id, from_peer_type
HAVING COUNT(*) >= %s
ORDER BY COUNT(*) DESC, from_external_id
LIMIT %s
"""

PEER_HAS_FORWARD_COVERAGE_SQL = """
SELECT (
  EXISTS (
    SELECT 1
    FROM telegram_forward_edges
    WHERE from_external_id = %s
  )
  OR EXISTS (
    SELECT 1
    FROM telegram_message_fetch_coverage
    WHERE peer_external_id = %s
      AND has_forward_sources = true
  )
) AS covered
"""

REBUILD_FRONTIER_FROM_EDGES_SQL = """
SELECT DISTINCT e.to_external_id, e.to_peer_type
FROM telegram_forward_edges e
WHERE NOT EXISTS (
  SELECT 1
  FROM telegram_message_fetch_coverage c
  WHERE c.peer_external_id = e.to_external_id
    AND c.has_forward_sources = true
)
"""


class ForwardEdgeRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    external_id: str
    peer_type: TelegramPeerTypeSchema
    count: int
    from_name: str | None = None


@dataclass(frozen=True, slots=True)
class PeerForwardNeighbors:
    """Top outbound (sources) and inbound (amplifiers) edges for one peer."""

    outbound: list[ForwardEdgeRef]
    outbound_total: int
    inbound: list[ForwardEdgeRef]
    inbound_total: int


@dataclass(frozen=True, slots=True)
class ForwardEdgeDelta:
    to_external_id: str
    to_peer_type: TelegramPeerTypeSchema
    delta: int
    last_from_name: str | None = None


def _external_id_from_fwd_from_id(from_id: object) -> str | None:
    if not isinstance(from_id, dict):
        return None
    tl = from_id.get("tl")
    if tl == "PeerChannel":
        raw_cid = from_id.get("channel_id")
        if raw_cid is None:
            return None
        try:
            channel_id = int(raw_cid)
        except (TypeError, ValueError):
            return None
        return str(-(1_000_000_000_000 + channel_id))
    if tl == "PeerChat":
        raw_cid = from_id.get("chat_id")
        if raw_cid is None:
            return None
        try:
            chat_id = int(raw_cid)
        except (TypeError, ValueError):
            return None
        return str(-chat_id if chat_id > 0 else chat_id)
    return None


def _external_id_from_fwd_from(fwd: object) -> str | None:
    if not isinstance(fwd, dict):
        return None
    for key in ("from_id", "saved_from_peer"):
        external_id = _external_id_from_fwd_from_id(fwd.get(key))
        if external_id:
            return external_id
    return None


def forward_edge_deltas_from_messages(
    messages: Sequence[Any],
    *,
    source_external_id: str,
) -> list[ForwardEdgeDelta]:
    """Aggregate forward targets in one upsert batch for ``increment_forward_edges``."""
    source_id = str(source_external_id).strip()
    tallies: dict[str, tuple[int, str | None]] = {}
    for msg in messages:
        fwd = getattr(msg, "fwd_from", None)
        if fwd is None and isinstance(msg, dict):
            fwd = msg.get("fwd_from")
        if not isinstance(fwd, dict):
            continue
        target_id = _external_id_from_fwd_from(fwd)
        if not target_id or target_id == source_id:
            continue
        raw_name = fwd.get("from_name")
        from_name = str(raw_name).strip() if raw_name else None
        prev_count, prev_name = tallies.get(target_id, (0, from_name))
        tallies[target_id] = (prev_count + 1, prev_name or from_name)
    return [
        ForwardEdgeDelta(
            to_external_id=target_id,
            to_peer_type=infer_peer_type(target_id),
            delta=count,
            last_from_name=from_name,
        )
        for target_id, (count, from_name) in tallies.items()
        if count > 0
    ]


def infer_peer_type(external_id: str) -> TelegramPeerTypeSchema:
    try:
        signed = int(external_id)
    except (TypeError, ValueError):
        return "channel"
    if channel_id_from_signed_peer_id(signed) is not None:
        return "channel"
    if signed < 0:
        return "chat"
    return "channel"


def _as_peer_type(raw: object) -> TelegramPeerTypeSchema:
    if raw in _PEER_TYPES:
        return raw  # type: ignore[return-value]
    return "channel"


async def increment_forward_edges(
    conn: Any,
    *,
    from_external_id: str,
    from_peer_type: TelegramPeerTypeSchema,
    updates: Sequence[ForwardEdgeDelta],
) -> int:
    """Add ``delta`` to edges from ``from_external_id``; returns rows touched."""
    if not updates:
        return 0
    from_id = str(from_external_id).strip()
    from_type = _as_peer_type(from_peer_type)
    params = [
        (
            from_id,
            from_type,
            str(item.to_external_id).strip(),
            _as_peer_type(item.to_peer_type),
            max(0, int(item.delta)),
            item.last_from_name,
        )
        for item in updates
        if item.to_external_id and str(item.to_external_id).strip() != from_id and item.delta > 0
    ]
    if not params:
        return 0
    async with conn.cursor() as cur:
        await cur.executemany(UPSERT_FORWARD_EDGE_SQL, params)
    return len(params)


async def resolve_peer_types_for_ids(
    conn: Any,
    external_ids: Sequence[str],
) -> dict[str, TelegramPeerTypeSchema]:
    """Best-effort peer_type lookup for a bounded id batch."""
    ids = [str(item).strip() for item in external_ids if str(item).strip()]
    if not ids:
        return {}
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(RESOLVE_PEER_TYPES_FOR_IDS_SQL, (ids, ids, ids))
        rows = await cur.fetchall()
    out: dict[str, TelegramPeerTypeSchema] = {}
    for row in rows:
        external_id, peer_type = row["external_id"], row["peer_type"]
        eid = str(external_id).strip()
        if eid:
            out[eid] = _as_peer_type(peer_type)
    return out


async def backfill_outbound_edges_for_peer(
    conn: Any,
    *,
    from_external_id: str,
    from_peer_type: TelegramPeerTypeSchema,
) -> int:
    """One-shot backfill: aggregate all ``fwd_from`` for a peer and increment edges."""
    from_id = str(from_external_id).strip()
    from_type = _as_peer_type(from_peer_type)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(AGGREGATE_FORWARD_SOURCES_SQL, (from_id, from_id))
        rows = await cur.fetchall()
    if not rows:
        return 0
    deltas = [
        ForwardEdgeDelta(
            to_external_id=str(row["external_id"]),
            to_peer_type=infer_peer_type(str(row["external_id"])),
            delta=int(row["forward_count"] or 0),
            last_from_name=str(row["from_name"]) if row["from_name"] else None,
        )
        for row in rows
        if row["external_id"] and int(row["forward_count"] or 0) > 0
    ]
    return await increment_forward_edges(
        conn,
        from_external_id=from_id,
        from_peer_type=from_type,
        updates=deltas,
    )


async def replace_outbound_edges_for_peer(
    conn: Any,
    *,
    from_external_id: str,
    from_peer_type: TelegramPeerTypeSchema,
) -> int:
    """Recompute outbound edges for one peer from ``telegram_messages.fwd_from``."""
    from_id = str(from_external_id).strip()
    from_type = _as_peer_type(from_peer_type)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(AGGREGATE_FORWARD_SOURCES_SQL, (from_id, from_id))
        rows = await cur.fetchall()
        await cur.execute(DELETE_OUTBOUND_EDGES_SQL, (from_id,))
        if rows:
            insert_params = [
                (
                    from_id,
                    from_type,
                    str(row["external_id"]),
                    infer_peer_type(str(row["external_id"])),
                    int(row["forward_count"] or 0),
                    str(row["from_name"]) if row["from_name"] else None,
                )
                for row in rows
                if row["external_id"]
            ]
            if insert_params:
                await cur.executemany(INSERT_FORWARD_EDGE_EXACT_SQL, insert_params)
    return len(rows)


async def fetch_outbound_refs_for_peers(
    conn: Any,
    peer_ids: Sequence[str],
) -> dict[str, list[ForwardEdgeRef]]:
    """Return outbound forward targets grouped by source peer id."""
    ids = [str(p).strip() for p in peer_ids if str(p).strip()]
    if not ids:
        return {}
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(OUTBOUND_EDGES_FOR_PEERS_SQL, (ids,))
        rows = await cur.fetchall()
    out: dict[str, list[ForwardEdgeRef]] = {pid: [] for pid in ids}
    for row in rows:
        from_id = str(row["from_external_id"]).strip()
        to_id = str(row["to_external_id"]).strip()
        if not from_id or not to_id:
            continue
        out.setdefault(from_id, []).append(
            ForwardEdgeRef(
                external_id=to_id,
                peer_type=_as_peer_type(row.get("to_peer_type")),
                count=int(row.get("forward_count") or 0),
                from_name=str(row["last_from_name"]).strip() if row.get("last_from_name") else None,
            )
        )
    return out


async def list_outbound_edges(
    conn: Any,
    *,
    from_external_id: str,
    limit: int = 100,
) -> list[ForwardEdgeRef]:
    limit = max(1, min(int(limit), 1000))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(OUTBOUND_EDGES_FOR_PEER_SQL, (str(from_external_id).strip(), limit))
        rows = await cur.fetchall()
    return [
        ForwardEdgeRef(
            external_id=str(row["to_external_id"]),
            peer_type=_as_peer_type(row.get("to_peer_type")),
            count=int(row.get("forward_count") or 0),
            from_name=str(row["last_from_name"]).strip() if row.get("last_from_name") else None,
        )
        for row in rows
    ]


async def list_inbound_edges(
    conn: Any,
    *,
    to_external_id: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Communities that forward content from ``to_external_id``."""
    limit = max(1, min(int(limit), 1000))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(INBOUND_EDGES_FOR_PEER_SQL, (str(to_external_id).strip(), limit))
        rows = await cur.fetchall()
    return [
        {
            "external_id": str(row["from_external_id"]),
            "peer_type": _as_peer_type(row.get("from_peer_type")),
            "forward_count": int(row.get("forward_count") or 0),
            "last_seen_at": row.get("last_seen_at"),
        }
        for row in rows
    ]


async def list_peer_forward_neighbors(
    conn: Any,
    *,
    peer_external_id: str,
    limit: int = 100,
) -> PeerForwardNeighbors:
    """Outbound sources and inbound amplifiers for one peer in a single query."""
    limit = max(1, min(int(limit), 1000))
    eid = str(peer_external_id).strip()
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(PEER_FORWARD_NEIGHBORS_SQL, (eid, eid, limit))
        rows = await cur.fetchall()

    outbound: list[ForwardEdgeRef] = []
    inbound: list[ForwardEdgeRef] = []
    outbound_total = 0
    inbound_total = 0
    for row in rows:
        ref = ForwardEdgeRef(
            external_id=str(row["other_external_id"]),
            peer_type=_as_peer_type(row.get("other_peer_type")),
            count=int(row.get("forward_count") or 0),
            from_name=str(row["last_from_name"]).strip() if row.get("last_from_name") else None,
        )
        total = int(row.get("direction_total") or 0)
        if row.get("direction") == "inbound":
            inbound.append(ref)
            inbound_total = total
        else:
            outbound.append(ref)
            outbound_total = total
    return PeerForwardNeighbors(
        outbound=outbound,
        outbound_total=outbound_total,
        inbound=inbound,
        inbound_total=inbound_total,
    )


async def fetch_top_outbound_source_peers(
    conn: Any,
    *,
    min_result_count: int,
    limit: int,
) -> list[dict[str, Any]]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(TOP_OUTBOUND_SOURCE_PEERS_SQL, (max(0, min_result_count), max(1, limit)))
        return await cur.fetchall()


async def peer_has_forward_coverage(conn: Any, external_id: str) -> bool:
    eid = str(external_id).strip()
    if not eid:
        return False
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(PEER_HAS_FORWARD_COVERAGE_SQL, (eid, eid))
        row = await cur.fetchone()
    return bool(row and row["covered"])


async def rebuild_frontier_from_edges(conn: Any) -> dict[str, str]:
    """Peers seen as forward targets but not yet marked ``has_forward_sources``."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(REBUILD_FRONTIER_FROM_EDGES_SQL)
        rows = await cur.fetchall()
    out: dict[str, str] = {}
    for row in rows:
        if not row["to_external_id"]:
            continue
        eid = str(row["to_external_id"]).strip()
        if eid:
            out[eid] = _as_peer_type(row.get("to_peer_type"))
    return out


def forward_edge_refs_as_json(refs: Sequence[ForwardEdgeRef]) -> str:
    payload = [
        {
            "external_id": ref.external_id,
            "count": ref.count,
            "peer_type": ref.peer_type,
            **({"from_name": ref.from_name} if ref.from_name else {}),
        }
        for ref in refs
    ]
    return json.dumps(payload)
