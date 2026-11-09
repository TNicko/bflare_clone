"""Shared coverage preview + gap detection for forward-source seed graphs.

A peer is covered (settled) when any of:
- outbound forward edges or ``has_forward_sources`` on fetch coverage
- canonical peer row has unavailable_at set (terminal resolve miss materialized)

Expandable peers are those at depth < max_depth.
"""

from __future__ import annotations

from collections import deque
from typing import Any

from psycopg.rows import dict_row

from db.crud.telegram.unresolved_peer import fetch_unavailable_peer_ids
from db.crud.telegram.forward_edges import (
    ForwardEdgeRef,
    fetch_outbound_refs_for_peers,
    peer_has_forward_coverage,
)

HARVESTERS_WITH_HASH_SQL = """
SELECT DISTINCT h.id, h.external_id::text AS external_id
FROM telegram_harvesters h
JOIN telegram_harvester_sessions s
  ON s.harvester_id = h.id
 AND s.session_type = 'system'
 AND s.auth_state = 'healthy'
JOIN telegram_harvester_peer_roles r
  ON r.harvester_id = h.id
 AND r.external_id = %s
 AND r.access_hash IS NOT NULL
 AND r.access_error_code IS NULL
WHERE h.is_active = true
ORDER BY h.external_id
"""

ELIGIBLE_SYSTEM_HARVESTERS_SQL = """
SELECT DISTINCT h.id, h.external_id::text AS external_id
FROM telegram_harvesters h
JOIN telegram_harvester_sessions s
  ON s.harvester_id = h.id
 AND s.session_type = 'system'
 AND s.auth_state = 'healthy'
WHERE h.is_active = true
ORDER BY h.external_id
"""

ACTIVE_SYSTEM_LEASE_HARVESTER_IDS_SQL = """
SELECT DISTINCT s.harvester_id
FROM telegram_harvester_sessions s
WHERE s.session_type = 'system'
  AND s.status = 'active'
  AND (s.leased_until IS NULL OR s.leased_until > NOW())
"""

PEER_HARVESTER_OPTIONS_SQL = """
SELECT DISTINCT
  h.id,
  h.external_id::text AS external_id,
  h.label,
  h.username,
  h.first_name,
  h.last_name,
  EXISTS (
    SELECT 1
    FROM telegram_harvester_peer_roles r
    WHERE r.harvester_id = h.id
      AND r.external_id = %s
      AND r.access_hash IS NOT NULL
      AND r.access_error_code IS NULL
  ) AS has_access_hash,
  EXISTS (
    SELECT 1
    FROM telegram_message_fetch_coverage fc
    WHERE fc.harvester_id = h.id
      AND fc.peer_external_id = %s
  ) AS has_fetch_coverage,
  (
    SELECT COUNT(*)::int
    FROM harvest_jobs j
    WHERE j.status = ANY(%s)
      AND (%s::text IS NULL OR j.task_type = %s::text)
      AND (
        j.params->>'harvester_id' = h.id::text
        OR j.params->>'harvester_external_id' = h.external_id::text
      )
  ) AS active_job_count
FROM telegram_harvesters h
JOIN telegram_harvester_sessions s
  ON s.harvester_id = h.id
 AND s.session_type = 'system'
 AND s.auth_state = 'healthy'
WHERE h.is_active = true
ORDER BY has_fetch_coverage DESC, has_access_hash DESC, active_job_count ASC, h.external_id
"""

# Backward-compatible alias (forward_graph job counts only).
HARVEST_HARVESTER_OPTIONS_SQL = PEER_HARVESTER_OPTIONS_SQL

ELIGIBLE_SYSTEM_HARVESTER_BY_ID_SQL = """
SELECT DISTINCT h.id, h.external_id::text AS external_id
FROM telegram_harvesters h
JOIN telegram_harvester_sessions s
  ON s.harvester_id = h.id
 AND s.session_type = 'system'
 AND s.auth_state = 'healthy'
WHERE h.is_active = true
  AND h.id = %s
"""


def _as_peer_type(raw: object) -> str:
    if raw in ("channel", "megagroup", "chat"):
        return str(raw)
    return "channel"


def _apply_from_name(
    nodes: dict[str, dict[str, Any]],
    external_id: str,
    from_name: str | None,
) -> None:
    if not from_name:
        return
    node = nodes.get(external_id)
    if node is None:
        return
    if not (node.get("label") or node.get("username")):
        node["label"] = from_name


async def _fetch_outbound_for_peers(conn: Any, peer_ids: list[str]) -> dict[str, list[ForwardEdgeRef]]:
    if not peer_ids:
        return {}
    return await fetch_outbound_refs_for_peers(conn, peer_ids)


def _refs_from_edges(refs: list[ForwardEdgeRef]) -> list[dict[str, Any]]:
    return [
        {
            "external_id": ref.external_id,
            "peer_type": _as_peer_type(ref.peer_type),
            "count": ref.count,
            "from_name": ref.from_name,
        }
        for ref in refs
    ]


async def preview_forward_graph_coverage(
    conn: Any,
    *,
    seed_external_id: str,
    seed_peer_type: str,
    seed_label: str | None,
    seed_username: str | None,
    max_depth: int,
    max_nodes: int = 50_000,
    max_edges: int = 150_000,
    uncovered_sample_limit: int = 12,
    include_graph: bool = False,
    seed_unavailable: bool = False,
) -> dict[str, Any]:
    """BFS coverage report for expandable peers (depth < max_depth).

    When ``include_graph`` is true, also accumulates the same node/edge topology
    that ``build_forward_graph_from_seed`` would return (before unavailable hydration).
    """
    max_depth = max(1, min(int(max_depth), 10))
    max_nodes = max(1, min(max_nodes, 100_000))
    max_edges = max(1, min(max_edges, 300_000))

    seed_id = str(seed_external_id).strip()
    seed_type = _as_peer_type(seed_peer_type)

    by_depth: dict[int, dict[str, Any]] = {
        d: {"depth": d, "expandable": 0, "covered": 0, "uncovered": 0, "uncovered_sample": []} for d in range(max_depth)
    }
    gap_peers: list[dict[str, Any]] = []
    truncated = False
    edge_count = 0

    nodes: dict[str, dict[str, Any]] | None = None
    edges: list[dict[str, Any]] | None = None
    edge_keys: set[tuple[str, str]] | None = None
    degrees: dict[str, int] | None = None
    peers_expanded = 0
    if include_graph:
        nodes = {
            seed_id: {
                "id": seed_id,
                "external_id": seed_id,
                "peer_type": seed_type,
                "label": seed_label,
                "username": seed_username,
                "degree": 0,
                "result_count": 0,
                "is_seed": True,
                "unavailable": seed_unavailable,
                "expanded": False,
            }
        }
        edges = []
        edge_keys = set()
        degrees = {}

    queue: deque[tuple[str, str, int]] = deque([(seed_id, seed_type, 0)])
    queued: set[str] = {seed_id}
    visited: set[str] = set()
    known_types: dict[str, str] = {seed_id: seed_type}

    def ensure_graph_node(peer_id: str, peer_type: str, *, result_count: int = 0) -> bool:
        assert nodes is not None
        if peer_id in nodes:
            return True
        if len(nodes) >= max_nodes:
            return False
        nodes[peer_id] = {
            "id": peer_id,
            "external_id": peer_id,
            "peer_type": peer_type,
            "label": seed_label if peer_id == seed_id else None,
            "username": seed_username if peer_id == seed_id else None,
            "degree": 0,
            "result_count": result_count,
            "is_seed": False,
            "unavailable": False,
            "expanded": False,
        }
        return True

    while queue:
        batch: list[tuple[str, str, int]] = []
        while queue and len(batch) < 200:
            batch.append(queue.popleft())

        expand_ids = [peer_id for peer_id, _ptype, depth in batch if depth < max_depth and peer_id not in visited]
        outbound_by_peer = await _fetch_outbound_for_peers(conn, expand_ids)
        unavailable_ids = await fetch_unavailable_peer_ids(conn, expand_ids)

        for peer_id, peer_type, depth in batch:
            if peer_id in visited:
                continue
            visited.add(peer_id)
            if depth >= max_depth:
                continue
            if len(visited) > max_nodes:
                truncated = True
                break

            bucket = by_depth[depth]
            bucket["expandable"] += 1
            if include_graph and not ensure_graph_node(peer_id, peer_type):
                truncated = True
                break

            settled_dead_end = peer_id in unavailable_ids
            covered = settled_dead_end or await peer_has_forward_coverage(conn, peer_id)
            if not covered:
                bucket["uncovered"] += 1
                sample = {
                    "external_id": peer_id,
                    "peer_type": peer_type,
                    "label": seed_label if peer_id == seed_id else None,
                    "username": seed_username if peer_id == seed_id else None,
                }
                if len(bucket["uncovered_sample"]) < uncovered_sample_limit:
                    bucket["uncovered_sample"].append(sample)
                gap_peers.append(
                    {
                        "external_id": peer_id,
                        "peer_type": peer_type,
                        "depth": depth,
                    }
                )
                continue

            bucket["covered"] += 1
            if settled_dead_end:
                # Unavailable: no forward children to expand.
                continue

            edge_refs = outbound_by_peer.get(peer_id) or []
            if not edge_refs:
                continue

            refs = _refs_from_edges(edge_refs)
            result_count = len(refs)
            peer_type_eff = peer_type

            if include_graph:
                assert nodes is not None
                peers_expanded += 1
                if peer_id in nodes:
                    nodes[peer_id]["result_count"] = result_count
                    nodes[peer_id]["expanded"] = True
                    if not nodes[peer_id].get("peer_type"):
                        nodes[peer_id]["peer_type"] = peer_type_eff
                elif not ensure_graph_node(peer_id, peer_type_eff, result_count=result_count):
                    truncated = True
                    break
                else:
                    nodes[peer_id]["result_count"] = result_count
                    nodes[peer_id]["expanded"] = True

            for ref in refs:
                if edge_count >= max_edges:
                    truncated = True
                    break
                edge_count += 1
                target_id = ref["external_id"]
                target_type = _as_peer_type(ref["peer_type"])
                known_types[target_id] = target_type

                if include_graph:
                    assert nodes is not None and edges is not None and edge_keys is not None and degrees is not None
                    if target_id == peer_id:
                        continue
                    if not ensure_graph_node(target_id, target_type):
                        truncated = True
                        break
                    _apply_from_name(nodes, target_id, ref.get("from_name"))
                    key = (peer_id, target_id)
                    if key not in edge_keys:
                        edge_keys.add(key)
                        edges.append(
                            {
                                "source": peer_id,
                                "target": target_id,
                                "count": ref.get("count") or 0,
                            }
                        )
                        degrees[peer_id] = degrees.get(peer_id, 0) + 1
                        degrees[target_id] = degrees.get(target_id, 0) + 1

                next_depth = depth + 1
                if next_depth < max_depth and target_id not in queued:
                    queued.add(target_id)
                    queue.append((target_id, target_type, next_depth))
            if truncated:
                break
        if truncated:
            break

    seed_unavailable = seed_id in await fetch_unavailable_peer_ids(conn, [seed_id])
    seed_covered = await peer_has_forward_coverage(conn, seed_id) or seed_unavailable
    expandable = sum(b["expandable"] for b in by_depth.values())
    covered = sum(b["covered"] for b in by_depth.values())
    uncovered = sum(b["uncovered"] for b in by_depth.values())

    return {
        "seed": {
            "external_id": seed_id,
            "peer_type": seed_type,
            "label": seed_label,
            "username": seed_username,
        },
        "max_depth": max_depth,
        "seed_covered": seed_covered,
        "by_depth": [by_depth[d] for d in range(max_depth)],
        "totals": {
            "expandable": expandable,
            "covered": covered,
            "uncovered": uncovered,
        },
        "truncated": truncated,
        "estimated_work": {"peers_to_scrape": uncovered},
        "gap_peers": gap_peers,
        "graph": (
            {
                "nodes": nodes,
                "edges": edges or [],
                "degrees": degrees or {},
                "peers_expanded": peers_expanded,
            }
            if include_graph and nodes is not None
            else None
        ),
    }


async def pick_job_harvester(
    conn: Any,
    *,
    seed_external_id: str,
) -> tuple[Any, str] | None:
    """Prefer harvester with access_hash for seed; else first eligible system harvester."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(HARVESTERS_WITH_HASH_SQL, (seed_external_id,))
        rows = await cur.fetchall()
        if not rows:
            await cur.execute(ELIGIBLE_SYSTEM_HARVESTERS_SQL)
            rows = await cur.fetchall()
    if not rows:
        return None
    row = rows[0]
    return row["id"], str(row["external_id"])


async def get_eligible_system_harvester(
    conn: Any,
    *,
    harvester_id: Any,
) -> tuple[Any, str] | None:
    """Return (id, external_id) if the harvester is active with a healthy system session."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(ELIGIBLE_SYSTEM_HARVESTER_BY_ID_SQL, (harvester_id,))
        row = await cur.fetchone()
    if row is None:
        return None
    return row["id"], str(row["external_id"])


def _peer_harvester_option_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "id": row["id"],
                "external_id": str(row["external_id"]),
                "label": row.get("label"),
                "username": row.get("username"),
                "first_name": row.get("first_name"),
                "last_name": row.get("last_name"),
                "has_access_hash": bool(row.get("has_access_hash")),
                "has_fetch_coverage": bool(row.get("has_fetch_coverage")),
                "active_job_count": int(row.get("active_job_count") or 0),
            }
        )
    return out


async def list_peer_harvester_options(
    conn: Any,
    *,
    peer_external_id: str,
    task_type: str | None = None,
) -> list[dict[str, Any]]:
    """Active system harvesters with peer access-hash and fetch-coverage flags."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            PEER_HARVESTER_OPTIONS_SQL,
            (
                peer_external_id,
                peer_external_id,
                ["queued", "running"],
                task_type,
                task_type,
            ),
        )
        rows = await cur.fetchall()
    return _peer_harvester_option_rows(rows)


async def list_harvest_harvester_options(
    conn: Any,
    *,
    seed_external_id: str,
) -> list[dict[str, Any]]:
    """Active system harvesters with seed access-hash flag and forward_graph job counts."""
    return await list_peer_harvester_options(
        conn,
        peer_external_id=seed_external_id,
        task_type="forward_graph",
    )


async def list_harvester_ids_with_active_system_session(conn: Any) -> set[Any]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(ACTIVE_SYSTEM_LEASE_HARVESTER_IDS_SQL)
        rows = await cur.fetchall()
    return {row["harvester_id"] for row in rows}
