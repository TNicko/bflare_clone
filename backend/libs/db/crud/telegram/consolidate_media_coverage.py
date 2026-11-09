"""One-off consolidation: merge per-harvester media coverage/cursors into peer-scoped rows."""

from __future__ import annotations

from collections import defaultdict
from typing import Any
from uuid import UUID

from sqlalchemy import text

from utils.coverage_intervals import MediaCoverageInterval, merge_media_intervals

SELECT_ALL_MEDIA_COVERAGE_SQL = """
SELECT harvester_id, peer_external_id, peer_type, covered_after, covered_before,
       count_image, count_gif, count_video, count_audio, count_document, count_webpage,
       phashes_in_milvus, videos_excluded, large_excluded, seals_gap
FROM telegram_media_download_coverage
ORDER BY peer_external_id, peer_type, covered_after NULLS FIRST, covered_before
"""

TRUNCATE_MEDIA_COVERAGE_SQL = "TRUNCATE telegram_media_download_coverage"

INSERT_MEDIA_COVERAGE_WITH_HARVESTER_SQL = """
INSERT INTO telegram_media_download_coverage (
  harvester_id, peer_external_id, peer_type, covered_after, covered_before,
  count_image, count_gif, count_video, count_audio, count_document, count_webpage,
  phashes_in_milvus, videos_excluded, large_excluded, seals_gap, updated_at
)
VALUES (
  :harvester_id, :peer_external_id, :peer_type, :covered_after, :covered_before,
  :count_image, :count_gif, :count_video, :count_audio, :count_document, :count_webpage,
  :phashes_in_milvus, :videos_excluded, :large_excluded, :seals_gap, now()
)
"""

SELECT_ALL_MEDIA_CURSORS_SQL = """
SELECT harvester_id, peer_external_id, peer_type, window_after, window_before, last_message_id, is_complete
FROM telegram_media_download_cursors
"""

TRUNCATE_MEDIA_CURSORS_SQL = "TRUNCATE telegram_media_download_cursors"

INSERT_MEDIA_CURSOR_WITH_HARVESTER_SQL = """
INSERT INTO telegram_media_download_cursors (
  harvester_id, peer_external_id, peer_type, window_after, window_before,
  last_message_id, is_complete, updated_at
)
VALUES (
  :harvester_id, :peer_external_id, :peer_type, :window_after, :window_before,
  :last_message_id, :is_complete, now()
)
"""

_INSERT_BATCH_SIZE = 500


def set_migration_timeouts(conn: Any) -> None:
    """Allow long-running data rewrite inside Alembic's transaction."""
    conn.execute(text("SET LOCAL statement_timeout = 0"))
    conn.execute(text("SET LOCAL lock_timeout = '30min'"))


def _as_uuid(value: Any) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


def _media_interval_from_row(row: Any) -> MediaCoverageInterval:
    return MediaCoverageInterval(
        covered_after=row[3],
        covered_before=row[4],
        count_image=int(row[5] or 0),
        count_gif=int(row[6] or 0),
        count_video=int(row[7] or 0),
        count_audio=int(row[8] or 0),
        count_document=int(row[9] or 0),
        count_webpage=int(row[10] or 0),
        phashes_in_milvus=bool(row[11]),
        videos_excluded=bool(row[12]),
        large_excluded=bool(row[13]),
        seals_gap=bool(row[14]) if len(row) > 14 else True,
    )


def _executemany_batched(conn: Any, sql: str, params: list[dict[str, Any]]) -> None:
    stmt = text(sql)
    for offset in range(0, len(params), _INSERT_BATCH_SIZE):
        conn.execute(stmt, params[offset : offset + _INSERT_BATCH_SIZE])


def consolidate_media_coverage_rows(conn: Any) -> int:
    """Merge all harvester rows per peer (run before dropping ``harvester_id``)."""
    rows = conn.execute(text(SELECT_ALL_MEDIA_COVERAGE_SQL)).fetchall()
    if not rows:
        return 0

    by_peer: dict[tuple[str, str], tuple[list[MediaCoverageInterval], UUID]] = {}
    for row in rows:
        key = (str(row[1]), str(row[2]))
        harvester_id = _as_uuid(row[0])
        if key not in by_peer:
            by_peer[key] = ([], harvester_id)
        by_peer[key][0].append(_media_interval_from_row(row))

    insert_params: list[dict[str, Any]] = []
    for (peer_external_id, peer_type), (intervals, harvester_id) in by_peer.items():
        for island in merge_media_intervals(intervals):
            insert_params.append(
                {
                    "harvester_id": harvester_id,
                    "peer_external_id": peer_external_id,
                    "peer_type": peer_type,
                    "covered_after": island.covered_after,
                    "covered_before": island.covered_before,
                    "count_image": island.count_image,
                    "count_gif": island.count_gif,
                    "count_video": island.count_video,
                    "count_audio": island.count_audio,
                    "count_document": island.count_document,
                    "count_webpage": island.count_webpage,
                    "phashes_in_milvus": island.phashes_in_milvus,
                    "videos_excluded": island.videos_excluded,
                    "large_excluded": island.large_excluded,
                    "seals_gap": island.seals_gap,
                }
            )

    conn.execute(text(TRUNCATE_MEDIA_COVERAGE_SQL))
    if insert_params:
        _executemany_batched(conn, INSERT_MEDIA_COVERAGE_WITH_HARVESTER_SQL, insert_params)
    return len(by_peer)


def consolidate_media_download_cursors(conn: Any) -> int:
    """Merge cursor rows per peer/window (run before dropping ``harvester_id``)."""
    rows = conn.execute(text(SELECT_ALL_MEDIA_CURSORS_SQL)).fetchall()
    if not rows:
        return 0

    by_peer: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for row in rows:
        by_peer[(str(row[1]), str(row[2]))].append(row)

    insert_params: list[dict[str, Any]] = []
    for (peer_external_id, peer_type), peer_rows in by_peer.items():
        by_window: dict[tuple[Any, Any], list[Any]] = defaultdict(list)
        harvester_id = _as_uuid(peer_rows[0][0])
        for row in peer_rows:
            by_window[(row[3], row[4])].append(row)

        for (window_after, window_before), window_rows in by_window.items():
            insert_params.append(
                {
                    "harvester_id": harvester_id,
                    "peer_external_id": peer_external_id,
                    "peer_type": peer_type,
                    "window_after": window_after,
                    "window_before": window_before,
                    "last_message_id": max(int(r[5] or 0) for r in window_rows),
                    "is_complete": any(bool(r[6]) for r in window_rows),
                }
            )

    conn.execute(text(TRUNCATE_MEDIA_CURSORS_SQL))
    if insert_params:
        _executemany_batched(conn, INSERT_MEDIA_CURSOR_WITH_HARVESTER_SQL, insert_params)
    return len(by_peer)
