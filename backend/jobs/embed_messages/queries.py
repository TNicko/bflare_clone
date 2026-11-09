"""Raw SQL for text message embedding (peer-scoped; no harvester)."""

from __future__ import annotations

SELECT_MESSAGES_NEEDING_EMBED_SQL = """
SELECT tm.id, tm.message_id, tm.source_external_id, tm.peer_type, tm.content, tm.date
FROM telegram_messages tm
WHERE tm.source_external_id = %s
  AND (%s::timestamptz IS NULL OR tm.date >= %s::timestamptz)
  AND tm.date < %s::timestamptz
  AND tm.message_id > %s
  AND COALESCE(TRIM(tm.content), '') <> ''
ORDER BY tm.message_id ASC
LIMIT %s
"""

SELECT_FETCH_COVERAGE_FOR_PEER_SQL = """
SELECT covered_after, covered_before
FROM telegram_message_fetch_coverage
WHERE peer_external_id = %s
  AND peer_type = %s
ORDER BY covered_after NULLS FIRST, covered_before
"""

SELECT_EMBED_COVERAGE_SQL = """
SELECT covered_after, covered_before
FROM telegram_message_embed_coverage
WHERE peer_external_id = %s
  AND peer_type = %s
ORDER BY covered_after NULLS FIRST, covered_before
"""

DELETE_EMBED_COVERAGE_SQL = """
DELETE FROM telegram_message_embed_coverage
WHERE peer_external_id = %s
  AND peer_type = %s
"""

INSERT_EMBED_COVERAGE_SQL = """
INSERT INTO telegram_message_embed_coverage (
  peer_external_id,
  peer_type,
  covered_after,
  covered_before,
  updated_at
)
VALUES (%s, %s, %s, %s, now())
"""

SELECT_EMBED_CURSOR_SQL = """
SELECT last_message_id, is_complete
FROM telegram_message_embed_cursors
WHERE peer_external_id = %s
  AND peer_type = %s
  AND window_before = %s
  AND window_after IS NOT DISTINCT FROM %s
LIMIT 1
"""

UPSERT_EMBED_CURSOR_SQL = """
INSERT INTO telegram_message_embed_cursors AS c (
  peer_external_id,
  peer_type,
  window_after,
  window_before,
  last_message_id,
  is_complete,
  updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, now())
ON CONFLICT (
  peer_external_id,
  peer_type,
  window_before,
  window_after
)
DO UPDATE SET
  last_message_id = GREATEST(c.last_message_id, EXCLUDED.last_message_id),
  is_complete = EXCLUDED.is_complete,
  updated_at = now()
"""

# Peers with fetch coverage whose embed tail lags fetch (or incomplete cursor).
LIST_PEERS_PENDING_EMBED_SQL = """
WITH params AS (
  SELECT
    %s::timestamptz AS window_after,
    %s::timestamptz AS window_before
),
communities AS (
  SELECT 'channel'::text AS peer_type, external_id
  FROM telegram_channels
  UNION ALL
  SELECT 'megagroup'::text AS peer_type, external_id
  FROM telegram_megagroups
  UNION ALL
  SELECT 'chat'::text AS peer_type, external_id
  FROM telegram_chats
),
fetch_stats AS (
  SELECT
    cov.peer_external_id,
    cov.peer_type,
    MAX(cov.covered_before) AS max_fetch_before
  FROM telegram_message_fetch_coverage cov
  CROSS JOIN params p
  WHERE (
    p.window_after IS NULL
    OR cov.covered_after IS NULL
    OR cov.covered_after <= p.window_after
  )
  GROUP BY cov.peer_external_id, cov.peer_type
),
embed_stats AS (
  SELECT
    cov.peer_external_id,
    cov.peer_type,
    MAX(cov.covered_before) AS max_embed_before
  FROM telegram_message_embed_coverage cov
  GROUP BY cov.peer_external_id, cov.peer_type
),
pending AS (
  SELECT
    c.external_id AS peer_external_id,
    c.peer_type,
    fs.max_fetch_before,
    es.max_embed_before
  FROM communities c
  CROSS JOIN params p
  INNER JOIN fetch_stats fs
    ON fs.peer_external_id = c.external_id
   AND fs.peer_type = c.peer_type
  LEFT JOIN embed_stats es
    ON es.peer_external_id = c.external_id
   AND es.peer_type = c.peer_type
  WHERE c.peer_type = ANY(%s)
    AND (
      es.max_embed_before IS NULL
      OR es.max_embed_before < CASE
        WHEN p.window_before IS NULL THEN fs.max_fetch_before
        ELSE LEAST(fs.max_fetch_before, p.window_before)
      END
      OR EXISTS (
        SELECT 1
        FROM telegram_message_embed_cursors cur
        WHERE cur.peer_external_id = c.external_id
          AND cur.peer_type = c.peer_type
          AND cur.is_complete = false
          AND cur.window_after IS NOT DISTINCT FROM p.window_after
          AND (
            p.window_before IS NULL
            OR cur.window_before IS NOT DISTINCT FROM p.window_before
          )
      )
    )
)
SELECT peer_external_id, peer_type
FROM pending
ORDER BY max_embed_before ASC NULLS FIRST, peer_type, peer_external_id
LIMIT %s
"""

LIST_PEERS_WITH_FETCH_COVERAGE_SQL = """
WITH communities AS (
  SELECT 'channel'::text AS peer_type, external_id
  FROM telegram_channels
  UNION ALL
  SELECT 'megagroup'::text AS peer_type, external_id
  FROM telegram_megagroups
  UNION ALL
  SELECT 'chat'::text AS peer_type, external_id
  FROM telegram_chats
)
SELECT DISTINCT c.external_id AS peer_external_id, c.peer_type
FROM communities c
INNER JOIN telegram_message_fetch_coverage cov
  ON cov.peer_external_id = c.external_id
 AND cov.peer_type = c.peer_type
WHERE c.peer_type = ANY(%s)
ORDER BY c.peer_type, c.external_id
LIMIT %s
"""

LIST_PEERS_FOR_EXTERNAL_ID_SQL = """
SELECT external_id AS peer_external_id, peer_type
FROM (
  SELECT 'channel'::text AS peer_type, external_id
  FROM telegram_channels
  WHERE external_id = %s
  UNION ALL
  SELECT 'megagroup'::text AS peer_type, external_id
  FROM telegram_megagroups
  WHERE external_id = %s
  UNION ALL
  SELECT 'chat'::text AS peer_type, external_id
  FROM telegram_chats
  WHERE external_id = %s
) t
LIMIT 1
"""

LIST_PEERS_FOR_COMMUNITY_ID_SQL = """
SELECT external_id AS peer_external_id, peer_type
FROM (
  SELECT 'channel'::text AS peer_type, id, external_id
  FROM telegram_channels
  UNION ALL
  SELECT 'megagroup'::text AS peer_type, id, external_id
  FROM telegram_megagroups
  UNION ALL
  SELECT 'chat'::text AS peer_type, id, external_id
  FROM telegram_chats
) t
WHERE id = %s
LIMIT 1
"""

LIST_PEERS_FOR_NAME_SQL = """
SELECT external_id AS peer_external_id, peer_type
FROM (
  SELECT 'channel'::text AS peer_type, external_id, title
  FROM telegram_channels
  UNION ALL
  SELECT 'megagroup'::text AS peer_type, external_id, title
  FROM telegram_megagroups
  UNION ALL
  SELECT 'chat'::text AS peer_type, external_id, title
  FROM telegram_chats
) t
WHERE title ILIKE %s
ORDER BY peer_type, external_id
"""

LIST_PEERS_FOR_CATEGORIES_SQL = """
WITH peer_set AS (
  SELECT DISTINCT pc.peer_type, pc.community_id
  FROM peer_categories pc
  JOIN categories c
    ON c.id = pc.category_id
  WHERE c.slug = ANY(%s)
),
communities AS (
  SELECT 'channel'::text AS peer_type, id AS community_id, external_id
  FROM telegram_channels
  UNION ALL
  SELECT 'megagroup'::text AS peer_type, id AS community_id, external_id
  FROM telegram_megagroups
  UNION ALL
  SELECT 'chat'::text AS peer_type, id AS community_id, external_id
  FROM telegram_chats
)
SELECT DISTINCT u.external_id AS peer_external_id, u.peer_type
FROM peer_set ps
JOIN communities u
  ON u.peer_type = ps.peer_type
 AND u.community_id = ps.community_id
ORDER BY u.peer_type, u.external_id
"""
