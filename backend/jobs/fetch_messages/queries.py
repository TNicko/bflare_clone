"""Raw SQL (upserts/selects) for Telegram message fetching state."""

from __future__ import annotations

UPSERT_CURSOR_SQL = """
INSERT INTO telegram_message_fetch_cursors AS c (
  harvester_id,
  peer_external_id,
  peer_type,
  window_before,
  window_after,
  next_offset_id,
  is_complete,
  last_message_id_seen,
  last_message_date_seen,
  updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (harvester_id, peer_external_id, window_before, window_after)
DO UPDATE SET
  next_offset_id = EXCLUDED.next_offset_id,
  is_complete = EXCLUDED.is_complete,
  last_message_id_seen = EXCLUDED.last_message_id_seen,
  last_message_date_seen = EXCLUDED.last_message_date_seen,
  updated_at = now()
"""


SELECT_CURSOR_SQL = """
SELECT next_offset_id, is_complete
FROM telegram_message_fetch_cursors
WHERE harvester_id = %s
  AND peer_external_id = %s
  AND peer_type = %s
  AND window_before IS NOT DISTINCT FROM %s
  AND window_after IS NOT DISTINCT FROM %s
LIMIT 1
"""

#: Resume an incomplete open-ended fetch when ``TELEGRAM_FETCH_BEFORE`` is unset:
#: each run uses ``now`` as gap end, so the exact window_before key never matches.
SELECT_INCOMPLETE_CURSOR_FOR_AFTER_SQL = """
SELECT next_offset_id, is_complete, window_before
FROM telegram_message_fetch_cursors
WHERE harvester_id = %s
  AND peer_external_id = %s
  AND peer_type = %s
  AND window_after IS NOT DISTINCT FROM %s
  AND is_complete = false
ORDER BY updated_at DESC
LIMIT 1
"""

DELETE_OTHER_INCOMPLETE_CURSORS_FOR_AFTER_SQL = """
DELETE FROM telegram_message_fetch_cursors
WHERE harvester_id = %s
  AND peer_external_id = %s
  AND peer_type = %s
  AND window_after IS NOT DISTINCT FROM %s
  AND is_complete = false
  AND window_before IS DISTINCT FROM %s
"""


# Capacity-aware: among worker_harvester_ids with parent role + healthy system
# session, assign each peer to the least-loaded harvester (tie-break lowest
# external_id), then filter to assignee_harvester_id.
LIST_PEER_ASSIGNMENTS_SQL = """
WITH communities AS (
  SELECT 'channel'::text AS peer_type, external_id
  FROM telegram_channels
  UNION ALL
  SELECT 'megagroup'::text AS peer_type, external_id
  FROM telegram_megagroups
  UNION ALL
  SELECT 'chat'::text AS peer_type, external_id
  FROM telegram_chats
),
eligible AS (
  SELECT
    c.external_id AS peer_external_id,
    c.peer_type,
    r.access_hash,
    h.id AS harvester_id,
    h.external_id AS harvester_external_id
  FROM communities AS c
  INNER JOIN telegram_harvester_peer_roles AS r
    ON r.external_id = c.external_id
   AND r.role = 'parent'
   AND r.access_error_code IS NULL
  INNER JOIN telegram_harvesters AS h
    ON h.id = r.harvester_id
   AND h.is_active = true
  WHERE c.peer_type = ANY(%s)
    AND h.id = ANY(%s)
    AND EXISTS (
      SELECT 1 FROM telegram_harvester_sessions hs
      WHERE hs.harvester_id = h.id
        AND hs.session_type = 'system'
        AND hs.auth_state = 'healthy'
    )
),
harvester_load AS (
  SELECT harvester_id, COUNT(DISTINCT peer_external_id)::bigint AS pending_n
  FROM eligible
  GROUP BY harvester_id
),
ranked AS (
  SELECT
    e.peer_external_id,
    e.peer_type,
    e.access_hash,
    e.harvester_id,
    e.harvester_external_id,
    ROW_NUMBER() OVER (
      PARTITION BY e.peer_external_id
      ORDER BY hl.pending_n ASC, e.harvester_external_id ASC
    ) AS rn
  FROM eligible AS e
  JOIN harvester_load AS hl ON hl.harvester_id = e.harvester_id
)
SELECT
  peer_external_id,
  peer_type,
  access_hash,
  harvester_id,
  harvester_external_id
FROM ranked
WHERE rn = 1
  AND harvester_id = %s
ORDER BY peer_external_id
"""


LIST_PEER_ASSIGNMENTS_LIMITED_SQL = """
WITH communities AS (
  SELECT 'channel'::text AS peer_type, external_id
  FROM telegram_channels
  UNION ALL
  SELECT 'megagroup'::text AS peer_type, external_id
  FROM telegram_megagroups
  UNION ALL
  SELECT 'chat'::text AS peer_type, external_id
  FROM telegram_chats
),
eligible AS (
  SELECT
    c.external_id AS peer_external_id,
    c.peer_type,
    r.access_hash,
    h.id AS harvester_id,
    h.external_id AS harvester_external_id
  FROM communities AS c
  INNER JOIN telegram_harvester_peer_roles AS r
    ON r.external_id = c.external_id
   AND r.role = 'parent'
   AND r.access_error_code IS NULL
  INNER JOIN telegram_harvesters AS h
    ON h.id = r.harvester_id
   AND h.is_active = true
  WHERE c.peer_type = ANY(%s)
    AND h.id = ANY(%s)
    AND EXISTS (
      SELECT 1 FROM telegram_harvester_sessions hs
      WHERE hs.harvester_id = h.id
        AND hs.session_type = 'system'
        AND hs.auth_state = 'healthy'
    )
),
harvester_load AS (
  SELECT harvester_id, COUNT(DISTINCT peer_external_id)::bigint AS pending_n
  FROM eligible
  GROUP BY harvester_id
),
ranked AS (
  SELECT
    e.peer_external_id,
    e.peer_type,
    e.access_hash,
    e.harvester_id,
    e.harvester_external_id,
    ROW_NUMBER() OVER (
      PARTITION BY e.peer_external_id
      ORDER BY hl.pending_n ASC, e.harvester_external_id ASC
    ) AS rn
  FROM eligible AS e
  JOIN harvester_load AS hl ON hl.harvester_id = e.harvester_id
)
SELECT
  peer_external_id,
  peer_type,
  access_hash,
  harvester_id,
  harvester_external_id
FROM ranked
WHERE rn = 1
  AND harvester_id = %s
ORDER BY peer_external_id
LIMIT %s
"""


LIST_PEER_ASSIGNMENTS_PENDING_SQL = """
WITH params AS (
  SELECT
    %s::timestamptz AS window_after,
    %s::timestamptz AS window_before,
    %s::int AS slack_minutes
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
assigned AS (
  SELECT
    c.peer_type,
    c.external_id AS peer_external_id,
    r.access_hash,
    h.id AS harvester_id,
    h.external_id AS harvester_external_id
  FROM communities c
  INNER JOIN telegram_harvester_peer_roles r
    ON r.external_id = c.external_id
   AND r.role = 'parent'
   AND r.access_error_code IS NULL
  INNER JOIN telegram_harvesters h
    ON h.id = r.harvester_id
   AND h.is_active = true
  WHERE c.peer_type = ANY(%s)
    AND h.id = ANY(%s)
    AND EXISTS (
      SELECT 1
      FROM telegram_harvester_sessions hs
      WHERE hs.harvester_id = h.id
        AND hs.session_type = 'system'
        AND hs.auth_state = 'healthy'
    )
),
coverage_stats AS (
  SELECT
    cov.harvester_id,
    cov.peer_external_id,
    cov.peer_type,
    MAX(cov.covered_before) AS max_covered_before
  FROM telegram_message_fetch_coverage cov
  CROSS JOIN params p
  WHERE (
    p.window_after IS NULL
    OR cov.covered_after IS NULL
    OR cov.covered_after <= p.window_after
  )
  GROUP BY cov.harvester_id, cov.peer_external_id, cov.peer_type
),
pending AS (
  SELECT
    a.peer_external_id,
    a.peer_type,
    a.access_hash,
    a.harvester_id,
    a.harvester_external_id,
    cs.max_covered_before
  FROM assigned a
  CROSS JOIN params p
  LEFT JOIN coverage_stats cs
    ON cs.harvester_id = a.harvester_id
   AND cs.peer_external_id = a.peer_external_id
   AND cs.peer_type = a.peer_type
  WHERE
    cs.max_covered_before IS NULL
    OR cs.max_covered_before < CASE
      WHEN p.window_before IS NULL THEN
        now() - (p.slack_minutes || ' minutes')::interval
      ELSE
        p.window_before
    END
    -- Open window_after (−∞): still pending if no coverage interval starts at −∞
    -- (e.g. prior AFTER=2026-05-20 left covered_after set → history before that is a gap).
    OR (
      p.window_after IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM telegram_message_fetch_coverage cov
        WHERE cov.harvester_id = a.harvester_id
          AND cov.peer_external_id = a.peer_external_id
          AND cov.peer_type = a.peer_type
          AND cov.covered_after IS NULL
      )
    )
    OR EXISTS (
      SELECT 1
      FROM telegram_message_fetch_cursors cur
      WHERE cur.harvester_id = a.harvester_id
        AND cur.peer_external_id = a.peer_external_id
        AND cur.peer_type = a.peer_type
        AND cur.is_complete = false
        AND cur.window_after IS NOT DISTINCT FROM p.window_after
        AND (
          p.window_before IS NULL
          OR cur.window_before IS NOT DISTINCT FROM p.window_before
        )
    )
),
harvester_load AS (
  SELECT harvester_id, COUNT(DISTINCT peer_external_id)::bigint AS pending_n
  FROM pending
  GROUP BY harvester_id
),
ranked AS (
  SELECT
    p.peer_external_id,
    p.peer_type,
    p.access_hash,
    p.harvester_id,
    p.harvester_external_id,
    p.max_covered_before,
    ROW_NUMBER() OVER (
      PARTITION BY p.peer_external_id
      ORDER BY hl.pending_n ASC, p.harvester_external_id ASC
    ) AS rn
  FROM pending AS p
  JOIN harvester_load AS hl ON hl.harvester_id = p.harvester_id
)
SELECT
  peer_external_id,
  peer_type,
  access_hash,
  harvester_id,
  harvester_external_id
FROM ranked
WHERE rn = 1
  AND harvester_id = %s
ORDER BY max_covered_before ASC NULLS FIRST, peer_type, peer_external_id
LIMIT %s
"""

SELECT_PEER_USERNAME_SQL = """
SELECT username
FROM telegram_channels
WHERE external_id = %s
UNION ALL
SELECT username
FROM telegram_megagroups
WHERE external_id = %s
LIMIT 1
"""

# Legacy LEFT JOIN (all parent roles + unassigned). Used by harvest_peer /
# fetch_dialogues and by fetch_messages bootstrap resolve for unassigned peers.
LIST_PEER_ASSIGNMENTS_FOR_EXTERNAL_ID_SQL = """
WITH target AS (
  SELECT %s::text AS external_id
),
peer_type_hint AS (
  SELECT 'channel'::text AS peer_type, external_id
  FROM telegram_channels
  WHERE external_id = (SELECT external_id FROM target)
  UNION ALL
  SELECT 'megagroup'::text AS peer_type, external_id
  FROM telegram_megagroups
  WHERE external_id = (SELECT external_id FROM target)
  UNION ALL
  SELECT 'chat'::text AS peer_type, external_id
  FROM telegram_chats
  WHERE external_id = (SELECT external_id FROM target)
  LIMIT 1
)
SELECT
  t.external_id AS peer_external_id,
  COALESCE(r.peer_type, pth.peer_type, 'channel') AS peer_type,
  r.access_hash,
  h.id AS harvester_id,
  h.external_id AS harvester_external_id
FROM target t
LEFT JOIN peer_type_hint pth
  ON pth.external_id = t.external_id
LEFT JOIN telegram_harvester_peer_roles AS r
  ON r.external_id = t.external_id
 AND r.role = 'parent'
 AND r.access_error_code IS NULL
LEFT JOIN telegram_harvesters AS h
  ON h.id = r.harvester_id
 AND h.is_active = true
 AND EXISTS (
   SELECT 1 FROM telegram_harvester_sessions hs
   WHERE hs.harvester_id = h.id
     AND hs.session_type = 'system'
     AND hs.auth_state = 'healthy'
 )
"""

LIST_PEER_ASSIGNMENTS_FOR_EXTERNAL_ID_ASSIGNED_SQL = """
WITH target AS (
  SELECT %s::text AS external_id
),
peer_type_hint AS (
  SELECT 'channel'::text AS peer_type, external_id
  FROM telegram_channels
  WHERE external_id = (SELECT external_id FROM target)
  UNION ALL
  SELECT 'megagroup'::text AS peer_type, external_id
  FROM telegram_megagroups
  WHERE external_id = (SELECT external_id FROM target)
  UNION ALL
  SELECT 'chat'::text AS peer_type, external_id
  FROM telegram_chats
  WHERE external_id = (SELECT external_id FROM target)
  LIMIT 1
),
eligible AS (
  SELECT
    t.external_id AS peer_external_id,
    COALESCE(r.peer_type, pth.peer_type, 'channel') AS peer_type,
    r.access_hash,
    h.id AS harvester_id,
    h.external_id AS harvester_external_id
  FROM target t
  LEFT JOIN peer_type_hint pth
    ON pth.external_id = t.external_id
  INNER JOIN telegram_harvester_peer_roles AS r
    ON r.external_id = t.external_id
   AND r.role = 'parent'
   AND r.access_error_code IS NULL
  INNER JOIN telegram_harvesters AS h
    ON h.id = r.harvester_id
   AND h.is_active = true
  WHERE h.id = ANY(%s)
    AND EXISTS (
      SELECT 1 FROM telegram_harvester_sessions hs
      WHERE hs.harvester_id = h.id
        AND hs.session_type = 'system'
        AND hs.auth_state = 'healthy'
    )
),
harvester_load AS (
  SELECT harvester_id, COUNT(DISTINCT peer_external_id)::bigint AS pending_n
  FROM eligible
  GROUP BY harvester_id
),
ranked AS (
  SELECT
    e.peer_external_id,
    e.peer_type,
    e.access_hash,
    e.harvester_id,
    e.harvester_external_id,
    ROW_NUMBER() OVER (
      PARTITION BY e.peer_external_id
      ORDER BY hl.pending_n ASC, e.harvester_external_id ASC
    ) AS rn
  FROM eligible AS e
  JOIN harvester_load AS hl ON hl.harvester_id = e.harvester_id
)
SELECT
  peer_external_id,
  peer_type,
  access_hash,
  harvester_id,
  harvester_external_id
FROM ranked
WHERE rn = 1
  AND harvester_id = %s
"""

LIST_PEER_ASSIGNMENTS_FOR_NAME_SQL = """
WITH communities AS (
  SELECT 'channel'::text AS community_peer_type, id AS community_id, external_id, title
  FROM telegram_channels
  UNION ALL
  SELECT 'megagroup'::text AS community_peer_type, id AS community_id, external_id, title
  FROM telegram_megagroups
  UNION ALL
  SELECT 'chat'::text AS community_peer_type, id AS community_id, external_id, title
  FROM telegram_chats
),
matched AS (
  SELECT DISTINCT community_peer_type AS peer_type, external_id
  FROM communities
  WHERE title ILIKE %s
)
SELECT
  m.external_id AS peer_external_id,
  m.peer_type,
  r.access_hash,
  h.id AS harvester_id,
  h.external_id AS harvester_external_id
FROM matched m
LEFT JOIN telegram_harvester_peer_roles AS r
  ON r.external_id = m.external_id
 AND r.role = 'parent'
 AND r.access_error_code IS NULL
LEFT JOIN telegram_harvesters AS h
  ON h.id = r.harvester_id
 AND h.is_active = true
 AND EXISTS (
   SELECT 1 FROM telegram_harvester_sessions hs
   WHERE hs.harvester_id = h.id
     AND hs.session_type = 'system'
     AND hs.auth_state = 'healthy'
 )
ORDER BY h.external_id NULLS LAST, m.peer_type, m.external_id
"""

LIST_PEER_ASSIGNMENTS_FOR_NAME_ASSIGNED_SQL = """
WITH communities AS (
  SELECT 'channel'::text AS community_peer_type, id AS community_id, external_id, title
  FROM telegram_channels
  UNION ALL
  SELECT 'megagroup'::text AS community_peer_type, id AS community_id, external_id, title
  FROM telegram_megagroups
  UNION ALL
  SELECT 'chat'::text AS community_peer_type, id AS community_id, external_id, title
  FROM telegram_chats
),
matched AS (
  SELECT DISTINCT community_peer_type AS peer_type, external_id
  FROM communities
  WHERE title ILIKE %s
),
eligible AS (
  SELECT
    m.external_id AS peer_external_id,
    m.peer_type,
    r.access_hash,
    h.id AS harvester_id,
    h.external_id AS harvester_external_id
  FROM matched m
  INNER JOIN telegram_harvester_peer_roles AS r
    ON r.external_id = m.external_id
   AND r.role = 'parent'
   AND r.access_error_code IS NULL
  INNER JOIN telegram_harvesters AS h
    ON h.id = r.harvester_id
   AND h.is_active = true
  WHERE h.id = ANY(%s)
    AND EXISTS (
      SELECT 1 FROM telegram_harvester_sessions hs
      WHERE hs.harvester_id = h.id
        AND hs.session_type = 'system'
        AND hs.auth_state = 'healthy'
    )
),
harvester_load AS (
  SELECT harvester_id, COUNT(DISTINCT peer_external_id)::bigint AS pending_n
  FROM eligible
  GROUP BY harvester_id
),
ranked AS (
  SELECT
    e.peer_external_id,
    e.peer_type,
    e.access_hash,
    e.harvester_id,
    e.harvester_external_id,
    ROW_NUMBER() OVER (
      PARTITION BY e.peer_external_id
      ORDER BY hl.pending_n ASC, e.harvester_external_id ASC
    ) AS rn
  FROM eligible AS e
  JOIN harvester_load AS hl ON hl.harvester_id = e.harvester_id
)
SELECT
  peer_external_id,
  peer_type,
  access_hash,
  harvester_id,
  harvester_external_id
FROM ranked
WHERE rn = 1
  AND harvester_id = %s
ORDER BY peer_type, peer_external_id
"""

LIST_PEER_ASSIGNMENTS_FOR_COMMUNITY_ID_SQL = """
WITH communities AS (
  SELECT 'channel'::text AS community_peer_type, id AS community_id, external_id
  FROM telegram_channels
  UNION ALL
  SELECT 'megagroup'::text AS community_peer_type, id AS community_id, external_id
  FROM telegram_megagroups
  UNION ALL
  SELECT 'chat'::text AS community_peer_type, id AS community_id, external_id
  FROM telegram_chats
),
target AS (
  SELECT community_peer_type AS peer_type, external_id
  FROM communities
  WHERE community_id = %s
  LIMIT 1
)
SELECT
  t.external_id AS peer_external_id,
  t.peer_type,
  r.access_hash,
  h.id AS harvester_id,
  h.external_id AS harvester_external_id
FROM target t
LEFT JOIN telegram_harvester_peer_roles AS r
  ON r.external_id = t.external_id
 AND r.role = 'parent'
 AND r.access_error_code IS NULL
LEFT JOIN telegram_harvesters AS h
  ON h.id = r.harvester_id
 AND h.is_active = true
 AND EXISTS (
   SELECT 1 FROM telegram_harvester_sessions hs
   WHERE hs.harvester_id = h.id
     AND hs.session_type = 'system'
     AND hs.auth_state = 'healthy'
 )
"""

LIST_PEER_ASSIGNMENTS_FOR_COMMUNITY_ID_ASSIGNED_SQL = """
WITH communities AS (
  SELECT 'channel'::text AS community_peer_type, id AS community_id, external_id
  FROM telegram_channels
  UNION ALL
  SELECT 'megagroup'::text AS community_peer_type, id AS community_id, external_id
  FROM telegram_megagroups
  UNION ALL
  SELECT 'chat'::text AS community_peer_type, id AS community_id, external_id
  FROM telegram_chats
),
target AS (
  SELECT community_peer_type AS peer_type, external_id
  FROM communities
  WHERE community_id = %s
  LIMIT 1
),
eligible AS (
  SELECT
    t.external_id AS peer_external_id,
    t.peer_type,
    r.access_hash,
    h.id AS harvester_id,
    h.external_id AS harvester_external_id
  FROM target t
  INNER JOIN telegram_harvester_peer_roles AS r
    ON r.external_id = t.external_id
   AND r.role = 'parent'
   AND r.access_error_code IS NULL
  INNER JOIN telegram_harvesters AS h
    ON h.id = r.harvester_id
   AND h.is_active = true
  WHERE h.id = ANY(%s)
    AND EXISTS (
      SELECT 1 FROM telegram_harvester_sessions hs
      WHERE hs.harvester_id = h.id
        AND hs.session_type = 'system'
        AND hs.auth_state = 'healthy'
    )
),
harvester_load AS (
  SELECT harvester_id, COUNT(DISTINCT peer_external_id)::bigint AS pending_n
  FROM eligible
  GROUP BY harvester_id
),
ranked AS (
  SELECT
    e.peer_external_id,
    e.peer_type,
    e.access_hash,
    e.harvester_id,
    e.harvester_external_id,
    ROW_NUMBER() OVER (
      PARTITION BY e.peer_external_id
      ORDER BY hl.pending_n ASC, e.harvester_external_id ASC
    ) AS rn
  FROM eligible AS e
  JOIN harvester_load AS hl ON hl.harvester_id = e.harvester_id
)
SELECT
  peer_external_id,
  peer_type,
  access_hash,
  harvester_id,
  harvester_external_id
FROM ranked
WHERE rn = 1
  AND harvester_id = %s
"""

LIST_PEER_ASSIGNMENTS_FOR_CATEGORIES_SQL = """
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
),
targets AS (
  SELECT DISTINCT u.peer_type, u.external_id
  FROM peer_set ps
  JOIN communities u
    ON u.peer_type = ps.peer_type
   AND u.community_id = ps.community_id
)
SELECT
  t.external_id AS peer_external_id,
  t.peer_type,
  r.access_hash,
  h.id AS harvester_id,
  h.external_id AS harvester_external_id
FROM targets t
LEFT JOIN telegram_harvester_peer_roles AS r
  ON r.external_id = t.external_id
 AND r.role = 'parent'
 AND r.access_error_code IS NULL
LEFT JOIN telegram_harvesters AS h
  ON h.id = r.harvester_id
 AND h.is_active = true
 AND EXISTS (
   SELECT 1 FROM telegram_harvester_sessions hs
   WHERE hs.harvester_id = h.id
     AND hs.session_type = 'system'
     AND hs.auth_state = 'healthy'
 )
ORDER BY h.external_id NULLS LAST, t.peer_type, t.external_id
"""

LIST_PEER_ASSIGNMENTS_FOR_CATEGORIES_ASSIGNED_SQL = """
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
),
targets AS (
  SELECT DISTINCT u.peer_type, u.external_id
  FROM peer_set ps
  JOIN communities u
    ON u.peer_type = ps.peer_type
   AND u.community_id = ps.community_id
),
eligible AS (
  SELECT
    t.external_id AS peer_external_id,
    t.peer_type,
    r.access_hash,
    h.id AS harvester_id,
    h.external_id AS harvester_external_id
  FROM targets t
  INNER JOIN telegram_harvester_peer_roles AS r
    ON r.external_id = t.external_id
   AND r.role = 'parent'
   AND r.access_error_code IS NULL
  INNER JOIN telegram_harvesters AS h
    ON h.id = r.harvester_id
   AND h.is_active = true
  WHERE h.id = ANY(%s)
    AND EXISTS (
      SELECT 1 FROM telegram_harvester_sessions hs
      WHERE hs.harvester_id = h.id
        AND hs.session_type = 'system'
        AND hs.auth_state = 'healthy'
    )
),
harvester_load AS (
  SELECT harvester_id, COUNT(DISTINCT peer_external_id)::bigint AS pending_n
  FROM eligible
  GROUP BY harvester_id
),
ranked AS (
  SELECT
    e.peer_external_id,
    e.peer_type,
    e.access_hash,
    e.harvester_id,
    e.harvester_external_id,
    ROW_NUMBER() OVER (
      PARTITION BY e.peer_external_id
      ORDER BY hl.pending_n ASC, e.harvester_external_id ASC
    ) AS rn
  FROM eligible AS e
  JOIN harvester_load AS hl ON hl.harvester_id = e.harvester_id
)
SELECT
  peer_external_id,
  peer_type,
  access_hash,
  harvester_id,
  harvester_external_id
FROM ranked
WHERE rn = 1
  AND harvester_id = %s
ORDER BY peer_type, peer_external_id
"""

# Unassigned peers for bootstrap resolve (all-communities / filtered modes).
LIST_UNASSIGNED_PEER_SELECTIONS_SQL = """
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
SELECT
  c.external_id AS peer_external_id,
  c.peer_type,
  NULL::bigint AS access_hash,
  NULL::uuid AS harvester_id,
  NULL::text AS harvester_external_id
FROM communities AS c
WHERE c.peer_type = ANY(%s)
  AND NOT EXISTS (
    SELECT 1
    FROM telegram_harvester_peer_roles AS r
    INNER JOIN telegram_harvesters AS h
      ON h.id = r.harvester_id
     AND h.is_active = true
    WHERE r.external_id = c.external_id
      AND r.role = 'parent'
      AND r.access_error_code IS NULL
      AND EXISTS (
        SELECT 1 FROM telegram_harvester_sessions hs
        WHERE hs.harvester_id = h.id
          AND hs.session_type = 'system'
          AND hs.auth_state = 'healthy'
      )
  )
ORDER BY c.peer_type, c.external_id
LIMIT %s
"""

SELECT_COVERAGE_SQL = """
SELECT covered_after, covered_before
FROM telegram_message_fetch_coverage
WHERE harvester_id = %s
  AND peer_external_id = %s
  AND peer_type = %s
ORDER BY covered_after NULLS FIRST, covered_before
"""

DELETE_COVERAGE_SQL = """
DELETE FROM telegram_message_fetch_coverage
WHERE harvester_id = %s
  AND peer_external_id = %s
  AND peer_type = %s
"""

INSERT_COVERAGE_SQL = """
INSERT INTO telegram_message_fetch_coverage (
  harvester_id,
  peer_external_id,
  peer_type,
  covered_after,
  covered_before,
  updated_at
)
VALUES (%s, %s, %s, %s, %s, now())
"""
