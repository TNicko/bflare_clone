"""Fill a missing community language from text already stored for that peer.

Page loads stay fast: detection runs off the request and the client polls until
the column is written. Peers that already have a language, including ``und``,
are left alone.
"""

from __future__ import annotations

import asyncio
import logging
import time
from psycopg import sql
from psycopg.rows import dict_row
from pydantic import BaseModel

from utils.language_detect import (
    UNDETERMINED,
    build_detection_text,
    label_from_samples,
    letter_char_count,
)
from services.api.src.crud.supabase import get_conn

lg = logging.getLogger(__name__)

# Detection thresholds for on-demand labeling.
_MESSAGE_SAMPLES = 40
_MIN_CHARS = 40
_MIN_CONFIDENCE = 0.45
# Not enough stored text yet. Retry on a later visit instead of writing ``und``.
_SKIP_SECONDS = 120.0

_TABLES = {
    "channel": "telegram_channels",
    "megagroup": "telegram_megagroups",
}


class PeerLanguageResponse(BaseModel):
    language: str | None = None
    pending: bool = False


_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}
_skip_until: dict[tuple[str, str], float] = {}
_detect_lock = asyncio.Lock()


def language_is_unset(language: str | None) -> bool:
    return not str(language or "").strip()


def language_should_detect(language: str | None) -> bool:
    """True when a page open should try to label the peer.

    ``und`` is a previous "not enough signal" result. Stored posts can make a
    real code possible later, so it is filled again.
    """
    value = str(language or "").strip().lower()
    return not value or value == "und"


def choose_language_code(
    *,
    title: str | None,
    about: str | None,
    messages: list[str],
) -> str | None:
    """ISO code from stored profile text and message samples, or None when too thin.

    ``None`` means do not write. A returned ``und`` is a real result and should be stored.
    """
    text = build_detection_text(title=title, about=about, messages=messages, include_profile=True)
    if letter_char_count(text) < _MIN_CHARS:
        return None
    guess = label_from_samples(
        title=title,
        about=about,
        messages=messages,
        min_chars=_MIN_CHARS,
        min_confidence=_MIN_CONFIDENCE,
    )
    return guess.code


def _skipped(key: tuple[str, str]) -> bool:
    until = _skip_until.get(key)
    if until is None:
        return False
    if time.monotonic() >= until:
        _skip_until.pop(key, None)
        return False
    return True


def _mark_skipped(key: tuple[str, str]) -> None:
    _skip_until[key] = time.monotonic() + _SKIP_SECONDS


def _peer_select(table: str) -> sql.Composed:
    return sql.SQL(
        """
        SELECT title, about, language
        FROM {}
        WHERE external_id::text = %s
        """
    ).format(sql.Identifier(table))


async def _read_language(external_id: str, peer_type: str) -> str | None:
    """Stored language, or None when the peer is missing or unlabeled."""
    table = _TABLES.get(peer_type)
    if table is None:
        return None
    async with get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_peer_select(table), (external_id,))
            row = await cur.fetchone()
    if row is None or row.get("language") is None:
        return None
    return str(row["language"])


async def _load_peer_text(
    external_id: str,
    peer_type: str,
) -> tuple[str | None, str | None, str | None, list[str]] | None:
    table = _TABLES.get(peer_type)
    if table is None:
        return None
    async with get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_peer_select(table), (external_id,))
            row = await cur.fetchone()
            if row is None:
                return None
            await cur.execute(
                """
                SELECT content
                FROM telegram_messages
                WHERE source_external_id::text = %s
                  AND content IS NOT NULL
                  AND btrim(content) <> ''
                  AND COALESCE(deleted, false) = false
                ORDER BY date DESC
                LIMIT %s
                """,
                (external_id, _MESSAGE_SAMPLES),
            )
            messages = [str(item["content"]).strip() for item in await cur.fetchall() if item.get("content")]
    return (
        str(row["language"]) if row.get("language") is not None else None,
        str(row["title"]) if row.get("title") is not None else None,
        str(row["about"]) if row.get("about") is not None else None,
        messages,
    )


async def _write_language(external_id: str, peer_type: str, code: str) -> None:
    table = _TABLES[peer_type]
    update_sql = sql.SQL(
        """
        UPDATE {}
        SET language = %s,
            updated_at = now()
        WHERE external_id::text = %s
          AND (
            NULLIF(btrim(COALESCE(language, '')), '') IS NULL
            OR lower(btrim(language)) = 'und'
          )
        """
    ).format(sql.Identifier(table))
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(update_sql, (code, external_id))


async def _fill(external_id: str, peer_type: str) -> None:
    key = (peer_type, external_id)
    try:
        loaded = await _load_peer_text(external_id, peer_type)
        if loaded is None:
            _mark_skipped(key)
            return
        language, title, about, messages = loaded
        if not language_should_detect(language):
            return
        async with _detect_lock:
            code = await asyncio.to_thread(
                choose_language_code,
                title=title,
                about=about,
                messages=messages,
            )
        if code is None or code == UNDETERMINED:
            lg.info(
                "peer language skipped external_id=%s peer_type=%s reason=%s messages=%s",
                external_id,
                peer_type,
                "thin_text" if code is None else "undetermined",
                len(messages),
            )
            _mark_skipped(key)
            return
        await _write_language(external_id, peer_type, code)
        lg.info(
            "peer language labeled external_id=%s peer_type=%s language=%s messages=%s",
            external_id,
            peer_type,
            code,
            len(messages),
        )
    except Exception:
        lg.exception(
            "peer language fill failed external_id=%s peer_type=%s",
            external_id,
            peer_type,
        )
        _mark_skipped(key)
    finally:
        current = _tasks.get(key)
        if current is asyncio.current_task():
            _tasks.pop(key, None)


def _schedule(external_id: str, peer_type: str) -> None:
    key = (peer_type, external_id)
    task = _tasks.get(key)
    if task is not None and not task.done():
        return
    _tasks[key] = asyncio.create_task(
        _fill(external_id, peer_type),
        name=f"peer-language:{peer_type}:{external_id}",
    )


async def peer_language_status(external_id: str, peer_type: str) -> PeerLanguageResponse:
    """Return the stored language, starting a background fill when it is missing."""
    if peer_type not in _TABLES:
        return PeerLanguageResponse(language=None, pending=False)

    key = (peer_type, external_id)
    language = await _read_language(external_id, peer_type)
    if not language_should_detect(language):
        return PeerLanguageResponse(language=str(language).strip(), pending=False)

    task = _tasks.get(key)
    if task is not None and not task.done():
        return PeerLanguageResponse(language=None, pending=True)
    if _skipped(key):
        return PeerLanguageResponse(language=None, pending=False)

    _schedule(external_id, peer_type)
    return PeerLanguageResponse(language=None, pending=True)
