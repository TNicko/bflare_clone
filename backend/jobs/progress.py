"""Report live progress into ``harvest_jobs.progress`` (JSONB merge).

The fleet agent sets ``HARVEST_JOB_ID`` in the task subprocess environment.
Any task can call :func:`report_progress` / :func:`flush_progress` — no-ops when
the env var is unset (local one-shot CLI runs).

Shared keys (convention):

- ``phase``: starting | running | finishing | done | failed
- ``task_type``: echo of the fleet task type
- ``detail``: short human string
- ``last_event``: ``{code, message, at}`` — latest operational event (FloodWait, etc.)
- ``warnings``: capped list of recent non-fatal event messages
- ``peers_seen`` / ``peers_done`` / ``peers_total`` / ``peers_failed`` / ``peers_skipped``
- ``messages_fetched`` / ``messages_scanned`` / ``messages_persisted``
- ``media_downloaded`` / ``media_failed`` / ``downloaded`` / ``skipped`` / ``embedded``
- ``current_peer``: string id or ``{external_id, label?}``
- ``current_depth``, ``depth_peers_total``, ``depth_peers_done``, ``depth_peers_pending``
- ``finished_peers``: capped list of ``{external_id, messages, status}``
- ``open_ended``: bool (fetch_messages with no peer scope)
- ``session_leases``: ``[{session_id, server_id}, ...]`` held Telethon session leases
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from db.connection import connect_async
from db.crud.system.jobs import update_job_progress

lg = logging.getLogger(__name__)

_ENV_JOB_ID = "HARVEST_JOB_ID"
_DEFAULT_MIN_INTERVAL_S = 2.0
_FINISHED_PEERS_CAP = 50
_WARNINGS_CAP = 20

_lock = asyncio.Lock()
_pending: dict[str, Any] = {}
_last_flush_at = 0.0
_job_id: UUID | None = None
_dsn: str | None = None
_loaded = False
_held_leases: list[dict[str, str]] = []


def _ensure_loaded() -> None:
    global _job_id, _dsn, _loaded
    if _loaded:
        return
    _loaded = True
    raw = (os.environ.get(_ENV_JOB_ID) or "").strip()
    if raw:
        try:
            _job_id = UUID(raw)
        except ValueError:
            lg.warning("invalid %s=%r — progress reporting disabled", _ENV_JOB_ID, raw)
            _job_id = None
    _dsn = (os.environ.get("POSTGRES_DSN") or "").strip() or None


def harvest_job_id() -> UUID | None:
    _ensure_loaded()
    return _job_id


def session_leases_from_progress(progress: Any) -> list[tuple[UUID, str]]:
    """Parse ``session_leases`` written by :func:`note_session_lease`."""
    if not isinstance(progress, dict):
        return []
    raw = progress.get("session_leases")
    if not isinstance(raw, list):
        return []
    out: list[tuple[UUID, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        sid = str(item.get("session_id") or "").strip()
        server_id = str(item.get("server_id") or "").strip()
        if not sid or not server_id:
            continue
        key = (sid, server_id)
        if key in seen:
            continue
        try:
            out.append((UUID(sid), server_id))
        except ValueError:
            continue
        seen.add(key)
    return out


async def note_session_lease(*, session_id: UUID, server_id: str) -> None:
    """Record a held session lease so the agent can release it after SIGKILL."""
    _ensure_loaded()
    if _job_id is None or not _dsn:
        return
    entry = {"session_id": str(session_id), "server_id": str(server_id)}
    async with _lock:
        if entry not in _held_leases:
            _held_leases.append(entry)
        _pending["session_leases"] = list(_held_leases)
        await _flush_locked(force=True)


async def drop_session_lease(*, session_id: UUID, server_id: str) -> None:
    """Drop a session lease from job progress after the job released it."""
    _ensure_loaded()
    if _job_id is None or not _dsn:
        return
    want = {"session_id": str(session_id), "server_id": str(server_id)}
    async with _lock:
        _held_leases[:] = [row for row in _held_leases if row != want]
        _pending["session_leases"] = list(_held_leases)
        await _flush_locked(force=True)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def make_event(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message, "at": utc_now_iso()}


def append_finished_peer(
    finished: list[dict[str, Any]] | None,
    *,
    external_id: str,
    messages: int = 0,
    status: str = "done",
    cap: int = _FINISHED_PEERS_CAP,
) -> list[dict[str, Any]]:
    """Append a finished-peer entry, capped to the most recent ``cap`` items."""
    out = list(finished or [])
    out.append(
        {
            "external_id": str(external_id),
            "messages": int(messages),
            "status": status,
        }
    )
    if len(out) > cap:
        out = out[-cap:]
    return out


def append_warning(warnings: list[Any] | None, message: str, *, cap: int = _WARNINGS_CAP) -> list[str]:
    out = [str(w) for w in (warnings or []) if w]
    msg = message.strip()
    if msg:
        out.append(msg)
    if len(out) > cap:
        out = out[-cap:]
    return out


async def report_progress(**fields: Any) -> None:
    """Merge ``fields`` into the running job's progress (throttled DB write)."""
    global _pending
    _ensure_loaded()
    if _job_id is None or not _dsn:
        return
    clean = {k: v for k, v in fields.items() if v is not None}
    if not clean:
        return
    async with _lock:
        _pending.update(clean)
        await _flush_locked(force=False)


async def flush_progress(**fields: Any) -> None:
    """Merge optional ``fields`` and write immediately."""
    global _pending
    _ensure_loaded()
    if _job_id is None or not _dsn:
        return
    clean = {k: v for k, v in fields.items() if v is not None}
    async with _lock:
        if clean:
            _pending.update(clean)
        await _flush_locked(force=True)


async def report_event(code: str, message: str, *, warning: bool = True) -> None:
    """Write a human-readable operational event into progress (and optional warnings)."""
    event = make_event(code, message)
    fields: dict[str, Any] = {"last_event": event, "detail": message}
    if warning:
        # warnings list is merged by SharedProgress; standalone callers replace with single entry.
        fields["warnings"] = [message]
    await report_progress(**fields)


async def _flush_locked(*, force: bool) -> None:
    """Caller must hold ``_lock``."""
    global _pending, _last_flush_at
    if not _pending:
        return
    now = asyncio.get_running_loop().time()
    if not force and (now - _last_flush_at) < _DEFAULT_MIN_INTERVAL_S:
        return
    patch = dict(_pending)
    _pending.clear()
    _last_flush_at = now
    assert _job_id is not None and _dsn is not None
    try:
        async with await connect_async(_dsn) as conn:
            await update_job_progress(conn, job_id=_job_id, progress=patch, merge=True)
            await conn.commit()
    except Exception:
        lg.exception("failed to write harvest job progress job_id=%s", _job_id)
        merged = dict(patch)
        merged.update(_pending)
        _pending = merged


class SharedProgress:
    """Process-wide state for multi-harvester jobs (same asyncio process).

    Concurrent workers ``bump`` integer deltas; ``set`` writes absolute fields.
    Safe when several harvesters run via ``run_across_harvesters``.
    """

    def __init__(self, **initial: Any) -> None:
        self._lock = asyncio.Lock()
        self._state: dict[str, Any] = {k: v for k, v in initial.items() if v is not None}

    async def set(self, **fields: Any) -> None:
        clean = {k: v for k, v in fields.items() if v is not None}
        if not clean:
            return
        async with self._lock:
            self._state.update(clean)
            patch = dict(self._state)
        await report_progress(**patch)

    async def bump(self, **deltas: int) -> None:
        clean = {k: int(v) for k, v in deltas.items() if v}
        if not clean:
            return
        async with self._lock:
            for key, delta in clean.items():
                prev = self._state.get(key, 0)
                self._state[key] = (prev if isinstance(prev, int) else 0) + delta
            patch = dict(self._state)
        await report_progress(**patch)

    async def event(self, code: str, message: str, *, warning: bool = True) -> None:
        event = make_event(code, message)
        async with self._lock:
            self._state["last_event"] = event
            self._state["detail"] = message
            if warning:
                self._state["warnings"] = append_warning(self._state.get("warnings"), message)
            patch = dict(self._state)
        await report_progress(**patch)

    async def finish_peer(
        self,
        *,
        external_id: str,
        messages: int = 0,
        status: str = "done",
    ) -> None:
        async with self._lock:
            self._state["finished_peers"] = append_finished_peer(
                self._state.get("finished_peers"),
                external_id=external_id,
                messages=messages,
                status=status,
            )
            patch = dict(self._state)
        await report_progress(**patch)

    async def snapshot(self, **fields: Any) -> None:
        """Merge fields and flush immediately (end of run / phase change)."""
        clean = {k: v for k, v in fields.items() if v is not None}
        async with self._lock:
            if clean:
                self._state.update(clean)
            patch = dict(self._state)
        await flush_progress(**patch)
