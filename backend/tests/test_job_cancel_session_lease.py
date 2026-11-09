"""Cancel must unwind session leases instead of leaving sessions active."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from jobs.progress import session_leases_from_progress
from utils.graceful_stop import cancel_all_asyncio_tasks


def test_session_leases_from_progress_parses_unique_pairs() -> None:
    sid = uuid4()
    parsed = session_leases_from_progress(
        {
            "session_leases": [
                {"session_id": str(sid), "server_id": "fwd:pi:1"},
                {"session_id": str(sid), "server_id": "fwd:pi:1"},
                {"session_id": "nope", "server_id": "fwd:pi:1"},
                {"session_id": str(sid)},
            ]
        }
    )
    assert parsed == [(sid, "fwd:pi:1")]


def test_session_leases_from_progress_ignores_junk() -> None:
    assert session_leases_from_progress(None) == []
    assert session_leases_from_progress({"session_leases": "x"}) == []
    assert session_leases_from_progress({"session_leases": [1, "a"]}) == []


def test_cancel_all_asyncio_tasks_unwinds_finally() -> None:
    async def _main() -> None:
        released = asyncio.Event()

        async def _hold() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                released.set()

        child = asyncio.create_task(_hold())
        await asyncio.sleep(0)
        n = cancel_all_asyncio_tasks(exclude=asyncio.current_task())
        assert n >= 1
        with pytest.raises(asyncio.CancelledError):
            await child
        assert released.is_set()

    asyncio.run(_main())
