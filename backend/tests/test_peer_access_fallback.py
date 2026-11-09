"""Alternate harvester access hashes are used when the preferred session is down."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

from services.api.src.peer_access import connected_peer_role


def test_connected_peer_role_skips_unauthorized_harvester(monkeypatch) -> None:
    dead = uuid4()
    live = uuid4()
    live_client = object()
    pool = AsyncMock()

    async def get_client(_conn, harvester_id):
        if harvester_id == dead:
            return None
        if harvester_id == live:
            return live_client
        return None

    pool.get_client = AsyncMock(side_effect=get_client)

    async def candidates(_conn, _external_id):
        return [
            (dead, 111, "dead-account"),
            (live, 222, "live-account"),
        ]

    monkeypatch.setattr(
        "services.api.src.peer_access.list_peer_access_candidates",
        candidates,
    )

    result = asyncio.run(
        connected_peer_role(
            pool,
            object(),
            peer_external_id="-1002380670959",
            skip_harvester_ids=set(),
        )
    )
    assert result is not None
    client, hid, access_hash, external_id = result
    assert client is live_client
    assert hid == live
    assert access_hash == 222
    assert external_id == "live-account"
    assert pool.get_client.await_count == 2
