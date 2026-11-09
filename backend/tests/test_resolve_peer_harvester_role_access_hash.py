"""Foreign access_hash must not be used when resolving for another harvester."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from jobs.fetch_messages.db import PeerSelectionRow
from jobs.fetch_messages.resolve import resolve_peer_harvester_role


def test_resolve_ignores_foreign_access_hash_and_falls_back_to_username(
    monkeypatch,
) -> None:
    owner = uuid4()
    other = uuid4()
    peer_id = "-1002224039481"
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=AssertionError("must not use foreign access_hash"))

    fetch_username = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "jobs.fetch_messages.resolve.fetch_peer_username",
        fetch_username,
    )

    row = PeerSelectionRow(
        peer_external_id=peer_id,
        peer_type="channel",
        access_hash=-2479001084381027103,
        harvester_id=owner,
        harvester_external_id="111",
    )
    result = asyncio.run(
        resolve_peer_harvester_role(
            client,
            MagicMock(),
            row=row,
            harvester_id=other,
            harvester_external_id="222",
        )
    )
    assert result is None
    fetch_username.assert_awaited_once()
    client.get_entity.assert_not_awaited()


def test_resolve_ignores_hash_when_harvester_id_missing(monkeypatch) -> None:
    harvester_id = uuid4()
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=AssertionError("ambiguous hash must not be used"))
    monkeypatch.setattr(
        "jobs.fetch_messages.resolve.fetch_peer_username",
        AsyncMock(return_value=None),
    )

    row = PeerSelectionRow(
        peer_external_id="-1002224039481",
        peer_type="channel",
        access_hash=-2479001084381027103,
        harvester_id=None,
        harvester_external_id=None,
    )
    result = asyncio.run(
        resolve_peer_harvester_role(
            client,
            MagicMock(),
            row=row,
            harvester_id=harvester_id,
            harvester_external_id="222",
        )
    )
    assert result is None
    client.get_entity.assert_not_awaited()
