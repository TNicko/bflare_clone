"""Phase 3b: Parquet + overlay hydrate and edit_date overlay gate."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from msgstore import (
    LocalSink,
    MessageShardWriter,
    get_overlay,
    hydrate_message_rows,
    hydrate_message_rows_async,
    needs_body_hydrate,
    overlay_payload_from_message_row,
    put_overlay,
    reset_message_archive_sink_for_tests,
)
from db.crud.telegram.message import _needs_cold_overlay
from db.schemas.telegram.message import TelegramMessageSchema


def test_needs_body_hydrate() -> None:
    assert not needs_body_hydrate({"content": "hi", "media": {"tl": "MessageMediaPhoto"}, "archive_key": "k"})
    assert not needs_body_hydrate({"content": None, "archive_key": None})
    assert needs_body_hydrate({"content": None, "media": {}, "archive_key": "k", "archive_row": 0})
    # Harvest stub alone must still hydrate TL media from Parquet.
    assert needs_body_hydrate(
        {
            "content": None,
            "media": {"harvest": {"s3_uri": "s3://bucket/x"}},
            "archive_key": "k",
            "archive_row": 0,
        }
    )


def test_hydrate_from_parquet(tmp_path: Path) -> None:
    sink = LocalSink(tmp_path)
    writer = MessageShardWriter(sink, prefix="messages/telegram", max_rows_per_shard=100)
    mid = uuid4()
    d0 = datetime(2024, 7, 1, 12, 0, tzinfo=timezone.utc)
    writer.add_row(
        {
            "id": mid,
            "message_id": 1,
            "source_external_id": "channel:1",
            "peer_type": "channel",
            "message_type": "message",
            "content": "from-parquet",
            "content_hash": "h",
            "out": False,
            "date": d0,
            "edit_date": None,
            "created_at": d0,
            "updated_at": d0,
            "reactions": [],
            "views": 1,
            "media": {"kind": "photo"},
            "pinned": False,
            "deleted": False,
            "silent": False,
            "mentioned": False,
            "grouped_id": None,
            "action_type": None,
            "action_data": None,
            "metadata": None,
            "fwd_from": None,
            "reply_to": None,
            "from_external_id": None,
            "harvester_external_id": "user:1",
        }
    )
    flushed = writer.flush()
    key, _n, _batch = flushed[0]

    rows = [
        {
            "id": mid,
            "content": None,
            "media": None,
            "archive_key": key,
            "archive_row": 0,
        }
    ]
    assert hydrate_message_rows(rows, sink, prefix="messages/telegram") >= 1
    assert rows[0]["content"] == "from-parquet"
    assert rows[0]["media"] == {"kind": "photo"}


def test_overlay_wins_over_parquet(tmp_path: Path) -> None:
    sink = LocalSink(tmp_path)
    writer = MessageShardWriter(sink, prefix="messages/telegram", max_rows_per_shard=100)
    mid = uuid4()
    d0 = datetime(2024, 7, 1, 12, 0, tzinfo=timezone.utc)
    writer.add_row(
        {
            "id": mid,
            "message_id": 1,
            "source_external_id": "channel:1",
            "peer_type": "channel",
            "message_type": "message",
            "content": "old",
            "content_hash": None,
            "out": False,
            "date": d0,
            "edit_date": None,
            "created_at": d0,
            "updated_at": d0,
            "reactions": [],
            "views": 0,
            "media": {},
            "pinned": False,
            "deleted": False,
            "silent": False,
            "mentioned": False,
            "grouped_id": None,
            "action_type": None,
            "action_data": None,
            "metadata": None,
            "fwd_from": None,
            "reply_to": None,
            "from_external_id": None,
            "harvester_external_id": None,
        }
    )
    key, _, _ = writer.flush()[0]
    put_overlay(
        sink,
        str(mid),
        {"content": "edited", "edit_date": d0.isoformat(), "deleted": False},
        prefix="messages/telegram",
    )
    assert get_overlay(sink, str(mid), prefix="messages/telegram")["content"] == "edited"

    rows = [{"id": mid, "content": None, "media": None, "archive_key": key, "archive_row": 0}]
    hydrate_message_rows(rows, sink, prefix="messages/telegram")
    assert rows[0]["content"] == "edited"


def test_needs_cold_overlay_edit_date_gate() -> None:
    msg = TelegramMessageSchema(
        message_id=1,
        source_external_id="channel:1",
        peer_type="channel",
        content="x",
        date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        edit_date=datetime(2024, 2, 1, tzinfo=timezone.utc),
    )
    existing = {
        "archive_key": "messages/telegram/dt=2024-01/part-x.parquet",
        "has_content": False,
        "edit_date": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "deleted": False,
        "id": uuid4(),
    }
    assert _needs_cold_overlay(msg, existing)
    existing_same = {**existing, "edit_date": msg.edit_date}
    assert not _needs_cold_overlay(msg, existing_same)
    existing_fat = {**existing, "has_content": True}
    assert not _needs_cold_overlay(msg, existing_fat)


def test_needs_cold_overlay_web_to_mtproto_upgrade() -> None:
    msg = TelegramMessageSchema(
        message_id=1,
        source_external_id="-1001",
        peer_type="channel",
        content="mtproto body",
        date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        harvester_external_id="h1",
    )
    existing = {
        "archive_key": "messages/telegram/dt=2024-01/part-x.parquet",
        "has_content": False,
        "edit_date": None,
        "deleted": False,
        "harvester_external_id": "web_tme",
        "id": uuid4(),
    }
    assert _needs_cold_overlay(msg, existing)
    web_again = msg.model_copy(update={"harvester_external_id": "web_tme", "content": "web"})
    assert not _needs_cold_overlay(web_again, existing)


def test_overlay_payload_roundtrip() -> None:
    reset_message_archive_sink_for_tests()
    payload = overlay_payload_from_message_row({"id": "a", "content": "hi", "deleted": False})
    assert payload["content"] == "hi"


def test_hydrate_merges_pg_harvest_stub(tmp_path: Path) -> None:
    sink = LocalSink(tmp_path)
    writer = MessageShardWriter(sink, prefix="messages/telegram", max_rows_per_shard=100)
    mid = uuid4()
    d0 = datetime(2024, 7, 1, 12, 0, tzinfo=timezone.utc)
    writer.add_row(
        {
            "id": mid,
            "message_id": 1,
            "source_external_id": "channel:1",
            "peer_type": "channel",
            "message_type": "message",
            "content": "caption",
            "content_hash": "h",
            "out": False,
            "date": d0,
            "edit_date": None,
            "created_at": d0,
            "updated_at": d0,
            "reactions": [],
            "views": 1,
            "media": {"tl": "MessageMediaDocument", "mime_type": "application/pdf", "filename": "a.pdf"},
            "pinned": False,
            "deleted": False,
            "silent": False,
            "mentioned": False,
            "grouped_id": None,
            "action_type": None,
            "action_data": None,
            "metadata": None,
            "fwd_from": None,
            "reply_to": None,
            "from_external_id": None,
            "harvester_external_id": "user:1",
        }
    )
    key, _, _ = writer.flush()[0]
    rows = [
        {
            "id": mid,
            "content": None,
            "media": {"harvest": {"s3_uri": "s3://bucket/a.pdf", "size_bytes": 12}},
            "archive_key": key,
            "archive_row": 0,
        }
    ]
    assert hydrate_message_rows(rows, sink, prefix="messages/telegram") >= 1
    assert rows[0]["content"] == "caption"
    assert rows[0]["media"]["tl"] == "MessageMediaDocument"
    assert rows[0]["media"]["filename"] == "a.pdf"
    assert rows[0]["media"]["harvest"]["s3_uri"] == "s3://bucket/a.pdf"
    assert rows[0]["media"]["harvest"]["size_bytes"] == 12


def test_hydrate_message_rows_async_empty() -> None:
    assert asyncio.run(hydrate_message_rows_async([], None)) == 0
