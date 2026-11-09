"""Unit tests for msgstore Parquet writer (local sink, no AWS)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq

from msgstore import LocalSink, MessageShardWriter, month_from_shard_key, overlay_key


def test_message_shard_writer_roundtrip(tmp_path: Path) -> None:
    sink = LocalSink(tmp_path)
    writer = MessageShardWriter(sink, prefix="messages/telegram", max_rows_per_shard=2, auto_flush=False)
    d0 = datetime(2024, 7, 1, 12, 0, tzinfo=timezone.utc)
    d1 = datetime(2024, 7, 2, 12, 0, tzinfo=timezone.utc)
    rows = [
        {
            "id": uuid4(),
            "message_id": 1,
            "source_external_id": "channel:1",
            "peer_type": "channel",
            "message_type": "message",
            "content": "hello",
            "content_hash": "abc",
            "out": False,
            "date": d0,
            "edit_date": None,
            "created_at": d0,
            "updated_at": d0,
            "reactions": [],
            "views": 3,
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
            "from_external_id": "user:9",
            "harvester_external_id": "user:1",
        },
        {
            "id": uuid4(),
            "message_id": 2,
            "source_external_id": "channel:1",
            "peer_type": "channel",
            "message_type": "message",
            "content": "world",
            "content_hash": "def",
            "out": False,
            "date": d1,
            "edit_date": None,
            "created_at": d1,
            "updated_at": d1,
            "reactions": [{"emoji": "👍"}],
            "views": 1,
            "media": {"kind": "photo"},
            "pinned": False,
            "deleted": True,
            "silent": False,
            "mentioned": False,
            "grouped_id": 99,
            "action_type": None,
            "action_data": None,
            "metadata": {"x": 1},
            "fwd_from": {"channel_id": 1},
            "reply_to": {"msg_id": 1},
            "from_external_id": None,
            "harvester_external_id": "user:1",
        },
    ]
    for row in rows:
        writer.add_row(row)
    flushed = writer.flush()
    assert len(flushed) == 1
    key, n, batch = flushed[0]
    assert n == 2
    assert month_from_shard_key(key) == "2024-07"
    assert key.startswith("messages/telegram/dt=2024-07/part-")
    assert len(batch) == 2
    table = pq.read_table(tmp_path / key)
    assert table.num_rows == 2
    assert table.column("content").to_pylist() == ["hello", "world"]
    assert table.column("deleted").to_pylist() == [False, True]


def test_overlay_key() -> None:
    assert overlay_key("AbC") == "messages/telegram/overlays/abc.json.zst"
