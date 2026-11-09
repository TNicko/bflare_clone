"""Unit tests for Postgres pHash Hamming helpers (no live DB)."""

from __future__ import annotations

import asyncio
from typing import Any

from db.crud.telegram.image_blobs import (
    ZERO_PHASH,
    normalize_phash_hex,
    s3_key_from_uri,
    search_near_duplicate_phashes,
)
from phash.core import hamming_distance, hex_to_int


def test_s3_key_from_uri() -> None:
    assert s3_key_from_uri("s3://messenger-harvest/path/to/img.jpg") == "path/to/img.jpg"
    assert s3_key_from_uri("s3://bucket-only") == ""
    assert s3_key_from_uri(None) == ""
    assert s3_key_from_uri("not-a-uri") == "not-a-uri"


def test_normalize_phash_hex() -> None:
    assert normalize_phash_hex("0123456789ABCDEF") == "0123456789abcdef"
    assert normalize_phash_hex("short") is None
    assert normalize_phash_hex(ZERO_PHASH) == ZERO_PHASH


class _FakeCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.execute_args: tuple[Any, ...] | None = None

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None

    async def execute(self, query: str, params: Any = None) -> None:
        self.execute_args = (query, params)

    async def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)


class _FakeConn:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.cursor_obj = _FakeCursor(rows)

    def cursor(self, **kwargs: Any) -> _FakeCursor:
        return self.cursor_obj


def test_search_near_duplicate_phashes_maps_rows() -> None:
    query = "0123456789abcdef"
    near = "0123456789abcdee"
    rows = [
        {
            "phash": query,
            "canonical_s3_uri": "s3://messenger-harvest/a.jpg",
            "distance": 0,
        },
        {
            "phash": near,
            "canonical_s3_uri": "s3://messenger-harvest/b.jpg",
            "distance": 1,
        },
    ]
    conn = _FakeConn(rows)

    async def _run() -> list[dict[str, Any]]:
        return await search_near_duplicate_phashes(conn, query, max_hamming=5, limit=10)

    hits = asyncio.run(_run())
    assert len(hits) == 2
    assert hits[0]["phash"] == query
    assert hits[0]["distance"] == 0.0
    assert hits[0]["s3_key"] == "a.jpg"
    assert hits[1]["s3_key"] == "b.jpg"
    assert conn.cursor_obj.execute_args is not None
    _q, params = conn.cursor_obj.execute_args
    assert params[0] == query
    assert params[1] == ZERO_PHASH
    assert params[3] == 5
    assert params[4] == 10


def test_search_near_duplicate_phashes_rejects_bad_query() -> None:
    conn = _FakeConn([])

    async def _run() -> None:
        assert await search_near_duplicate_phashes(conn, "nope") == []
        assert await search_near_duplicate_phashes(conn, ZERO_PHASH) == []

    asyncio.run(_run())


def test_hamming_distance_matches_core() -> None:
    a = "0123456789abcdef"
    b = "0123456789abcdee"
    assert hamming_distance(hex_to_int(a), hex_to_int(b)) >= 0
