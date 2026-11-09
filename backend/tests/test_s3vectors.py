"""Unit tests for ``s3vectors`` filter/similarity helpers (no AWS calls)."""

from __future__ import annotations

from typing import Any

import pytest

from s3vectors import (
    IMAGES_DIM,
    MESSAGES_DIM,
    META_DATE,
    META_PEER_TYPE,
    META_S3_KEY,
    META_SOURCE_EXTERNAL_ID,
    build_metadata_filter,
    cosine_similarity_from_distance,
    image_vector_put_item,
    message_vector_put_item,
    search_image_vectors,
    search_message_vectors,
)


def test_cosine_similarity_from_distance() -> None:
    assert cosine_similarity_from_distance(None) is None
    assert cosine_similarity_from_distance(0.0) == 1.0
    assert cosine_similarity_from_distance(0.25) == 0.75
    assert cosine_similarity_from_distance(-0.1) == 1.0
    assert cosine_similarity_from_distance(1.5) == 0.0


def test_build_metadata_filter_empty() -> None:
    assert build_metadata_filter() is None


def test_build_metadata_filter_single_and_combined() -> None:
    only_source = build_metadata_filter(source_external_id="12345")
    assert only_source == {META_SOURCE_EXTERNAL_ID: {"$eq": "12345"}}

    combined = build_metadata_filter(
        source_external_id="12345",
        peer_type="channel",
        date_from_epoch_ms=1_700_000_000_000,
        date_to_epoch_ms=1_800_000_000_000,
    )
    assert combined == {
        "$and": [
            {META_SOURCE_EXTERNAL_ID: {"$eq": "12345"}},
            {META_PEER_TYPE: {"$eq": "channel"}},
            {META_DATE: {"$gte": 1_700_000_000_000, "$lte": 1_800_000_000_000}},
        ]
    }


def test_message_vector_put_item_rejects_wrong_dim() -> None:
    with pytest.raises(ValueError, match="vector dim"):
        message_vector_put_item(
            id="abc",
            vector=[0.0, 1.0],
            date_epoch_ms=1,
        )


def test_message_vector_put_item_shape() -> None:
    vec = [0.1] * MESSAGES_DIM
    item = message_vector_put_item(
        id="msg-uuid",
        vector=vec,
        date_epoch_ms=1_700_000_000_000,
        source_external_id="99",
        peer_type="user",
        message_id=42,
    )
    assert item["key"] == "msg-uuid"
    assert len(item["data"]["float32"]) == MESSAGES_DIM
    assert item["metadata"][META_DATE] == 1_700_000_000_000
    assert item["metadata"][META_SOURCE_EXTERNAL_ID] == "99"
    assert item["metadata"][META_PEER_TYPE] == "user"
    assert item["metadata"]["message_id"] == 42


class _FakeS3VectorsClient:
    def __init__(self, vectors: list[dict[str, Any]]) -> None:
        self._vectors = vectors
        self.last_kwargs: dict[str, Any] | None = None

    def query_vectors(self, **kwargs: Any) -> dict[str, Any]:
        self.last_kwargs = kwargs
        top_k = int(kwargs["topK"])
        return {"vectors": self._vectors[:top_k]}


def test_search_message_vectors_paginates_and_maps_hits() -> None:
    raw = [
        {
            "key": f"id-{i}",
            "distance": 0.1 * i,
            "metadata": {
                META_DATE: 1_700_000_000_000 + i,
                META_SOURCE_EXTERNAL_ID: "s",
                META_PEER_TYPE: "channel",
            },
        }
        for i in range(5)
    ]
    client = _FakeS3VectorsClient(raw)
    query = [0.0] * MESSAGES_DIM

    page = search_message_vectors(client, query, limit=2, offset=1)
    assert client.last_kwargs is not None
    assert client.last_kwargs["topK"] == 3
    assert [h["id"] for h in page] == ["id-1", "id-2"]
    assert page[0]["similarity"] == pytest.approx(0.9)
    assert page[0]["date"] == 1_700_000_000_001
    assert page[0]["source_external_id"] == "s"
    assert page[0]["peer_type"] == "channel"


def test_search_message_vectors_passes_filter() -> None:
    client = _FakeS3VectorsClient([])
    query = [0.0] * MESSAGES_DIM
    search_message_vectors(
        client,
        query,
        limit=10,
        date_from_epoch_ms=100,
        date_to_epoch_ms=200,
        source_external_id="ext",
    )
    assert client.last_kwargs is not None
    assert client.last_kwargs["filter"] == {
        "$and": [
            {META_SOURCE_EXTERNAL_ID: {"$eq": "ext"}},
            {META_DATE: {"$gte": 100, "$lte": 200}},
        ]
    }


class _FakeDeleteClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def delete_vectors(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {}


def test_delete_message_vectors_batches() -> None:
    from s3vectors import delete_message_vectors

    client = _FakeDeleteClient()
    keys = [f"id-{i}" for i in range(12)]
    n = delete_message_vectors(client, keys, batch_size=5)
    assert n == 12
    assert len(client.calls) == 3
    assert client.calls[0]["keys"] == keys[:5]
    assert client.calls[2]["keys"] == keys[10:]


def test_dedupe_put_items_last_wins() -> None:
    from s3vectors import dedupe_put_items

    items = [
        {"key": "aa", "data": {"float32": [1.0]}},
        {"key": "bb", "data": {"float32": [2.0]}},
        {"key": "aa", "data": {"float32": [3.0]}},
    ]
    out = dedupe_put_items(items)
    by_key = {r["key"]: r for r in out}
    assert set(by_key) == {"aa", "bb"}
    assert by_key["aa"]["data"]["float32"] == [3.0]


class _FakePutClient:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def put_vectors(self, **kwargs: Any) -> dict[str, Any]:
        keys = [str(v["key"]) for v in kwargs["vectors"]]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate keys")
        self.calls.append(keys)
        return {}


def test_put_image_vectors_dedupes_and_batches() -> None:
    from s3vectors import put_image_vectors

    client = _FakePutClient()
    vec = [0.0] * IMAGES_DIM
    items = [
        image_vector_put_item(id="0123456789abcdef", vector=vec, date_epoch_ms=1, s3_key="a"),
        image_vector_put_item(id="0123456789abcdef", vector=vec, date_epoch_ms=2, s3_key="b"),
        image_vector_put_item(id="fedcba9876543210", vector=vec, date_epoch_ms=3, s3_key="c"),
    ]
    n = put_image_vectors(client, items, batch_size=10, concurrency=1)
    assert n == 2
    assert len(client.calls) == 1
    assert set(client.calls[0]) == {"0123456789abcdef", "fedcba9876543210"}


def test_image_vector_put_item_rejects_wrong_dim() -> None:
    with pytest.raises(ValueError, match="vector dim"):
        image_vector_put_item(
            id="0123456789abcdef",
            vector=[0.0, 1.0],
            date_epoch_ms=1,
            s3_key="media/x.jpg",
        )


def test_image_vector_put_item_shape() -> None:
    vec = [0.1] * IMAGES_DIM
    item = image_vector_put_item(
        id="0123456789ABCDEF",
        vector=vec,
        date_epoch_ms=1_700_000_000_000,
        s3_key="media/abc.jpg",
        source_external_id="99",
        peer_type="channel",
    )
    assert item["key"] == "0123456789abcdef"
    assert len(item["data"]["float32"]) == IMAGES_DIM
    assert item["metadata"][META_DATE] == 1_700_000_000_000
    assert item["metadata"][META_S3_KEY] == "media/abc.jpg"
    assert item["metadata"][META_SOURCE_EXTERNAL_ID] == "99"
    assert item["metadata"][META_PEER_TYPE] == "channel"
    assert item["metadata"]["msg_id"] == "0123456789abcdef"


def test_is_transient_s3vectors_error() -> None:
    from s3vectors.client import _is_transient_s3vectors_error

    class ConnectionClosedError(Exception):
        pass

    assert _is_transient_s3vectors_error(ConnectionClosedError("Connection was closed before…"))
    assert _is_transient_s3vectors_error(RuntimeError("connection was closed before we received"))
    assert not _is_transient_s3vectors_error(ValueError("vector dim wrong"))


def test_search_image_vectors_maps_hits() -> None:
    raw = [
        {
            "key": "0123456789abcdef",
            "distance": 0.2,
            "metadata": {
                META_DATE: 1_700_000_000_000,
                META_SOURCE_EXTERNAL_ID: "s",
                META_PEER_TYPE: "channel",
                META_S3_KEY: "media/hit.jpg",
                "msg_id": "0123456789abcdef",
            },
        }
    ]
    client = _FakeS3VectorsClient(raw)
    page = search_image_vectors(client, [0.0] * IMAGES_DIM, limit=5)
    assert len(page) == 1
    assert page[0]["id"] == "0123456789abcdef"
    assert page[0]["s3_key"] == "media/hit.jpg"
    assert page[0]["msg_id"] == "0123456789abcdef"
    assert page[0]["similarity"] == pytest.approx(0.8)
