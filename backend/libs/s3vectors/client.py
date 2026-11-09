"""Amazon S3 Vectors client for message + image semantic/ANN search.

Derived ANN indexes: durable vectors stay in the Parquet embstore; this module upserts
and queries S3 Vectors. Hit dict shapes match ``milvus.client.search_*_vectors`` so
API search modules can swap backends without hydration changes.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, TypeVar

from s3vectors.constants import (
    DEFAULT_IMAGES_INDEX_NAME,
    DEFAULT_INDEX_NAME,
    DEFAULT_PUT_BATCH_SIZE,
    DEFAULT_PUT_CONCURRENCY,
    DEFAULT_VECTOR_BUCKET,
    IMAGES_DIM,
    MESSAGES_DIM,
    META_DATE,
    META_MESSAGE_ID,
    META_MSG_ID,
    META_PEER_TYPE,
    META_S3_KEY,
    META_SOURCE_EXTERNAL_ID,
)

lg = logging.getLogger(__name__)

_T = TypeVar("_T")

#: Transient network / service blips on long PutVectors backfills.
_PUT_MAX_ATTEMPTS = 8
_PUT_BASE_SLEEP_S = 1.5


def get_client(*, region: str | None = None) -> Any:
    """Build a boto3 ``s3vectors`` client (requires boto3 with S3 Vectors support)."""
    import boto3
    from botocore.config import Config as BotoConfig

    resolved = (region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "").strip()
    if not resolved:
        raise ValueError("AWS region required for S3 Vectors (set AWS_REGION)")
    return boto3.client(
        "s3vectors",
        region_name=resolved,
        config=BotoConfig(
            retries={"max_attempts": 10, "mode": "adaptive"},
            connect_timeout=60,
            read_timeout=300,
        ),
    )


def _is_transient_s3vectors_error(exc: BaseException) -> bool:
    name = type(exc).__name__
    if name in {
        "ConnectionClosedError",
        "EndpointConnectionError",
        "ConnectTimeoutError",
        "ReadTimeoutError",
        "ResponseStreamingError",
        "IncompleteReadError",
    }:
        return True
    # botocore ClientError — throttle / 5xx
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping):
        meta = response.get("ResponseMetadata") if isinstance(response.get("ResponseMetadata"), Mapping) else {}
        status = meta.get("HTTPStatusCode")
        if isinstance(status, int) and (status == 429 or status >= 500):
            return True
        err = response.get("Error") if isinstance(response.get("Error"), Mapping) else {}
        code = str(err.get("Code") or "")
        if code in {"ThrottlingException", "TooManyRequestsException", "RequestTimeout", "ServiceUnavailable"}:
            return True
    msg = str(exc).lower()
    return "connection was closed" in msg or "temporarily unavailable" in msg


def _call_with_retries(op: Callable[[], _T], *, label: str) -> _T:
    last: BaseException | None = None
    for attempt in range(1, _PUT_MAX_ATTEMPTS + 1):
        try:
            return op()
        except Exception as exc:
            last = exc
            if not _is_transient_s3vectors_error(exc) or attempt >= _PUT_MAX_ATTEMPTS:
                raise
            sleep_s = min(60.0, _PUT_BASE_SLEEP_S * (2 ** (attempt - 1)))
            lg.warning(
                "s3vectors %s transient error (attempt %s/%s), retry in %.1fs: %s",
                label,
                attempt,
                _PUT_MAX_ATTEMPTS,
                sleep_s,
                exc,
            )
            time.sleep(sleep_s)
    assert last is not None
    raise last


def cosine_similarity_from_distance(distance: float | None) -> float | None:
    """Map S3 Vectors cosine ``distance`` to similarity (higher = closer).

    For the cosine metric, AWS returns a distance where identical vectors are near 0
    (``1 - cosine_similarity`` on unit vectors). Clamp to ``[0, 1]``.
    """
    if distance is None:
        return None
    sim = 1.0 - float(distance)
    if sim < 0.0:
        return 0.0
    if sim > 1.0:
        return 1.0
    return sim


def build_metadata_filter(
    *,
    source_external_id: str | None = None,
    peer_type: str | None = None,
    date_from_epoch_ms: int | None = None,
    date_to_epoch_ms: int | None = None,
) -> dict[str, Any] | None:
    """Build a QueryVectors metadata filter (Mongo-style operators)."""
    clauses: list[dict[str, Any]] = []
    if source_external_id:
        clauses.append({META_SOURCE_EXTERNAL_ID: {"$eq": str(source_external_id)}})
    if peer_type:
        clauses.append({META_PEER_TYPE: {"$eq": str(peer_type)}})
    if date_from_epoch_ms is not None or date_to_epoch_ms is not None:
        date_clause: dict[str, Any] = {}
        if date_from_epoch_ms is not None:
            date_clause["$gte"] = int(date_from_epoch_ms)
        if date_to_epoch_ms is not None:
            date_clause["$lte"] = int(date_to_epoch_ms)
        clauses.append({META_DATE: date_clause})
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return {"$and": clauses}


def message_vector_put_item(
    *,
    id: str,
    vector: Sequence[float],
    date_epoch_ms: int,
    source_external_id: str | None = None,
    peer_type: str | None = None,
    message_id: int | None = None,
) -> dict[str, Any]:
    """One ``PutVectors`` element (key + float32 data + metadata)."""
    if len(vector) != MESSAGES_DIM:
        raise ValueError(f"vector dim {len(vector)} != expected {MESSAGES_DIM} for id={id}")
    metadata: dict[str, Any] = {
        META_DATE: int(date_epoch_ms),
        META_SOURCE_EXTERNAL_ID: str(source_external_id) if source_external_id is not None else "",
        META_PEER_TYPE: str(peer_type) if peer_type is not None else "",
    }
    if message_id is not None:
        metadata[META_MESSAGE_ID] = int(message_id)
    return {
        "key": str(id),
        "data": {"float32": [float(x) for x in vector]},
        "metadata": metadata,
    }


def image_vector_put_item(
    *,
    id: str,
    vector: Sequence[float],
    date_epoch_ms: int,
    s3_key: str,
    msg_id: str | None = None,
    source_external_id: str | None = None,
    peer_type: str | None = None,
    message_id: int | None = None,
) -> dict[str, Any]:
    """One image ``PutVectors`` element. ``id`` is the exact 16-hex pHash."""
    if len(vector) != IMAGES_DIM:
        raise ValueError(f"vector dim {len(vector)} != expected {IMAGES_DIM} for id={id}")
    phash = str(id).strip().lower()
    metadata: dict[str, Any] = {
        META_DATE: int(date_epoch_ms),
        META_SOURCE_EXTERNAL_ID: str(source_external_id) if source_external_id is not None else "",
        META_PEER_TYPE: str(peer_type) if peer_type is not None else "",
        META_S3_KEY: str(s3_key),
        META_MSG_ID: str(msg_id) if msg_id is not None else phash,
    }
    if message_id is not None:
        metadata[META_MESSAGE_ID] = int(message_id)
    return {
        "key": phash,
        "data": {"float32": [float(x) for x in vector]},
        "metadata": metadata,
    }


def dedupe_put_items(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Collapse duplicate ``key`` values (last wins). AWS rejects dupes in one PutVectors call."""
    by_key: dict[str, dict[str, Any]] = {}
    for row in items:
        key = str(row.get("key") or "").strip()
        if not key:
            continue
        by_key[key] = dict(row)
    return list(by_key.values())


def _put_vectors(
    client: Any,
    items: Sequence[Mapping[str, Any]],
    *,
    vector_bucket_name: str,
    index_name: str,
    batch_size: int,
    log_label: str,
    concurrency: int = DEFAULT_PUT_CONCURRENCY,
) -> int:
    rows = dedupe_put_items(items)
    if not rows:
        return 0
    size = max(1, min(int(batch_size), DEFAULT_PUT_BATCH_SIZE))
    chunks = [rows[start : start + size] for start in range(0, len(rows), size)]
    workers = max(1, min(int(concurrency), len(chunks)))

    def _put_one(chunk: list[dict[str, Any]], chunk_idx: int) -> int:
        def _put() -> None:
            client.put_vectors(
                vectorBucketName=vector_bucket_name,
                indexName=index_name,
                vectors=chunk,
            )

        _call_with_retries(_put, label=f"{log_label}.put[{chunk_idx}]")
        return len(chunk)

    total = 0
    t0 = time.monotonic()
    if workers == 1:
        for i, chunk in enumerate(chunks):
            total += _put_one(chunk, i)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_put_one, chunk, i) for i, chunk in enumerate(chunks)]
            for fut in as_completed(futures):
                total += fut.result()
    elapsed = max(1e-6, time.monotonic() - t0)
    lg.info(
        "s3vectors %s bucket=%s index=%s count=%s chunks=%s workers=%s %.0f vec/s",
        log_label,
        vector_bucket_name,
        index_name,
        total,
        len(chunks),
        workers,
        total / elapsed,
    )
    return total


def _delete_vectors(
    client: Any,
    keys: Sequence[str],
    *,
    vector_bucket_name: str,
    index_name: str,
    batch_size: int,
    log_label: str,
) -> int:
    ids = [str(k) for k in keys if str(k).strip()]
    if not ids:
        return 0
    size = max(1, min(int(batch_size), 500))
    total = 0
    for start in range(0, len(ids), size):
        chunk = ids[start : start + size]

        def _delete(chunk: list[str] = chunk) -> None:
            client.delete_vectors(
                vectorBucketName=vector_bucket_name,
                indexName=index_name,
                keys=chunk,
            )

        _call_with_retries(_delete, label=f"{log_label}.delete")
        total += len(chunk)
    lg.info(
        "s3vectors %s bucket=%s index=%s count=%s",
        log_label,
        vector_bucket_name,
        index_name,
        total,
    )
    return total


def put_message_vectors(
    client: Any,
    items: Sequence[Mapping[str, Any]],
    *,
    vector_bucket_name: str = DEFAULT_VECTOR_BUCKET,
    index_name: str = DEFAULT_INDEX_NAME,
    batch_size: int = DEFAULT_PUT_BATCH_SIZE,
    concurrency: int = DEFAULT_PUT_CONCURRENCY,
) -> int:
    """Upsert message vectors. ``items`` are outputs of :func:`message_vector_put_item`.

    Returns the number of unique vectors submitted (duplicate keys collapsed).
    """
    return _put_vectors(
        client,
        items,
        vector_bucket_name=vector_bucket_name,
        index_name=index_name,
        batch_size=batch_size,
        log_label="put_message_vectors",
        concurrency=concurrency,
    )


def put_image_vectors(
    client: Any,
    items: Sequence[Mapping[str, Any]],
    *,
    vector_bucket_name: str = DEFAULT_VECTOR_BUCKET,
    index_name: str = DEFAULT_IMAGES_INDEX_NAME,
    batch_size: int = DEFAULT_PUT_BATCH_SIZE,
    concurrency: int = DEFAULT_PUT_CONCURRENCY,
) -> int:
    """Upsert image vectors. ``items`` are outputs of :func:`image_vector_put_item`."""
    return _put_vectors(
        client,
        items,
        vector_bucket_name=vector_bucket_name,
        index_name=index_name,
        batch_size=batch_size,
        log_label="put_image_vectors",
        concurrency=concurrency,
    )


def delete_message_vectors(
    client: Any,
    keys: Sequence[str],
    *,
    vector_bucket_name: str = DEFAULT_VECTOR_BUCKET,
    index_name: str = DEFAULT_INDEX_NAME,
    batch_size: int = DEFAULT_PUT_BATCH_SIZE,
) -> int:
    """Delete message vectors by key. Returns the number of keys submitted."""
    return _delete_vectors(
        client,
        keys,
        vector_bucket_name=vector_bucket_name,
        index_name=index_name,
        batch_size=batch_size,
        log_label="delete_message_vectors",
    )


def delete_image_vectors(
    client: Any,
    keys: Sequence[str],
    *,
    vector_bucket_name: str = DEFAULT_VECTOR_BUCKET,
    index_name: str = DEFAULT_IMAGES_INDEX_NAME,
    batch_size: int = DEFAULT_PUT_BATCH_SIZE,
) -> int:
    """Delete image vectors by key (16-hex pHash). Returns the number of keys submitted."""
    return _delete_vectors(
        client,
        keys,
        vector_bucket_name=vector_bucket_name,
        index_name=index_name,
        batch_size=batch_size,
        log_label="delete_image_vectors",
    )


def _query_vectors(
    client: Any,
    query_vector: Sequence[float],
    *,
    expected_dim: int,
    limit: int,
    offset: int,
    source_external_id: str | None,
    peer_type: str | None,
    date_from_epoch_ms: int | None,
    date_to_epoch_ms: int | None,
    vector_bucket_name: str,
    index_name: str,
) -> tuple[list[Any], int, int]:
    if len(query_vector) != expected_dim:
        raise ValueError(f"query vector dim {len(query_vector)} != expected {expected_dim}")

    page_offset = max(0, int(offset))
    page_limit = max(1, int(limit))
    fetch_limit = page_offset + page_limit

    filt = build_metadata_filter(
        source_external_id=source_external_id,
        peer_type=peer_type,
        date_from_epoch_ms=date_from_epoch_ms,
        date_to_epoch_ms=date_to_epoch_ms,
    )
    kwargs: dict[str, Any] = {
        "vectorBucketName": vector_bucket_name,
        "indexName": index_name,
        "queryVector": {"float32": [float(x) for x in query_vector]},
        "topK": fetch_limit,
        "returnDistance": True,
        "returnMetadata": True,
    }
    if filt is not None:
        kwargs["filter"] = filt

    response = client.query_vectors(**kwargs)
    return list(response.get("vectors") or []), page_offset, page_limit


def search_message_vectors(
    client: Any,
    query_vector: Sequence[float],
    *,
    limit: int = 10,
    offset: int = 0,
    source_external_id: str | None = None,
    peer_type: str | None = None,
    date_from_epoch_ms: int | None = None,
    date_to_epoch_ms: int | None = None,
    vector_bucket_name: str = DEFAULT_VECTOR_BUCKET,
    index_name: str = DEFAULT_INDEX_NAME,
) -> list[dict[str, Any]]:
    """ANN search over the messages S3 Vectors index.

    Returns hits ``[{id, date, similarity, distance, source_external_id, peer_type}]``
    best-first — same shape as ``milvus.client.search_message_vectors``.
    """
    raw_hits, page_offset, page_limit = _query_vectors(
        client,
        query_vector,
        expected_dim=MESSAGES_DIM,
        limit=limit,
        offset=offset,
        source_external_id=source_external_id,
        peer_type=peer_type,
        date_from_epoch_ms=date_from_epoch_ms,
        date_to_epoch_ms=date_to_epoch_ms,
        vector_bucket_name=vector_bucket_name,
        index_name=index_name,
    )

    hits: list[dict[str, Any]] = []
    for hit in raw_hits:
        if not isinstance(hit, Mapping):
            continue
        meta = hit.get("metadata") if isinstance(hit.get("metadata"), Mapping) else {}
        raw_distance = hit.get("distance")
        hits.append(
            {
                "id": hit.get("key"),
                "date": meta.get(META_DATE),
                "similarity": cosine_similarity_from_distance(
                    float(raw_distance) if raw_distance is not None else None
                ),
                "distance": raw_distance,
                "source_external_id": meta.get(META_SOURCE_EXTERNAL_ID) or None,
                "peer_type": meta.get(META_PEER_TYPE) or None,
            }
        )
    return hits[page_offset : page_offset + page_limit]


def search_image_vectors(
    client: Any,
    query_vector: Sequence[float],
    *,
    limit: int = 10,
    offset: int = 0,
    source_external_id: str | None = None,
    peer_type: str | None = None,
    date_from_epoch_ms: int | None = None,
    date_to_epoch_ms: int | None = None,
    vector_bucket_name: str = DEFAULT_VECTOR_BUCKET,
    index_name: str = DEFAULT_IMAGES_INDEX_NAME,
) -> list[dict[str, Any]]:
    """ANN search over the images S3 Vectors index.

    Returns hits ``[{id, date, msg_id, s3_key, similarity, distance, source_external_id,
    peer_type}]`` best-first — same shape as ``milvus.client.search_image_vectors``.
    """
    raw_hits, page_offset, page_limit = _query_vectors(
        client,
        query_vector,
        expected_dim=IMAGES_DIM,
        limit=limit,
        offset=offset,
        source_external_id=source_external_id,
        peer_type=peer_type,
        date_from_epoch_ms=date_from_epoch_ms,
        date_to_epoch_ms=date_to_epoch_ms,
        vector_bucket_name=vector_bucket_name,
        index_name=index_name,
    )

    hits: list[dict[str, Any]] = []
    for hit in raw_hits:
        if not isinstance(hit, Mapping):
            continue
        meta = hit.get("metadata") if isinstance(hit.get("metadata"), Mapping) else {}
        raw_distance = hit.get("distance")
        key = hit.get("key")
        hits.append(
            {
                "id": key,
                "date": meta.get(META_DATE),
                "msg_id": meta.get(META_MSG_ID) or key,
                "s3_key": meta.get(META_S3_KEY) or "",
                "similarity": cosine_similarity_from_distance(
                    float(raw_distance) if raw_distance is not None else None
                ),
                "distance": raw_distance,
                "source_external_id": meta.get(META_SOURCE_EXTERNAL_ID) or None,
                "peer_type": meta.get(META_PEER_TYPE) or None,
            }
        )
    return hits[page_offset : page_offset + page_limit]
