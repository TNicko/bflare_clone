"""Compact micro embedding shards into large canonical ``part-*.parquet`` objects."""

from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .layout import DEFAULT_PREFIX, month_from_shard_key, shard_key

if TYPE_CHECKING:
    from db.crud.system.embedding_store import MicroShardRef

    from .sink import Sink

log = logging.getLogger(__name__)

__all__ = [
    "CompactedMonth",
    "compact_micro_shards",
    "month_from_shard_key",
    "reconcile_micro_shards",
]


@dataclass(frozen=True, slots=True)
class CompactedMonth:
    month: str
    row_count: int
    micro_keys: list[str]
    canonical_key: str


def _pack_micros(
    shards: list[MicroShardRef],
    *,
    target_rows: int,
) -> list[list[MicroShardRef]]:
    """Greedy packs of whole micros each totaling >= ``target_rows``. Remainder omitted."""
    packs: list[list[MicroShardRef]] = []
    i = 0
    while i < len(shards):
        pack: list[MicroShardRef] = []
        pack_rows = 0
        while i < len(shards) and pack_rows < target_rows:
            pack.append(shards[i])
            pack_rows += shards[i].row_count
            i += 1
        if pack_rows < target_rows:
            break
        packs.append(pack)
    return packs


def _read_parquet_table(sink: Sink, key: str):
    import pyarrow as pa
    import pyarrow.parquet as pq

    return pq.read_table(pa.BufferReader(sink.get_bytes(key)))


def _parquet_num_rows(sink: Sink, key: str) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(pa.BufferReader(sink.get_bytes(key)))
    return int(pf.metadata.num_rows)


def _write_canonical_table(
    sink: Sink,
    *,
    model: str,
    modality: str,
    month: str,
    prefix: str,
    table,
) -> tuple[str, int]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    out = pa.BufferOutputStream()
    pq.write_table(table, out, compression="snappy")
    data = out.getvalue().to_pybytes()
    key = shard_key(model, modality, month, uuid.uuid4().hex, prefix=prefix)
    sink.put_bytes(key, data)
    return key, int(table.num_rows)


def _dedupe_table_by_id(table: Any) -> Any:
    """Keep the last row per ``id`` so canonical shards stay unique for Milvus."""
    if "id" not in table.column_names or table.num_rows < 2:
        return table
    ids = table.column("id").to_pylist()
    last_index: dict[str, int] = {}
    for i, vid in enumerate(ids):
        last_index[str(vid)] = i
    if len(last_index) == len(ids):
        return table
    keep = sorted(last_index.values())
    dropped = len(ids) - len(keep)
    log.warning("compact dedupe: dropped %s duplicate id row(s)", dropped)
    return table.take(keep)


def _merge_micro_tables(
    sink: Sink,
    keys: list[str],
    *,
    max_workers: int,
):
    import pyarrow as pa

    if not keys:
        raise ValueError("no micro keys to merge")
    if len(keys) == 1:
        return _dedupe_table_by_id(_read_parquet_table(sink, keys[0]))

    workers = max(1, min(max_workers, len(keys)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_read_parquet_table, sink, key): key for key in keys}
        by_key: dict[str, Any] = {}
        for fut in as_completed(futures):
            key = futures[fut]
            by_key[key] = fut.result()
    tables = [by_key[k] for k in keys]
    return _dedupe_table_by_id(pa.concat_tables(tables, promote_options="default"))


def compact_micro_shards(
    sink: Sink,
    conn: Any,
    *,
    model: str,
    modality: str,
    prefix: str = DEFAULT_PREFIX,
    min_rows: int = 100_000,
    target_rows: int = 100_000,
    month: str | None = None,
    max_workers: int = 12,
) -> list[CompactedMonth]:
    """Merge catalogued ``micro/`` shards into canonical parts using Postgres planning.

    Eligible months are those with ``SUM(row_count) >= min_rows``. Micros are packed
    into canonical chunks of at least ``target_rows``; remainders below ``target_rows``
    are left for a later run. Each successful chunk updates the catalog and is committed
    before S3 micro deletes.
    """
    from db.crud.system.embedding_store import (
        list_micro_months_ready,
        list_micro_shards_for_month,
        replace_compacted_shards,
    )

    if target_rows < 1:
        raise ValueError("target_rows must be >= 1")
    if min_rows < 1:
        raise ValueError("min_rows must be >= 1")

    ready = list_micro_months_ready(
        conn,
        model=model,
        modality=modality,
        min_rows=min_rows,
        month=month,
    )
    if not ready:
        log.info(
            "no months ready to compact (min_rows=%s, month=%s)",
            min_rows,
            month,
        )
        return []

    compacted: list[CompactedMonth] = []
    for month_key, total_rows in ready:
        shards = list_micro_shards_for_month(conn, model=model, modality=modality, month=month_key)
        packs = _pack_micros(shards, target_rows=target_rows)
        if not packs:
            log.info(
                "month %s: %s micro row(s) but no full pack of target_rows=%s; skip",
                month_key,
                total_rows,
                target_rows,
            )
            continue

        log.info(
            "month %s: compacting %s pack(s) from %s micro row(s) in %s shard(s)",
            month_key,
            len(packs),
            total_rows,
            len(shards),
        )
        for pack in packs:
            micro_keys = [s.shard_key for s in pack]
            table = _merge_micro_tables(sink, micro_keys, max_workers=max_workers)
            canonical_key, n_rows = _write_canonical_table(
                sink,
                model=model,
                modality=modality,
                month=month_key,
                prefix=prefix,
                table=table,
            )
            replace_compacted_shards(
                conn,
                model=model,
                modality=modality,
                micro_keys=micro_keys,
                canonical_key=canonical_key,
                month=month_key,
                row_count=n_rows,
            )
            conn.commit()
            # Catalog first, then S3 delete: orphaned S3 micros are safer than dangling catalog keys.
            sink.delete_keys(micro_keys)
            compacted.append(
                CompactedMonth(
                    month=month_key,
                    row_count=n_rows,
                    micro_keys=micro_keys,
                    canonical_key=canonical_key,
                )
            )
            log.info(
                "month %s: compacted %s row(s) from %s micro(s) -> %s",
                month_key,
                n_rows,
                len(micro_keys),
                canonical_key,
            )

    return compacted


def reconcile_micro_shards(
    sink: Sink,
    conn: Any,
    *,
    model: str,
    modality: str,
    prefix: str = DEFAULT_PREFIX,
) -> int:
    """Register S3 micro objects missing from the catalog. Returns newly registered count."""
    from db.crud.system.embedding_store import (
        MicroShardRef,
        list_catalog_shard_keys,
        register_micro_shards,
    )
    from .reader import list_shards

    catalogued = list_catalog_shard_keys(conn, model=model, modality=modality, tier="micro")
    s3_keys = list_shards(sink, model, modality, prefix=prefix, kind="micro")
    missing = [k for k in s3_keys if k not in catalogued]
    if not missing:
        log.info("reconcile: no orphan micro shards for %s/%s", model, modality)
        return 0

    refs: list[MicroShardRef] = []
    for key in missing:
        month = month_from_shard_key(key)
        if month is None:
            log.warning("reconcile: skip micro key with no dt= month: %s", key)
            continue
        try:
            n = _parquet_num_rows(sink, key)
        except Exception:
            log.exception("reconcile: failed reading %s", key)
            continue
        refs.append(MicroShardRef(shard_key=key, month=month, row_count=n))

    if not refs:
        return 0
    register_micro_shards(conn, model=model, modality=modality, shards=refs)
    conn.commit()
    log.info(
        "reconcile: registered %s orphan micro shard(s) for %s/%s",
        len(refs),
        model,
        modality,
    )
    return len(refs)
