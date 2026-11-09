"""CLI: compact embed micro shards into canonical Parquet parts.

PYTHONPATH=libs:services python -m jobs.embed_messages.compact_cli
PYTHONPATH=libs:services python -m jobs.embed_messages.compact_cli --reconcile
PYTHONPATH=libs:services python -m jobs.embed_messages.compact_cli --month 2026-06
"""

from __future__ import annotations

import argparse
import logging
import re

from db.connection import connect
from embstore import LocalSink, S3Sink, compact_micro_shards, reconcile_micro_shards
from jobs.embed_messages.config import ConfigFactory

_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


def _build_sink(cfg):
    if cfg.store_backend == "local":
        return LocalSink(cfg.local_dir)
    return S3Sink(
        cfg.s3_bucket,
        region=cfg.aws_region,
        access_key=cfg.aws_access_key_id,
        secret_key=cfg.aws_secret_access_key,
        endpoint_url=cfg.s3_endpoint_url,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Compact embed micro shards into canonical parts")
    parser.add_argument(
        "--min-rows",
        type=int,
        default=100_000,
        help="compact a month when its catalogued micro rows reach this count (default 100000)",
    )
    parser.add_argument(
        "--target-rows",
        type=int,
        default=100_000,
        help="rows per canonical part when packing micros (default 100000)",
    )
    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help="only consider this YYYY-MM month",
    )
    parser.add_argument(
        "--reconcile",
        action="store_true",
        help="register S3 micro shards missing from the catalog, then compact",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=12,
        help="parallel S3 GET workers when merging a pack (default 12)",
    )
    args = parser.parse_args()

    if args.month is not None and not _MONTH_RE.match(args.month):
        raise SystemExit("--month must be YYYY-MM")

    cfg = ConfigFactory.get_config()
    cfg.validate_required()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    sink = _build_sink(cfg)
    with connect(cfg.postgres_dsn) as conn:
        if args.reconcile:
            n = reconcile_micro_shards(
                sink,
                conn,
                model=cfg.embedding_model,
                modality=cfg.embedding_modality,
                prefix=cfg.store_prefix,
            )
            print(f"reconciled {n} orphan micro shard(s)")

        result = compact_micro_shards(
            sink,
            conn,
            model=cfg.embedding_model,
            modality=cfg.embedding_modality,
            prefix=cfg.store_prefix,
            min_rows=args.min_rows,
            target_rows=args.target_rows,
            month=args.month,
            max_workers=args.max_workers,
        )

    if not result:
        print("no months compacted")
    else:
        for item in result:
            print(
                f"compacted {item.month}: {item.row_count} rows ({len(item.micro_keys)} micros -> {item.canonical_key})"
            )


if __name__ == "__main__":
    main()
