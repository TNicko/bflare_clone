"""CLI: archive cold telegram_messages to S3 Parquet + Phase 3c strip.

Status:
  PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli status
  PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli status --month 2024-07

Archive (3a):
  PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli run --month 2024-07
  PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli run --max-months 500

Strip fat columns after archive (3c) — start with one small month:
  PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli strip --month 2015-09 --dry-run
  PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli strip --month 2015-09
  PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli strip --max-months 500
"""

from __future__ import annotations

import argparse
import logging
import sys

from jobs.archive_messages.archive import run_archive, status_summary
from jobs.archive_messages.config import ConfigFactory, hot_cutoff_month_start
from jobs.archive_messages.strip import (
    run_strip,
    strip_backlog_count,
    strip_month_has_work,
)


def _cfg():
    return ConfigFactory.get_config()


def _cmd_status(args: argparse.Namespace) -> int:
    cfg = _cfg()
    cfg.validate_required()
    st = status_summary(cfg, month=args.month)
    if args.month:
        st["strip_backlog"] = strip_backlog_count(cfg, month=args.month)
        st["needs_strip"] = strip_month_has_work(cfg, args.month)
    for k, v in st.items():
        print(f"{k}={v}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    cfg = _cfg()
    if args.dry_run:
        cfg = cfg.model_copy(update={"dry_run": True})
    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    months = [args.month] if args.month else None
    if months is None and args.max_months <= 0:
        print(
            "refusing to archive all cold months in one invocation; pass --month YYYY-MM or --max-months N",
            file=sys.stderr,
        )
        return 2
    totals = run_archive(
        cfg,
        months=months,
        max_months=args.max_months,
        reset=args.reset,
    )
    print(
        f"done months={totals['months']} scanned={totals['scanned']} "
        f"marked={totals['marked']} parts={totals['parts']} "
        f"targets={totals.get('targets')}"
    )
    return 0


def _cmd_strip(args: argparse.Namespace) -> int:
    cfg = _cfg()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    months = [args.month] if args.month else None
    if months is None and args.max_months <= 0:
        print(
            "refusing to strip all cold months in one invocation; pass --month YYYY-MM or --max-months N",
            file=sys.stderr,
        )
        return 2
    try:
        totals = run_strip(
            cfg,
            months=months,
            max_months=args.max_months,
            dry_run=args.dry_run,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(
        f"done months={totals['months']} stripped={totals['stripped']} "
        f"would_strip={totals['would_strip']} chunks={totals['chunks']} "
        f"targets={totals.get('targets')}"
    )
    return 0


def _cmd_cutoff(_: argparse.Namespace) -> int:
    cfg = _cfg()
    print(hot_cutoff_month_start(hot_months=cfg.hot_months).isoformat())
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Archive / strip cold telegram_messages")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_status = sub.add_parser("status", help="Hot cutoff + archive/strip backlog signal")
    p_status.add_argument("--month", default=None, help="Exact COUNT for one YYYY-MM")
    p_status.set_defaults(func=_cmd_status)

    p_run = sub.add_parser("run", help="Archive one or more cold months to S3 Parquet")
    p_run.add_argument("--month", default=None, help="YYYY-MM (must be colder than hot window)")
    p_run.add_argument(
        "--max-months",
        type=int,
        default=0,
        help="When --month omitted: archive this many oldest backlog months",
    )
    p_run.add_argument("--reset", action="store_true", help="Ignore checkpoint for the month(s)")
    p_run.add_argument("--dry-run", action="store_true", help="No durable S3 PUT / no PG mark")
    p_run.set_defaults(func=_cmd_run)

    p_strip = sub.add_parser(
        "strip",
        help="NULL fat columns on archived cold months (Phase 3c; disables search/edit triggers)",
    )
    p_strip.add_argument("--month", default=None, help="YYYY-MM (archived + cold)")
    p_strip.add_argument(
        "--max-months",
        type=int,
        default=0,
        help="When --month omitted: strip this many oldest months that still have fat bodies",
    )
    p_strip.add_argument("--dry-run", action="store_true", help="Count only; no UPDATE")
    p_strip.set_defaults(func=_cmd_strip)

    p_cut = sub.add_parser("hot-cutoff", help="Print inclusive hot month start (YYYY-MM-DD)")
    p_cut.set_defaults(func=_cmd_cutoff)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
