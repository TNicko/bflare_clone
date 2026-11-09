"""CLI for the search indexer (drains `search_outbox` into Quickwit).

Examples (from repo root):
  python -m services.indexer.cli run            # long-running worker (loop)
  python -m services.indexer.cli drain          # process backlog once, then exit (good for cron/tests)
  python -m services.indexer.cli status         # queue depth + oldest pending age
"""

from __future__ import annotations

import argparse
from utils.logger import configure_logging

from .config import ConfigFactory
from .indexer import outbox_status, run_drain, run_loop


def _cmd_run(cfg, _args) -> None:
    run_loop(cfg)


def _cmd_drain(cfg, _args) -> None:
    total = run_drain(cfg)
    print(f"consumed {total} events")


def _cmd_status(cfg, _args) -> None:
    s = outbox_status(cfg)
    age = s.get("oldest_age_seconds")
    age_str = f"{age:.1f}s" if isinstance(age, (int, float)) else "n/a"
    print(f"outbox depth: {s.get('depth', 0)} (upserts={s.get('upserts', 0)} deletes={s.get('deletes', 0)})")
    print(f"oldest pending: {age_str}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Search indexer: search_outbox -> INDEX_SINKS")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="run the worker loop (polls the outbox continuously)")
    sub.add_parser("drain", help="process the current backlog once, then exit")
    sub.add_parser("status", help="show outbox depth + oldest pending age")

    args = parser.parse_args()
    cfg = ConfigFactory.get_config()
    configure_logging(level=cfg.log_level)

    handlers = {"run": _cmd_run, "drain": _cmd_drain, "status": _cmd_status}
    handlers[args.command](cfg, args)


if __name__ == "__main__":
    main()
