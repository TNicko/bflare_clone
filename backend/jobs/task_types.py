"""Task type catalog for harvest_jobs."""

from __future__ import annotations

from db.crud.system.jobs import TASK_TYPES

# Re-export for the jobs agent without importing DB helpers' side effects beyond constants.
ALL_TASK_TYPES: tuple[str, ...] = TASK_TYPES

# CLI module path for subprocess runners: python -m <module> ...
TASK_CLI_MODULES: dict[str, str] = {
    "embed_images": "jobs.embed_images.cli",
    "embed_messages": "jobs.embed_messages.cli",
    "fetch_messages": "jobs.fetch_messages.cli",
    "telegram_search": "jobs.telegram_search.cli",
}
