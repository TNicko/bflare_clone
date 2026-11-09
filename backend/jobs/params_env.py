"""Map harvest_jobs.params → subprocess environment for task CLIs.

Many CLIs read peer/harvester selection from env rather than argparse.
The agent merges these into the child process env before launch.
"""

from __future__ import annotations

from typing import Any

from jobs.harvester_ids import harvester_external_ids_from_params


def _truthy(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str) and value.strip().lower() in ("1", "true", "yes", "on"):
        return True
    return False


def _str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _set(env: dict[str, str], key: str, value: Any) -> None:
    s = _str(value)
    if s is not None:
        env[key] = s


def _set_bool(env: dict[str, str], key: str, value: Any, *, default: bool | None = None) -> None:
    if value is None and default is None:
        return
    on = _truthy(value) if value is not None else bool(default)
    env[key] = "1" if on else "0"


def params_to_env(task_type: str, params: dict[str, Any] | None) -> dict[str, str]:
    """Return env vars to overlay for ``task_type`` given job ``params``."""
    p = dict(params or {})
    env: dict[str, str] = {}

    peer = _str(p.get("peer_external_id") or p.get("external_id") or p.get("seed_external_id") or p.get("seed"))
    harvester = _str(p.get("harvester_external_id"))

    if task_type == "fetch_messages":
        _set(env, "TELEGRAM_PEER_EXTERNAL_ID", peer)
        _set(env, "TELEGRAM_COMMUNITY_ID", p.get("community_id"))
        _set(env, "TELEGRAM_PEER_NAME_QUERY", p.get("peer_name_query"))
        _set(env, "HARVESTER_EXTERNAL_ID", harvester)
        slugs = p.get("category_slugs")
        if isinstance(slugs, list):
            _set(env, "TELEGRAM_CATEGORY_SLUGS", ",".join(str(s) for s in slugs if str(s).strip()))
        else:
            _set(env, "TELEGRAM_CATEGORY_SLUGS", slugs)
        if "pending_only" in p:
            _set_bool(env, "FETCH_PENDING_ONLY", p.get("pending_only"))

    elif task_type in ("embed_messages", "embed_images"):
        _set(env, "TELEGRAM_PEER_EXTERNAL_ID", peer)
        if "pending_only" in p:
            _set_bool(env, "EMBED_PENDING_ONLY", p.get("pending_only"), default=True)
        if task_type == "embed_images" and p.get("model"):
            _set(env, "IMAGE_EMBEDDING_MODEL", p.get("model"))
        _set(env, "TELEGRAM_FETCH_AFTER", p.get("after"))
        _set(env, "TELEGRAM_FETCH_BEFORE", p.get("before"))

    elif task_type == "telegram_search":
        selected = harvester_external_ids_from_params(p)
        if len(selected) == 1:
            _set(env, "HARVESTER_EXTERNAL_ID", selected[0])

    return env
