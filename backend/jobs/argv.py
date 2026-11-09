"""Map harvest_jobs.params JSON → CLI argv for each task type.

Only emit flags each CLI actually accepts. Peer/harvester selection that is
env-driven is handled by :mod:`jobs.params_env`.
"""

from __future__ import annotations

from typing import Any, Callable

from jobs.harvester_ids import harvester_external_ids_from_params


def _str_flag(args: list[str], flag: str, value: Any) -> None:
    if value is None:
        return
    s = str(value).strip()
    if not s:
        return
    args.extend([flag, s])


def _bool_flag(args: list[str], flag: str, value: Any) -> None:
    if value is True or (isinstance(value, str) and value.strip().lower() in ("1", "true", "yes", "on")):
        args.append(flag)


def _argv_fetch_messages(params: dict[str, Any]) -> list[str]:
    # Peer/community/pending → env via params_env. Only CLI-accepted flags here.
    args: list[str] = []
    _str_flag(args, "--harvester-external-id", params.get("harvester_external_id"))
    _bool_flag(args, "--embed", params.get("embed"))
    _bool_flag(args, "--debug", params.get("debug"))
    return args


def _argv_embed_images(params: dict[str, Any]) -> list[str]:
    args: list[str] = []
    _str_flag(args, "--model", params.get("model"))
    return args


def _argv_embed_messages(params: dict[str, Any]) -> list[str]:
    return _argv_embed_images(params)


def _argv_telegram_search(params: dict[str, Any]) -> list[str]:
    args: list[str] = []
    for external_id in harvester_external_ids_from_params(params):
        args.extend(["--harvester-external-id", external_id])
    return args


_BUILDERS: dict[str, Callable[[dict[str, Any]], list[str]]] = {
    "embed_images": _argv_embed_images,
    "embed_messages": _argv_embed_messages,
    "fetch_messages": _argv_fetch_messages,
    "telegram_search": _argv_telegram_search,
}

TASK_CLI_OVERRIDE: dict[str, str] = {}


def params_to_argv(task_type: str, params: dict[str, Any] | None) -> list[str]:
    builder = _BUILDERS.get(task_type)
    if builder is None:
        raise ValueError(f"no argv builder for task_type={task_type}")
    raw = dict(params or {})
    # Allow explicit passthrough: {"argv": ["--foo", "bar"]}
    if isinstance(raw.get("argv"), list):
        return [str(x) for x in raw["argv"]]
    extra = raw.pop("extra_argv", None)
    args = builder(raw)
    if isinstance(extra, list):
        args.extend(str(x) for x in extra)
    return args
