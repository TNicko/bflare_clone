"""Environment loading for services, jobs, and one-off scripts.

Call :func:`load_service_dotenv` once at startup. After that, :func:`getenv`
and the ``env_*`` helpers read that snapshot, not whatever happens to be in
the shell.

Which file is loaded depends only on the path you pass:

* A module under ``services/<name>/`` loads ``services/<name>/.env`` and
  nothing else.
* A module under ``jobs/`` loads ``jobs/.env`` and nothing else.
* Anything else (a script, a library helper) does not search. Pass the
  ``.env`` file you want, or the process environment is used as-is.

A present file replaces the environment: shell exports for keys omitted from
the file are ignored. A missing file falls back to the process environment
(Docker injects vars and does not ship ``.env``).

The first successful load in a process wins. Later calls are ignored unless
``force=True``, so an imported package cannot replace the caller's env.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import overload

from dotenv import dotenv_values

_SERVICE_ENV: dict[str, str] = {}
_LOADED: bool = False


def _owned_env_file(start: Path) -> Path | None:
    """``services/<name>/.env`` or ``jobs/.env`` when ``start`` lives in that tree."""
    for directory in (start, *start.parents):
        if directory.parent.name == "services":
            return directory / ".env"
        if directory.name == "jobs":
            return directory / ".env"
    return None


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return dict(os.environ)
    return {key: value for key, value in dotenv_values(path).items() if value is not None}


def load_service_dotenv(service_file: str | Path, *, force: bool = False) -> None:
    """Load the one ``.env`` that belongs to ``service_file``.

    ``service_file`` is usually ``__file__``. Pass a ``.env`` path to load
    that file and no other (scripts outside ``services/`` and ``jobs/``).
    """
    global _SERVICE_ENV, _LOADED
    if _LOADED and not force:
        return

    path = Path(service_file).resolve()
    if path.name == ".env" or path.suffix == ".env":
        chosen = path
    else:
        start = path if path.is_dir() else path.parent
        chosen = _owned_env_file(start)

    _SERVICE_ENV = _read_env_file(chosen) if chosen is not None else dict(os.environ)
    _LOADED = True


def override_service_env(name: str, value: str) -> None:
    """Set a key after load (CLI flags).

    ``getenv`` reads the snapshot, not ``os.environ``. Writing only the
    process env is ignored.
    """
    _SERVICE_ENV[name] = value
    os.environ[name] = value


@overload
def getenv(name: str, default: str) -> str: ...


@overload
def getenv(name: str, default: None = None) -> str | None: ...


def getenv(name: str, default: str | None = None) -> str | None:
    """Read a key from the loaded snapshot; missing -> ``default``."""
    if name not in _SERVICE_ENV:
        return default
    return _SERVICE_ENV[name]


def parse_dt(value: str | None) -> datetime | None:
    """Parse an ISO 8601 datetime string; empty/whitespace -> ``None``."""
    text = (value or "").strip()
    if not text:
        return None
    return datetime.fromisoformat(text)


def env_int(name: str) -> int | None:
    """Read an integer env var; unset or blank -> ``None``."""
    raw = getenv(name)
    if raw is None or not (text := raw.strip()):
        return None
    return int(text)


def env_float(name: str) -> float | None:
    """Read a float env var; unset or blank -> ``None``."""
    raw = getenv(name)
    if raw is None or not (text := raw.strip()):
        return None
    return float(text)


def env_str(name: str) -> str | None:
    """Read a string env var; unset or blank -> ``None``."""
    raw = getenv(name)
    if raw is None or not (text := raw.strip()):
        return None
    return text


def env_csv(name: str) -> list[str]:
    """Read a comma-separated env var; unset -> ``[]``."""
    raw = getenv(name)
    if raw is None:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def env_truthy(name: str, *, default: bool = False) -> bool:
    """Interpret ``1``/``true``/``yes``/``on`` and ``0``/``false``/``no``/``off``."""
    raw = (getenv(name) or "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default
