"""Process-wide message-archive sink (API / jobs)."""

from __future__ import annotations

import logging
import threading
from typing import Any

from utils.env import getenv

from .layout import DEFAULT_PREFIX

lg = logging.getLogger(__name__)

_lock = threading.Lock()
_sink: Any | None = None
_prefix: str = DEFAULT_PREFIX
_disabled = False


def message_archive_prefix() -> str:
    raw = (getenv("MESSAGE_ARCHIVE_PREFIX") or DEFAULT_PREFIX).strip()
    if raw.startswith("s3://"):
        without = raw[len("s3://") :]
        parts = without.split("/", 1)
        return (parts[1] if len(parts) > 1 else DEFAULT_PREFIX).strip("/") or DEFAULT_PREFIX
    return raw.strip("/") or DEFAULT_PREFIX


def message_archive_bucket() -> str:
    raw = (getenv("MESSAGE_ARCHIVE_PREFIX") or "").strip()
    if raw.startswith("s3://"):
        without = raw[len("s3://") :]
        return without.split("/", 1)[0] or (
            getenv("MESSAGE_ARCHIVE_BUCKET")
            or getenv("HARVEST_S3_BUCKET")
            or getenv("S3_BUCKET")
            or "messenger-harvest"
        )
    return (
        getenv("MESSAGE_ARCHIVE_BUCKET") or getenv("HARVEST_S3_BUCKET") or getenv("S3_BUCKET") or "messenger-harvest"
    ).strip() or "messenger-harvest"


def get_message_archive_sink() -> Any | None:
    """Lazy S3/local sink for hydrate + overlays. None if unconfigured / disabled."""
    global _sink, _prefix, _disabled
    if _disabled:
        return None
    if _sink is not None:
        return _sink
    with _lock:
        if _disabled:
            return None
        if _sink is not None:
            return _sink
        backend = (getenv("MESSAGE_ARCHIVE_BACKEND") or "s3").strip().lower() or "s3"
        _prefix = message_archive_prefix()
        try:
            if backend == "local":
                from . import LocalSink

                local = (getenv("MESSAGE_ARCHIVE_LOCAL_DIR") or "").strip()
                if not local:
                    lg.warning("MESSAGE_ARCHIVE_BACKEND=local but MESSAGE_ARCHIVE_LOCAL_DIR empty")
                    _disabled = True
                    return None
                _sink = LocalSink(local)
            else:
                from . import S3Sink

                region = (getenv("AWS_REGION") or "").strip() or "eu-west-2"
                _sink = S3Sink(
                    message_archive_bucket(),
                    region=region,
                    access_key=getenv("AWS_ACCESS_KEY_ID") or None,
                    secret_key=getenv("AWS_SECRET_ACCESS_KEY") or None,
                    endpoint_url=getenv("S3_ENDPOINT_URL") or None,
                )
        except Exception:
            lg.exception("failed to init message archive sink")
            _disabled = True
            return None
        return _sink


def get_message_archive_prefix() -> str:
    get_message_archive_sink()
    return _prefix


def reset_message_archive_sink_for_tests() -> None:
    global _sink, _disabled, _prefix
    with _lock:
        _sink = None
        _disabled = False
        _prefix = DEFAULT_PREFIX
