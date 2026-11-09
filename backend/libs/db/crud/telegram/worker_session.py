"""Common kwargs for background worker Telethon sessions."""

from __future__ import annotations

import os
from typing import Literal, TypedDict

from telegram.session_env import get_server_id, get_session_location_id


class WorkerSessionKwargs(TypedDict):
    session_type: Literal["system"]
    location_id: str
    server_id: str


def worker_session_kwargs(*, service_name: str) -> WorkerSessionKwargs:
    server_id = get_server_id(service_name=service_name)
    pid = str(os.getpid())
    # A shared TELEGRAM_SERVER_ID must not make two job processes look like one
    # holder, or the second job joins the first job's live Telegram connection.
    if not server_id.endswith(f":{pid}"):
        server_id = f"{server_id}:{pid}"
    return {
        "session_type": "system",
        "location_id": get_session_location_id(),
        "server_id": server_id,
    }
