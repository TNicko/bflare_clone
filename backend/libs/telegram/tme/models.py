from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

PeerClassification = Literal["channel", "megagroup", "user", "dead", "bot", "error"]


class TmeProfile(BaseModel):
    username: str
    html_title: str | None = None
    display_title: str | None = None
    extra_text: str | None = None
    about: str | None = None
    has_preview_channel: bool = False
    photo_url: str | None = None
    participants_count: int | None = None
    online_count: int | None = None
    classification: PeerClassification = "error"
    http_status: int | None = None
