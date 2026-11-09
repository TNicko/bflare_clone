from __future__ import annotations

from typing import Literal


type PeerTypeSchema = Literal["channel", "chat", "megagroup", "wa_channel"]

type TelegramPeerTypeSchema = Literal["channel", "chat", "megagroup", "user"]

type TelegramCommunityProfileTierSchema = Literal["tg_full", "tg_min", "web"]
