"""Tests for HEIC harvest display helpers."""

from __future__ import annotations

from telegram.image_harvest import display_key_for_object_key, is_heif_mime


def test_is_heif_mime() -> None:
    assert is_heif_mime("image/heic")
    assert is_heif_mime("image/HEIF")
    assert not is_heif_mime("image/jpeg")
    assert not is_heif_mime(None)


def test_display_key_for_object_key() -> None:
    key = "chats/telegram/-1001/dt=2026-06/23537-0"
    assert display_key_for_object_key(key) == "chats/telegram/-1001/dt=2026-06/23537-0-display.jpg"
