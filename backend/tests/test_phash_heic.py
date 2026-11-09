"""Tests for HEIC Pillow registration used by pHash."""

from __future__ import annotations

import pytest

pytest.importorskip("pillow_heif")

from phash.pillow_io import ensure_heif_opener


def test_ensure_heif_opener_is_idempotent() -> None:
    ensure_heif_opener()
    ensure_heif_opener()
