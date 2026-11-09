"""Pillow helpers shared by pHash and other image decode paths."""

from __future__ import annotations

_heif_registered = False


def ensure_heif_opener() -> None:
    """Register HEIF/HEIC support with Pillow (idempotent).

    Requires the ``pillow-heif`` package in the runtime environment.
    """
    global _heif_registered
    if _heif_registered:
        return
    from pillow_heif import register_heif_opener

    register_heif_opener()
    _heif_registered = True
