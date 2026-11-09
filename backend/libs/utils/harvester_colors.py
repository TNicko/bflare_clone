"""Stable harvester colors shared by API logs and Grafana series.

Palette avoids Grafana log-level colors (info green, warn orange, error red)
so the label stays distinct from INFO/WARN/ERROR.

Grafana's log panel does not support 24-bit ANSI (``38;2;r;g;b``). It treats
the extra numbers as classic SGR codes, so RGB components like 42/107 become
green/white *backgrounds* (highlighted chips) instead of text color. Use
16-color foreground SGR only — the same mechanism as the log-level column.
"""

from __future__ import annotations

import os
import sys

# Keep in sync with Grafana harvester panels (color= hex without '#').
HARVESTER_PALETTE: tuple[str, ...] = (
    "#6ED0E0",  # cyan
    "#705DA0",  # violet
    "#CC79A7",  # magenta
    "#2AA198",  # teal
    "#D33682",  # pink
    "#268BD2",  # strong blue
    "#A6753A",  # brown
    "#E0B429",  # gold (was muted purple — too close to violet)
)

# Foreground-only 16-color SGR, same index as HARVESTER_PALETTE.
# Avoid 31/32/33 (error/info/warn) and 40–47 / 100–107 (backgrounds).
_HARVESTER_ANSI_SGR: tuple[str, ...] = (
    "36",  # cyan
    "35",  # violet → magenta
    "95",  # magenta
    "96",  # teal → bright cyan
    "95",  # pink → bright magenta
    "94",  # strong blue
    "90",  # brown → gray
    "93",  # gold → bright yellow
)

_ANSI_RESET = "\033[0m"


def _grafana_string_hash(value: str) -> int:
    """Unsigned 32-bit ``string-hash`` (same algorithm Grafana uses for by-name colors)."""
    h = 5381
    for i in range(len(value) - 1, -1, -1):
        left = (h * 33) % (2**32)
        if left >= 2**31:
            left -= 2**32
        h = left ^ ord(value[i])
        h = h % (2**32)
        if h >= 2**31:
            h -= 2**32
    return h % (2**32)


def _palette_index(label: str) -> int:
    return _grafana_string_hash(label) % len(HARVESTER_PALETTE)


def harvester_color_hex(label: str) -> str:
    return HARVESTER_PALETTE[_palette_index(label)]


def harvester_color_key(label: str) -> str:
    """Prometheus / Grafana label value (hex without '#')."""
    return harvester_color_hex(label).removeprefix("#").upper()


def harvester_color_ansi(label: str) -> str:
    return f"\033[{_HARVESTER_ANSI_SGR[_palette_index(label)]}m"


def harvester_label_color_enabled() -> bool:
    if os.getenv("NO_COLOR", "").strip():
        return False
    if os.getenv("FORCE_COLOR", "").strip():
        return True
    fmt = os.getenv("LOG_FORMAT", "").strip().lower()
    if fmt == "plain":
        return False
    if fmt == "json":
        return True
    return sys.stderr.isatty()


def format_harvester_log_label(label: str, *, color: bool | None = None) -> str:
    use_color = harvester_label_color_enabled() if color is None else color
    if not use_color:
        return label
    return f"{harvester_color_ansi(label)}{label}{_ANSI_RESET}"
