"""In-process Prometheus metrics for API harvester session-pool work allocation."""

from __future__ import annotations

from collections import defaultdict
from typing import Protocol
from uuid import UUID

from utils.harvester_colors import harvester_color_key

_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


class _PoolView(Protocol):
    sessions: dict[UUID, object]
    in_flight: dict[UUID, int]
    display_names: dict[UUID, str]


_work_started: dict[tuple[str, str], int] = defaultdict(int)
_work_finished: dict[tuple[str, str], int] = defaultdict(int)


def _label_values(harvester_id: UUID, display_name: str) -> tuple[str, str]:
    name = display_name.strip() if display_name else ""
    return str(harvester_id), name or str(harvester_id)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(harvester_id: str, harvester: str) -> str:
    color = harvester_color_key(harvester)
    return f'harvester_id="{_escape(harvester_id)}",harvester="{_escape(harvester)}",color="{_escape(color)}"'


def observe_work_started(harvester_id: UUID, display_name: str) -> None:
    _work_started[_label_values(harvester_id, display_name)] += 1


def observe_work_finished(harvester_id: UUID, display_name: str) -> None:
    _work_finished[_label_values(harvester_id, display_name)] += 1


def render_prometheus(pool: _PoolView | None) -> tuple[bytes, str]:
    names: dict[UUID, str] = {}
    connected: set[UUID] = set()
    in_flight: dict[UUID, int] = {}
    if pool is not None:
        names.update(getattr(pool, "display_names", {}) or {})
        in_flight = dict(getattr(pool, "in_flight", {}) or {})
        for hid, pooled in (getattr(pool, "sessions", {}) or {}).items():
            connected.add(hid)
            display = str(getattr(pooled, "display_name", "") or "")
            if display:
                names[hid] = display

    for hid, name in _work_started:
        try:
            names.setdefault(UUID(hid), name)
        except ValueError:
            pass
    for hid, name in _work_finished:
        try:
            names.setdefault(UUID(hid), name)
        except ValueError:
            pass

    known = set(names) | set(in_flight) | connected

    lines: list[str] = [
        "# HELP harvest_harvester_in_flight API requests currently using this harvester's Telethon session",
        "# TYPE harvest_harvester_in_flight gauge",
    ]
    for hid in sorted(known, key=str):
        harvester_id, harvester = _label_values(hid, names.get(hid, ""))
        lines.append(f"harvest_harvester_in_flight{{{_labels(harvester_id, harvester)}}} {in_flight.get(hid, 0)}")

    lines += [
        "# HELP harvest_harvester_session_connected 1 if the API process holds a connected Telethon session for this harvester",
        "# TYPE harvest_harvester_session_connected gauge",
    ]
    for hid in sorted(known, key=str):
        harvester_id, harvester = _label_values(hid, names.get(hid, ""))
        lines.append(
            f"harvest_harvester_session_connected{{{_labels(harvester_id, harvester)}}} {1 if hid in connected else 0}"
        )

    lines += [
        "# HELP harvest_harvester_work_started_total API requests that started Telethon work on this harvester",
        "# TYPE harvest_harvester_work_started_total counter",
    ]
    started_keys = set(_work_started) | {_label_values(hid, names.get(hid, "")) for hid in known}
    for hid, harvester in sorted(started_keys):
        lines.append(
            f"harvest_harvester_work_started_total{{{_labels(hid, harvester)}}} {_work_started[(hid, harvester)]}"
        )

    lines += [
        "# HELP harvest_harvester_work_finished_total API requests that finished Telethon work on this harvester",
        "# TYPE harvest_harvester_work_finished_total counter",
    ]
    finished_keys = set(_work_finished) | {_label_values(hid, names.get(hid, "")) for hid in known}
    for hid, harvester in sorted(finished_keys):
        lines.append(
            f"harvest_harvester_work_finished_total{{{_labels(hid, harvester)}}} {_work_finished[(hid, harvester)]}"
        )

    body = ("\n".join(lines) + "\n").encode("utf-8")
    return body, _CONTENT_TYPE
