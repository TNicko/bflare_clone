"""Flexible date parsing for search filters.

One parameter accepts a calendar date, a written date, an ISO timestamp, or a
unix time. A date with no time is midnight UTC for a start and the end of that
UTC day for an end. Times without a timezone are UTC.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from typing import Any, Literal

from pydantic_core import core_schema

Bound = Literal["start", "end", "exact"]

DATE_FORMAT_HINT = "Use 2024-08-15, 15 Aug 2024, 2024-08-15T14:30:00Z, or a unix timestamp."
DATE_FROM_DESCRIPTION = (
    "Optional start. 2024-08-15, 15 Aug 2024, 2024-08-15T14:30:00Z, or a unix timestamp. "
    "A date with no time starts at midnight UTC. Times without a timezone are UTC."
)
DATE_TO_DESCRIPTION = "Optional end. Same formats as date_from. A date with no time includes that whole UTC day."
DATE_EXACT_DESCRIPTION = (
    "That message's time. Send with message_id. 2024-08-15T14:30:00Z, 15 Aug 2024 14:30, or a unix timestamp."
)

_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

_ISO = re.compile(r"(\d{4}-\d{2}-\d{2})(?:[ T](.+))?\Z")
_YMD = re.compile(
    r"(?P<y>\d{4})/(?P<m>\d{1,2})/(?P<d>\d{1,2})"
    r"(?:[ T](?P<H>\d{1,2}):(?P<M>\d{2})(?::(?P<S>\d{2}))?)?\Z"
)
_DMY = re.compile(
    r"(?P<d>\d{1,2})[ -](?P<mon>[A-Za-z]+)[ -](?P<y>\d{4})"
    r"(?:[ T](?P<H>\d{1,2}):(?P<M>\d{2})(?::(?P<S>\d{2}))?)?\Z"
)
_MDY = re.compile(
    r"(?P<mon>[A-Za-z]+)\s+(?P<d>\d{1,2}),?\s+(?P<y>\d{4})"
    r"(?:[ T](?P<H>\d{1,2}):(?P<M>\d{2})(?::(?P<S>\d{2}))?)?\Z"
)


def parse_search_datetime(value: Any, *, bound: Bound) -> datetime:
    """Parse one search date. Raises ValueError when the text is not a date."""
    if isinstance(value, datetime):
        parsed, date_only = value, False
    elif isinstance(value, date):
        parsed, date_only = datetime(value.year, value.month, value.day), True
    elif isinstance(value, int) and not isinstance(value, bool):
        parsed, date_only = _from_unix(value), False
    elif isinstance(value, str):
        parsed, date_only = _parse_text(value)
    else:
        raise ValueError(DATE_FORMAT_HINT)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    if date_only and bound == "end":
        return parsed.replace(hour=23, minute=59, second=59, microsecond=999999)
    if date_only and bound == "start":
        return parsed.replace(hour=0, minute=0, second=0, microsecond=0)
    return parsed


def _parse_optional(value: Any, *, bound: Bound) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return parse_search_datetime(value, bound=bound)


def _parse_text(text: str) -> tuple[datetime, bool]:
    cleaned = re.sub(r"\s+", " ", text.strip())
    cleaned = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", cleaned, flags=re.IGNORECASE)
    if re.fullmatch(r"\d{10}|\d{13}", cleaned):
        return _from_unix(int(cleaned)), False

    iso = _ISO.fullmatch(cleaned)
    if iso is not None:
        if iso.group(2) is None:
            return _calendar(int(iso.group(1)[:4]), int(iso.group(1)[5:7]), int(iso.group(1)[8:10])), True
        stamp = cleaned[:-1] + "+00:00" if cleaned.endswith(("Z", "z")) else cleaned
        try:
            return datetime.fromisoformat(stamp), False
        except ValueError:
            pass

    for pattern in (_YMD, _DMY, _MDY):
        match = pattern.fullmatch(cleaned)
        if match is None:
            continue
        year = int(match.group("y"))
        day = int(match.group("d"))
        month_text = match.groupdict().get("mon")
        month = _MONTHS[month_text.lower()] if month_text else int(match.group("m"))
        hour = match.group("H")
        if hour is None:
            return _calendar(year, month, day), True
        return (
            _calendar(year, month, day, int(hour), int(match.group("M")), int(match.group("S") or 0)),
            False,
        )

    raise ValueError(DATE_FORMAT_HINT)


def _calendar(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> datetime:
    try:
        return datetime(year, month, day, hour, minute, second)
    except ValueError as exc:
        raise ValueError(DATE_FORMAT_HINT) from exc


def _from_unix(value: int) -> datetime:
    seconds = value / 1000 if abs(value) >= 10**12 else value
    try:
        return datetime.fromtimestamp(seconds, tz=UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError(DATE_FORMAT_HINT) from exc


def _search_date_schema(bound: Bound) -> core_schema.CoreSchema:
    def validate(value: Any) -> datetime | None:
        return _parse_optional(value, bound=bound)

    return core_schema.no_info_plain_validator_function(validate)


class SearchDateFrom(datetime):
    """Start of a search window. A date with no time is midnight UTC."""

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: Any) -> core_schema.CoreSchema:
        return _search_date_schema("start")


class SearchDateTo(datetime):
    """End of a search window. A date with no time includes that whole UTC day."""

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: Any) -> core_schema.CoreSchema:
        return _search_date_schema("end")


class SearchDateExact(datetime):
    """A message timestamp. A date with no time stays at midnight UTC."""

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: Any) -> core_schema.CoreSchema:
        return _search_date_schema("exact")
