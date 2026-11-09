"""Flexible search date parsing. No network."""

from datetime import UTC, datetime

import pytest
from fastapi import FastAPI, Query
from fastapi.testclient import TestClient
from pydantic import BaseModel

from services.api.src.search_dates import (
    DATE_FROM_DESCRIPTION,
    DATE_TO_DESCRIPTION,
    SearchDateFrom,
    SearchDateTo,
    parse_search_datetime,
)

_MIDNIGHT = datetime(2024, 8, 15, tzinfo=UTC)
_END = datetime(2024, 8, 15, 23, 59, 59, 999999, tzinfo=UTC)


@pytest.mark.parametrize(
    "text",
    [
        "2024-08-15",
        "2024/08/15",
        "15 Aug 2024",
        "15 August 2024",
        "15-Aug-2024",
        "Aug 15, 2024",
        "August 15, 2024",
        "August 15th, 2024",
    ],
)
def test_date_only_start_is_midnight_utc(text: str) -> None:
    assert parse_search_datetime(text, bound="start") == _MIDNIGHT


def test_single_digit_day() -> None:
    assert parse_search_datetime("5 Aug 2024", bound="start") == datetime(2024, 8, 5, tzinfo=UTC)


def test_date_only_end_includes_the_utc_day() -> None:
    assert parse_search_datetime("15 Aug 2024", bound="end") == _END


def test_iso_timestamp_keeps_its_time() -> None:
    parsed = parse_search_datetime("2024-08-15T14:30:00Z", bound="end")
    assert parsed == datetime(2024, 8, 15, 14, 30, tzinfo=UTC)


def test_offset_converts_to_utc() -> None:
    parsed = parse_search_datetime("2024-08-15T00:30:00+02:00", bound="start")
    assert parsed == datetime(2024, 8, 14, 22, 30, tzinfo=UTC)


def test_written_time_without_zone_is_utc() -> None:
    parsed = parse_search_datetime("15 Aug 2024 14:30", bound="exact")
    assert parsed == datetime(2024, 8, 15, 14, 30, tzinfo=UTC)


def test_unix_seconds_and_millis() -> None:
    instant = datetime(2024, 8, 15, tzinfo=UTC)
    seconds = int(instant.timestamp())
    assert parse_search_datetime(str(seconds), bound="start") == instant
    assert parse_search_datetime(str(seconds * 1000), bound="exact") == instant


def test_unrecognized_date_explains_the_formats() -> None:
    with pytest.raises(ValueError, match="15 Aug 2024"):
        parse_search_datetime("15/08/2024", bound="start")


def test_semantic_body_accepts_written_dates() -> None:
    class Body(BaseModel):
        date_from: SearchDateFrom | None = None
        date_to: SearchDateTo | None = None

    body = Body.model_validate({"date_from": "15 Aug 2024", "date_to": "2024-08-15"})
    assert body.date_from == _MIDNIGHT
    assert body.date_to == _END


def test_query_params_accept_written_dates() -> None:
    app = FastAPI()

    @app.get("/when")
    def when(
        date_from: SearchDateFrom | None = Query(None, description=DATE_FROM_DESCRIPTION),
        date_to: SearchDateTo | None = Query(None, description=DATE_TO_DESCRIPTION),
    ) -> dict[str, str | None]:
        return {
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
        }

    response = TestClient(app).get("/when", params={"date_from": "15 Aug 2024", "date_to": "Aug 16, 2024"})
    assert response.status_code == 200
    assert response.json()["date_from"].startswith("2024-08-15T00:00:00")
    assert response.json()["date_to"].startswith("2024-08-16T23:59:59")
