"""JSON logs must keep the exception on ``msg`` so Grafana/Loki can show it."""

from __future__ import annotations

import json
import logging

from utils.logger import JsonFormatter


def test_json_formatter_puts_exception_summary_on_msg() -> None:
    record = logging.LogRecord(
        name="jobs.telegram_search.pipeline",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="Harvester worker failed [Ariakso]",
        args=(),
        exc_info=(
            UndefinedColumn,
            UndefinedColumn("column c.harvester_id does not exist"),
            None,
        ),
    )
    payload = json.loads(JsonFormatter().format(record))
    assert "UndefinedColumn:" in payload["msg"]
    assert "c.harvester_id" in payload["msg"]
    assert "UndefinedColumn" in payload["exc_info"]


def test_json_formatter_does_not_duplicate_summary_already_in_msg() -> None:
    exc = ValueError("session not authorized")
    record = logging.LogRecord(
        name="jobs.telegram_search.pipeline",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="Harvester worker failed [Ariakso]: ValueError: session not authorized",
        args=(),
        exc_info=(ValueError, exc, None),
    )
    payload = json.loads(JsonFormatter().format(record))
    assert payload["msg"].count("ValueError: session not authorized") == 1


class UndefinedColumn(Exception):
    pass
