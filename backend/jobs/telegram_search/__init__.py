"""Telegram contacts.search fleet job."""

from __future__ import annotations

from jobs.telegram_search.pipeline import TelegramRunStats, run_telegram_search

__all__ = ["TelegramRunStats", "run_telegram_search"]
