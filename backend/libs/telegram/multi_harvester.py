"""Run scrape workers concurrently across eligible harvesters."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar
from uuid import UUID

from telethon import TelegramClient

from db.connection import connect_async_tuple
from db.crud.telegram.harvester import list_eligible_harvesters
from db.crud.telegram.worker_session import worker_session_kwargs
from db.schemas.telegram.harvester import TelegramHarvesterSchema
from jobs.harvester_ids import normalize_harvester_external_ids
from telegram.client import authorized_harvester_client
from telegram.harvester_logging import (
    HarvesterLoggerAdapter,
    format_harvester_prefix,
    harvester_log_context,
    harvester_logger,
)
from utils.graceful_stop import install_sigterm_cancels_asyncio_tasks

logger = logging.getLogger(__name__)

T = TypeVar("T")

HarvesterWorker = Callable[
    [
        object,  # AsyncConnection
        TelegramClient,
        TelegramHarvesterSchema,
        HarvesterLoggerAdapter,
    ],
    Awaitable[T],
]


def omit_reserved_harvesters(
    pool: Sequence[TelegramHarvesterSchema],
    reserved_external_ids: set[str],
) -> list[TelegramHarvesterSchema]:
    """Drop harvesters another job has already been assigned."""
    if not reserved_external_ids:
        return list(pool)
    return [harvester for harvester in pool if str(harvester.external_id) not in reserved_external_ids]


def select_worker_harvesters(
    pool: Sequence[TelegramHarvesterSchema],
    wanted_external_ids: Sequence[str],
) -> list[TelegramHarvesterSchema]:
    """Return ``wanted_external_ids`` from ``pool``, in request order.

    An empty wanted list returns the whole pool. Unknown ids raise ``ValueError``.
    """
    wanted = normalize_harvester_external_ids(wanted_external_ids)
    if not wanted:
        return list(pool)
    by_external_id = {str(harvester.external_id): harvester for harvester in pool}
    missing = [external_id for external_id in wanted if external_id not in by_external_id]
    if missing:
        known = ", ".join(str(harvester.external_id) for harvester in pool) or "(none)"
        raise ValueError(
            f"Harvester external_id(s) not found among eligible harvesters: {', '.join(missing)} (eligible: {known})"
        )
    return [by_external_id[external_id] for external_id in wanted]


async def resolve_worker_harvesters(
    conn: object,
    *,
    harvester_external_id: str | None = None,
    harvester_external_ids: Sequence[str] | None = None,
    session_type: str = "system",
) -> list[TelegramHarvesterSchema]:
    """Return eligible harvesters to run.

    With no ids, return every eligible harvester. With one or more Telegram
    user ids, return that subset in the requested order.
    """
    pool = await list_eligible_harvesters(conn, session_type=session_type)
    if not pool:
        raise ValueError(
            f"No eligible harvesters with healthy {session_type!r} sessions "
            "(need is_active=true and auth_state=healthy)"
        )

    wanted: list[str] = []
    if harvester_external_ids:
        wanted.extend(str(external_id) for external_id in harvester_external_ids)
    if harvester_external_id:
        wanted.append(str(harvester_external_id))
    return select_worker_harvesters(pool, wanted)


def worker_harvester_ids(harvesters: Sequence[TelegramHarvesterSchema]) -> list[UUID]:
    return [h.id for h in harvesters]


def format_worker_exception(exc: BaseException) -> str:
    """Single-line summary for log ``msg`` (JSON/Loki often hide ``exc_info``)."""
    text = str(exc).strip()
    if text:
        return f"{type(exc).__name__}: {text}"
    return type(exc).__name__


def log_harvester_worker_failure(
    log: logging.Logger,
    harvester: TelegramHarvesterSchema,
    exc: BaseException,
) -> None:
    prefix = format_harvester_prefix(harvester, color=False)
    detail = format_worker_exception(exc)
    log.error(
        "Harvester worker failed %s: %s",
        prefix,
        detail,
        exc_info=(type(exc), exc, exc.__traceback__),
    )


async def run_across_harvesters(
    *,
    postgres_dsn: str,
    telegram_api_id: int,
    telegram_api_hash: str,
    service_name: str,
    harvesters: Sequence[TelegramHarvesterSchema],
    worker: HarvesterWorker[T],
    base_logger: logging.Logger | None = None,
) -> list[T | BaseException]:
    """Lease each harvester on its own DB connection and run ``worker`` concurrently.

    Failures are returned as exceptions in the result list (``return_exceptions=True``);
    one worker aborting does not cancel the others.
    """
    if not harvesters:
        return []

    log_base = base_logger or logger
    install_sigterm_cancels_asyncio_tasks()

    async def _one(harvester: TelegramHarvesterSchema) -> T:
        log = harvester_logger(log_base, harvester)
        async with await connect_async_tuple(postgres_dsn) as conn:
            async with authorized_harvester_client(
                conn,
                telegram_api_id=telegram_api_id,
                telegram_api_hash=telegram_api_hash,
                harvester_external_id=harvester.external_id,
                **worker_session_kwargs(service_name=service_name),
            ) as (client, leased):
                with harvester_log_context(log):
                    return await worker(conn, client, leased, log)

    results = list(
        await asyncio.gather(
            *(_one(h) for h in harvesters),
            return_exceptions=True,
        )
    )
    if any(isinstance(result, asyncio.CancelledError) for result in results):
        raise asyncio.CancelledError()
    return results
