"""Cooperative graceful stop for long-running CLIs (SIGINT / SIGTERM).

First signal requests a clean stop after the current unit of work.
Second signal restores the default handler so a subsequent Ctrl+C forces exit.

Telegram harvest jobs also install :func:`install_sigterm_cancels_asyncio_tasks`
so fleet cancel (SIGTERM) unwinds ``async with`` blocks and releases session leases.
Default SIGTERM kills the process without running ``finally``.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import weakref
from types import FrameType

log = logging.getLogger(__name__)

_sigterm_loops: weakref.WeakSet[asyncio.AbstractEventLoop] = weakref.WeakSet()


class GracefulStop:
    """Thread-safe-ish flag set from signal handlers; poll ``requested`` between units of work."""

    def __init__(self, *, name: str = "process") -> None:
        self._requested = False
        self._name = name

    @property
    def requested(self) -> bool:
        return self._requested

    def request(self, signum: int | None = None) -> None:
        if self._requested:
            log.warning(
                "%s: second stop signal (%s); next Ctrl+C will force-exit",
                self._name,
                signum if signum is not None else "?",
            )
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            return
        self._requested = True
        try:
            sig_name = signal.Signals(signum).name if signum is not None else "manual"
        except (ValueError, TypeError):
            sig_name = str(signum) if signum is not None else "manual"
        log.info(
            "%s: stop requested (%s); finishing current page then exiting",
            self._name,
            sig_name,
        )

    def install(self) -> None:
        def _handler(signum: int, _frame: FrameType | None) -> None:
            self.request(signum)

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)


def install_graceful_stop(*, name: str = "process") -> GracefulStop:
    stop = GracefulStop(name=name)
    stop.install()
    return stop


def cancel_all_asyncio_tasks(
    loop: asyncio.AbstractEventLoop | None = None,
    *,
    exclude: asyncio.Task[object] | None = None,
) -> int:
    """Cancel pending tasks on ``loop`` so context-manager cleanup can run."""
    running = loop or asyncio.get_running_loop()
    n = 0
    for task in asyncio.all_tasks(running):
        if task.done() or task is exclude:
            continue
        task.cancel()
        n += 1
    return n


def install_sigterm_cancels_asyncio_tasks() -> None:
    """Make SIGTERM cancel asyncio tasks (fleet job cancel) instead of hard-killing.

    ``asyncio.run`` already cancels the main task on SIGINT. SIGTERM keeps the
    default OS handler unless we install this, so Telethon session ``finally``
    blocks never run and leases stay ``active`` until heartbeat TTL.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if loop in _sigterm_loops:
        return

    def _on_sigterm() -> None:
        n = cancel_all_asyncio_tasks(loop)
        log.warning(
            "SIGTERM: cancelling %s asyncio task(s) so session leases can be released",
            n,
        )

    try:
        loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
    except NotImplementedError:
        return
    _sigterm_loops.add(loop)
