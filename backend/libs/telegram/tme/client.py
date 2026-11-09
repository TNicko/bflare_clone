from __future__ import annotations

import asyncio
import logging

import httpx

from telegram.tme.classifier import parse_tme_html
from telegram.tme.models import TmeProfile

logger = logging.getLogger(__name__)


class TmeClient:
    def __init__(
        self,
        *,
        user_agent: str,
        timeout_seconds: float,
        concurrency: int,
        delay_ms: int,
        max_retries: int,
    ) -> None:
        self._user_agent = user_agent
        self._timeout = timeout_seconds
        self._semaphore = asyncio.Semaphore(concurrency)
        self._delay_seconds = delay_ms / 1000.0
        self._max_retries = max_retries
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> TmeClient:
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            headers={"User-Agent": self._user_agent},
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def fetch_profile(self, username: str) -> TmeProfile:
        assert self._client is not None
        url = f"https://t.me/{username}"
        last_error: str | None = None

        for attempt in range(self._max_retries):
            async with self._semaphore:
                if self._delay_seconds > 0:
                    await asyncio.sleep(self._delay_seconds)
                try:
                    response = await self._client.get(url)
                except httpx.HTTPError as exc:
                    last_error = str(exc)
                    await self._backoff(attempt)
                    continue

                if response.status_code in {429, 403, 503}:
                    last_error = f"HTTP {response.status_code}"
                    await self._backoff(attempt)
                    continue

                profile = parse_tme_html(
                    username,
                    response.text,
                    http_status=response.status_code,
                )
                if response.status_code >= 400 and profile.classification == "error":
                    profile.classification = "error"
                    last_error = f"HTTP {response.status_code}"
                    await self._backoff(attempt)
                    continue
                return profile

        logger.warning("t.me fetch failed for @%s: %s", username, last_error)
        profile = TmeProfile(username=username, classification="error")
        return profile

    async def fetch_profiles(
        self,
        usernames: list[str],
        *,
        batch_label: str = "",
    ) -> dict[str, TmeProfile]:
        prefix = f"{batch_label} " if batch_label else ""
        results: dict[str, TmeProfile] = {}
        tasks = {asyncio.create_task(self.fetch_profile(username)): username for username in usernames}
        for index, task in enumerate(asyncio.as_completed(tasks), start=1):
            profile = await task
            results[profile.username.lower()] = profile
            logger.info(
                "%st.me progress %s/%s (@%s -> %s)",
                prefix,
                index,
                len(usernames),
                profile.username,
                profile.classification,
            )
        return results

    async def _backoff(self, attempt: int) -> None:
        await asyncio.sleep(min(60.0, 2.0**attempt))
