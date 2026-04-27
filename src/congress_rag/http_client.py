"""HTTP client with retry and explicit error handling."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .config import ScraperConfig


logger = logging.getLogger(__name__)


class FetchError(RuntimeError):
    """Raised when a URL cannot be fetched with a descriptive message."""


class NotFoundError(FetchError):
    """Raised when a URL returns 404."""


class RetryableFetchError(FetchError):
    """Raised when a fetch can be retried."""


class CongressHttpClient:
    """Async HTTP wrapper for The Reporter lawmaker site."""

    def __init__(self, config: ScraperConfig) -> None:
        self.config = config
        self._request_lock = asyncio.Lock()
        self._next_request_at = 0.0
        self._client = httpx.AsyncClient(
            base_url=config.base_url,
            timeout=httpx.Timeout(config.timeout_seconds),
            http2=True,
            follow_redirects=True,
            headers={
                "User-Agent": config.user_agent,
                "Accept": "application/json,text/html,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate",
            },
        )

    async def aclose(self) -> None:
        """Close the underlying HTTP session."""

        await self._client.aclose()

    async def _wait_for_cooldown(self) -> None:
        """Throttle request start times to avoid traffic bursts."""

        interval = max(0.0, self.config.request_interval_seconds)
        jitter = max(0.0, self.config.request_jitter_seconds)
        if interval == 0 and jitter == 0:
            return

        async with self._request_lock:
            now = time.monotonic()
            wait_seconds = max(0.0, self._next_request_at - now)
            if wait_seconds > 0:
                logger.debug("Cooling down for %.3fs before next request", wait_seconds)
                await asyncio.sleep(wait_seconds)

            cooldown = interval + (random.uniform(0, jitter) if jitter > 0 else 0.0)
            self._next_request_at = time.monotonic() + cooldown

    @retry(
        retry=retry_if_exception_type(RetryableFetchError),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def get(self, url: str, *, headers: dict[str, str] | None = None) -> httpx.Response:
        """Fetch a URL and raise clear errors for bad statuses."""

        logger.debug("GET %s", url)
        await self._wait_for_cooldown()
        try:
            response = await self._client.get(url, headers=headers)
        except (httpx.TimeoutException, httpx.TransportError) as error:
            raise RetryableFetchError(f"Failed to fetch {url}: {error}") from error

        if response.status_code == 404:
            raise NotFoundError(f"URL returned 404: {url}")
        if response.status_code >= 500:
            raise RetryableFetchError(
                f"Server error {response.status_code} while fetching {url}"
            )
        if response.status_code >= 400:
            raise FetchError(
                f"Client error {response.status_code} while fetching {url}: "
                f"{response.text[:300]}"
            )
        return response

    async def get_text(self, url: str, *, headers: dict[str, str] | None = None) -> str:
        """Fetch a URL and return response text."""

        response = await self.get(url, headers=headers)
        return response.text

    async def get_bytes(self, url: str, *, headers: dict[str, str] | None = None) -> bytes:
        """Fetch a URL and return response bytes."""

        response = await self.get(url, headers=headers)
        return response.content

    async def get_json(self, url: str) -> dict[str, Any]:
        """Fetch a JSON endpoint and return the decoded object."""

        response = await self.get(url, headers={"Accept": "application/json"})
        try:
            payload = response.json()
        except ValueError as error:
            raise FetchError(f"Expected JSON from {url}, got invalid response") from error
        if not isinstance(payload, dict):
            raise FetchError(f"Expected JSON object from {url}, got {type(payload).__name__}")
        return payload


@asynccontextmanager
async def congress_http_client(config: ScraperConfig) -> AsyncIterator[CongressHttpClient]:
    """Context manager that closes the HTTP client reliably."""

    client = CongressHttpClient(config)
    try:
        yield client
    finally:
        await client.aclose()
