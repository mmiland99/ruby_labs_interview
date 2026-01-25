import asyncio
import logging
from typing import Any, Optional

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

BASE_URL = "https://jsonplaceholder.typicode.com"
TIMEOUT_SECONDS = 15


class RetryableError(Exception):
    # Raised for failures that should be retried (429, 5xx).
    pass


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, RetryableError)),
)
async def fetch_json(
    session: aiohttp.ClientSession,
    path: str,
    params: Optional[dict[str, Any]] = None,
) -> Any:
    url = BASE_URL.rstrip("/") + "/" + path.lstrip("/")
    logger.info("API GET start: %s params=%s", url, params)

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    async with session.get(url, params=params, timeout=timeout) as resp:
        text = await resp.text()

        if resp.status == 429 or 500 <= resp.status <= 599:
            logger.warning("API GET transient status %s for %s", resp.status, url)
            raise RetryableError(f"HTTP {resp.status}: {text[:200]}")

        if resp.status < 200 or resp.status >= 300:
            raise RuntimeError(f"HTTP {resp.status} for {url}: {text[:200]}")

        data = await resp.json()
        logger.info("API GET done: %s", url)
        return data