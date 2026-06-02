"""Scrapling fetcher chain with fallback and optional proxy rotation.

Consolidates the 3 identical implementations of build_fetchers / get_scrapling_fetchers
found in allkeyshop, leboncoin, and dropreference DAGs.

Requires scrapling >= 0.4.0 (v0.3.13 removed humanize/block_images params).
"""

import asyncio
import logging
import random
from typing import Any

from scrapling.fetchers import AsyncFetcher, StealthyFetcher

logger = logging.getLogger(__name__)

# Module-level proxy rotator, lazily initialized from Airflow Variable
_proxy_rotator: Any = None


def get_proxy_rotator() -> Any:
    """Load proxy list from Airflow Variable 'proxy_list' and return a rotator.

    Variable format: comma-separated proxy URLs, e.g.:
        http://proxy1:8080,http://user:pass@proxy2:8080,socks5://proxy3:1080

    Returns None if the variable is not set, empty, or ProxyRotator is unavailable.
    """
    global _proxy_rotator
    if _proxy_rotator is not None:
        return _proxy_rotator

    try:
        from scrapling.fetchers import ProxyRotator
    except ImportError:
        logger.debug("ProxyRotator not available in this scrapling version")
        return None

    try:
        from airflow.sdk import Variable

        raw = Variable.get("proxy_list", default="")
        proxies = [p.strip() for p in raw.split(",") if p.strip()]
        if proxies:
            _proxy_rotator = ProxyRotator(proxies)
            logger.info("ProxyRotator initialized with %d proxies", len(proxies))
            return _proxy_rotator
    except Exception:
        pass
    return None


def build_fetcher_chain(timeout: int = 30000) -> list[tuple[str, type, str, dict]]:
    """Build ordered fetcher configuration for fallback chain.

    Order: StealthyFetcher (Playwright) -> AsyncFetcher (httpx).

    Args:
        timeout: Request timeout in milliseconds.

    Returns:
        List of (name, class, method, kwargs) tuples.
    """
    return [
        (
            "StealthyFetcher",
            StealthyFetcher,
            "async_fetch",
            {
                "timeout": timeout,
                "network_idle": True,
                "disable_resources": False,
            },
        ),
        (
            "AsyncFetcher",
            AsyncFetcher,
            "get",
            {
                "timeout": timeout,
                "stealthy_headers": True,
                "follow_redirects": True,
            },
        ),
    ]


async def fetch_with_fallback(
    url: str,
    timeout: int = 30000,
    delay_min: float = 1.0,
    delay_max: float = 2.0,
    max_retries: int = 3,
    wait_selector: str | None = None,
    source: str | None = None,
    use_proxy: bool = False,
) -> Any | None:
    """Fetch URL using scrapling with fallback chain and retries.

    Tries each fetcher in order. On failure, moves to the next fetcher.
    If a fetcher's circuit breaker is open, it is skipped.
    After exhausting all fetchers, retries from the beginning.

    On 403/429 responses, automatically retries with proxy rotation if
    ``use_proxy=True`` and the ``proxy_list`` Airflow Variable is set.

    Args:
        url: URL to fetch.
        timeout: Request timeout in milliseconds.
        delay_min: Minimum delay between requests.
        delay_max: Maximum delay between requests.
        max_retries: Number of full retry cycles.
        wait_selector: CSS selector to wait for (StealthyFetcher only).
        source: Source name for circuit breaker tracking. If None, no circuit breaker is used.
        use_proxy: Enable proxy rotation on 429/403 responses.

    Returns:
        Page response object or None if all attempts fail.
    """
    from utils.circuit_breaker import CircuitOpenError, get_breaker

    fetchers = build_fetcher_chain(timeout)
    rotator = get_proxy_rotator() if use_proxy else None

    for attempt in range(max_retries):
        for name, fetcher_cls, method, kwargs in fetchers:
            # Check circuit breaker if source is specified
            if source:
                breaker = get_breaker(f"{source}_{name}")
                if breaker.state.value == "open":
                    logger.info("Skipping %s for %s (circuit open)", name, source)
                    continue

            backoff = random.uniform(delay_min, delay_max)
            await asyncio.sleep(backoff)

            try:
                fetch_kwargs = {**kwargs}
                if wait_selector and name == "StealthyFetcher":
                    fetch_kwargs["wait_selector"] = wait_selector

                fetch_fn = getattr(fetcher_cls, method)

                if source:
                    breaker = get_breaker(f"{source}_{name}")
                    page = await breaker.async_call(fetch_fn, url, **fetch_kwargs)
                else:
                    page = await fetch_fn(url, **fetch_kwargs)

                if page and hasattr(page, "status") and page.status in (200, 304):
                    # Reject empty/tiny responses — redirects can yield 200 with no body
                    body = getattr(page, "html_content", None) or getattr(page, "html", None) or ""
                    if not body and hasattr(page, "body") and page.body:
                        body = page.body.decode("utf-8") if isinstance(page.body, bytes) else str(page.body)
                    if len(body.strip()) < 100:
                        logger.warning(
                            "Fetch returned %d but empty body (%d bytes) [%s] for %s",
                            page.status,
                            len(body),
                            name,
                            url,
                        )
                    else:
                        logger.info(
                            "Fetch successful (%d) [%s] for %s",
                            page.status,
                            name,
                            url,
                        )
                        return page

                status = getattr(page, "status", None)
                logger.warning("Fetcher %s returned status %s for %s", name, status, url)

                # On 403/429, try with proxy if available
                if status in (403, 429) and rotator:
                    proxy = rotator.get_next()
                    logger.info("Retrying with proxy %s after %s", proxy, status)
                    fetch_kwargs["proxy"] = proxy
                    page = await fetch_fn(url, **fetch_kwargs)
                    if page and hasattr(page, "status") and page.status in (200, 304):
                        body = getattr(page, "html", None) or getattr(page, "content", None) or ""
                        if len(body.strip()) >= 100:
                            logger.info("Proxy fetch successful (%d) for %s", page.status, url)
                            return page
                        logger.warning("Proxy fetch returned %d but empty body for %s", page.status, url)

            except CircuitOpenError:
                logger.info("Skipping %s for %s (circuit open)", name, source)
                continue
            except Exception as e:
                logger.warning(
                    "Attempt %d: %s failed for %s: %s",
                    attempt + 1,
                    name,
                    url,
                    e,
                )

        # Exponential backoff between full retry cycles
        if attempt < max_retries - 1:
            backoff_time = min(2**attempt + random.uniform(1, 3), 20)
            logger.warning("Retry cycle %d failed, waiting %.1fs", attempt + 1, backoff_time)
            await asyncio.sleep(backoff_time)

    logger.error("All %d retry cycles failed for %s", max_retries, url)
    return None
