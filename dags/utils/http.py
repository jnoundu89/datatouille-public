"""HTTP session and fetch utilities.

Replaces the duplicated REQUEST_HEADERS, get_http_session, and fetch_page
patterns found in 6+ DAGs.
"""

import logging
import random
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}


def get_http_session(
    max_retries: int = 3,
    pool_size: int = 5,
    headers: dict[str, str] | None = None,
) -> requests.Session:
    """Create HTTP session with connection pooling and retry logic.

    Args:
        max_retries: Number of retries on failure.
        pool_size: Connection pool size.
        headers: Custom headers (defaults to DEFAULT_HEADERS).
    """
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size * 2,
        max_retries=retry,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(headers or DEFAULT_HEADERS)
    return session


def fetch_page(
    url: str,
    session: requests.Session | None = None,
    delay: float = 0.0,
    jitter: float = 0.0,
    timeout: tuple[int, int] = (10, 30),
    max_retries: int = 3,
) -> str:
    """Fetch a single page with optional delay/jitter and retries.

    Args:
        url: URL to fetch.
        session: Reusable session (creates one-shot request if None).
        delay: Base delay before request in seconds.
        jitter: Random jitter added to delay.
        timeout: (connect_timeout, read_timeout) tuple.
        max_retries: Number of retries on failure.

    Returns:
        HTML content as string.

    Raises:
        requests.RequestException: After all retries exhausted.
    """
    actual_delay = delay + random.uniform(0, jitter)
    if actual_delay > 0:
        time.sleep(actual_delay)

    requester = session or requests
    for attempt in range(max_retries):
        try:
            response = requester.get(
                url,
                headers=DEFAULT_HEADERS if session is None else {},
                timeout=timeout,
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
            if attempt == max_retries - 1:
                raise
            time.sleep(2**attempt)
    return ""
