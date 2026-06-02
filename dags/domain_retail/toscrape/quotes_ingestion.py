"""
Toscrape Quotes Ingestion DAG (Modularized Version)

Scrapes quotes and author biographies from quotes.toscrape.com using
Scrapling StealthyFetcher on the JavaScript-rendered endpoint (/js-delayed)
to demonstrate anti-bot bypass capabilities.

Pipeline: create_bucket > extract_quotes > extract_authors > load_to_postgres
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from utils.db import pg_connection
from utils.metrics import track_extraction
from utils.s3 import download_json, ensure_bucket, get_s3_client, upload_json
from utils.toscrape import (
    _parse_author_page,
    _parse_quotes_page,
    _quote_hash,
    insert_toscrape_authors,
    insert_toscrape_quotes,
)

logger = logging.getLogger(__name__)

BUCKET = "toscrape-quotes"
DAG_ID = "toscrape_quotes_ingestion"
BASE_URL = "https://quotes.toscrape.com"
TOTAL_PAGES = 10


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 4 * * 0",  # Sunday 04:00 UTC
    catchup=False,
    tags=["toscrape", "quotes", "ingestion", "scraping", "scrapling"],
    description="Weekly quotes + authors scraping from quotes.toscrape.com (Scrapling JS)",
    default_args={
        "owner": "data-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    doc_md=__doc__,
)
def toscrape_quotes_ingestion():
    """Toscrape quotes ingestion DAG."""

    @task
    def create_bucket() -> dict[str, str]:
        """Create MinIO bucket if not exists (idempotent)."""
        return ensure_bucket(BUCKET)

    @task
    def extract_quotes(bucket_result: dict, **context) -> dict[str, Any]:
        """Fetch all 10 quote pages via Scrapling StealthySession (JS rendering)."""
        from scrapling.fetchers import StealthySession

        logical_date = context.get("logical_date") or datetime.now(UTC)
        date_str = logical_date.strftime("%Y-%m-%d")
        extracted_at = logical_date.isoformat()

        with track_extraction("toscrape_quotes", DAG_ID, date_str, min_records=50) as metrics:
            all_quotes: list[dict[str, Any]] = []
            raw_pages: dict[str, str] = {}

            with StealthySession(headless=True) as session:
                for page_num in range(1, TOTAL_PAGES + 1):
                    url = f"{BASE_URL}/js/page/{page_num}/"
                    logger.info("Fetching quotes page %d/%d via Scrapling", page_num, TOTAL_PAGES)
                    try:
                        page = session.fetch(url, wait_selector="div.quote")
                        html_bytes = page.body
                        raw_pages[f"page_{page_num}"] = html_bytes

                        quotes = _parse_quotes_page(
                            html_bytes.decode("utf-8") if isinstance(html_bytes, bytes) else html_bytes
                        )
                        for q in quotes:
                            q["quote_hash"] = _quote_hash(q["text"], q["author_name"])
                            q["extracted_at"] = extracted_at
                            q["extraction_date"] = date_str
                        all_quotes.extend(quotes)
                        logger.info("Page %d: %d quotes", page_num, len(quotes))
                    except Exception as e:
                        logger.error("Failed to fetch page %d: %s", page_num, e)
                        metrics.errors += 1

            metrics.items_extracted = len(all_quotes)

            # Deduplicate by hash
            seen: set[str] = set()
            unique_quotes: list[dict[str, Any]] = []
            for q in all_quotes:
                if q["quote_hash"] not in seen:
                    seen.add(q["quote_hash"])
                    unique_quotes.append(q)

            metrics.items_loaded = len(unique_quotes)

            # Store raw HTML pages
            s3 = get_s3_client()
            for page_name, raw_body in raw_pages.items():
                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"raw/{date_str}/{page_name}.html",
                    Body=raw_body if isinstance(raw_body, bytes) else raw_body.encode("utf-8"),
                    ContentType="text/html",
                )

            # Store landing
            landing_key = f"landing/{date_str}/quotes.json"
            upload_json(BUCKET, landing_key, unique_quotes)
            logger.info("Stored %d unique quotes", len(unique_quotes))

        author_slugs = list({q["author_slug"] for q in unique_quotes if q.get("author_slug")})
        return {
            "landing_key": landing_key,
            "author_slugs": author_slugs,
            "count": len(unique_quotes),
            "date": date_str,
        }

    @task
    def extract_authors(quotes_result: dict[str, Any], **context) -> str:
        """Fetch author biography pages via Scrapling."""
        from scrapling.fetchers import StealthySession

        logical_date = context.get("logical_date") or datetime.now(UTC)
        date_str = logical_date.strftime("%Y-%m-%d")
        extracted_at = logical_date.isoformat()

        author_slugs = quotes_result["author_slugs"]
        authors: list[dict[str, Any]] = []

        with StealthySession(headless=True) as session:
            for slug in author_slugs:
                url = f"{BASE_URL}/author/{slug}/"
                logger.info("Fetching author: %s", slug)
                try:
                    page = session.fetch(url)
                    body = page.body
                    author = _parse_author_page(body.decode("utf-8") if isinstance(body, bytes) else body, slug)
                    if author:
                        author["extracted_at"] = extracted_at
                        author["extraction_date"] = date_str
                        authors.append(author)
                except Exception as e:
                    logger.warning("Failed to fetch author %s: %s", slug, e)

        if not authors:
            raise RuntimeError(f"Zero authors extracted (expected ~{len(author_slugs)})")

        landing_key = f"landing/{date_str}/authors.json"
        upload_json(BUCKET, landing_key, authors)
        logger.info("Stored %d authors", len(authors))
        return landing_key

    @task
    def load_to_postgres(
        quotes_result: dict[str, Any],
        authors_key: str,
    ) -> dict[str, int]:
        """Load quotes and authors to PostgreSQL."""
        quotes = download_json(BUCKET, quotes_result["landing_key"])
        authors = download_json(BUCKET, authors_key)

        with pg_connection() as conn:
            with conn.cursor() as cur:
                insert_toscrape_quotes(cur, quotes, quotes_result["date"], datetime.now(UTC).isoformat())
                insert_toscrape_authors(cur, authors, quotes_result["date"], datetime.now(UTC).isoformat())

        logger.info(
            "Loaded %d quotes and %d authors to PostgreSQL",
            len(quotes),
            len(authors),
        )
        return {"quotes": len(quotes), "authors": len(authors)}

    bucket = create_bucket()
    quotes = extract_quotes(bucket)
    authors = extract_authors(quotes)
    load_to_postgres(quotes, authors)


toscrape_quotes_dag = toscrape_quotes_ingestion()
