"""
Toscrape Books Ingestion DAG (Modularized Version)

Scrapes the complete book catalog from books.toscrape.com (1000 books across
50 listing pages + 1000 detail pages) using requests + BeautifulSoup.

Pipeline: create_bucket > extract_listings > extract_details > load_to_postgres
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from utils.db import pg_connection
from utils.http import fetch_page, get_http_session
from utils.metrics import track_extraction
from utils.s3 import download_json, ensure_bucket, get_partitioned_key, upload_json, upload_parquet
from utils.toscrape import (
    _parse_detail_page,
    _parse_listing_page,
    insert_toscrape_books,
)
from utils.validation import BookRecord, validate_records

logger = logging.getLogger(__name__)

BUCKET = "toscrape-books"
DAG_ID = "toscrape_books_ingestion"
BASE_URL = "https://books.toscrape.com"
TOTAL_PAGES = 50
MAX_WORKERS = 5


def _fetch_concurrent(
    urls: list[str],
    session: Any,
    max_workers: int,
) -> dict[str, str]:
    """Fetch multiple URLs concurrently. Returns url->html mapping."""
    results: dict[str, str] = {}

    def fetch_one(url: str) -> tuple[str, str | None]:
        try:
            return (url, fetch_page(url, session, delay=0.2, jitter=0.3))
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return (url, None)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_one, url): url for url in urls}
        for future in as_completed(futures):
            url, html = future.result()
            if html:
                results[url] = html

    return results


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 3 * * 0",  # Sunday 03:00 UTC
    catchup=False,
    tags=["toscrape", "books", "ingestion", "scraping"],
    description="Weekly book catalog scraping from books.toscrape.com",
    default_args={
        "owner": "data-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    doc_md=__doc__,
)
def toscrape_books_ingestion():
    """Toscrape books ingestion DAG."""

    @task
    def create_bucket() -> dict[str, str]:
        """Create MinIO bucket if not exists (idempotent)."""
        return ensure_bucket(BUCKET)

    @task
    def extract_listings(bucket_result: dict, **context) -> dict[str, Any]:
        """Fetch all 50 listing pages and collect book URLs."""
        logical_date = context.get("logical_date") or datetime.now(UTC)
        date_str = logical_date.strftime("%Y-%m-%d")

        session = get_http_session(max_retries=3, pool_size=MAX_WORKERS)
        urls = [f"{BASE_URL}/catalogue/page-{i}.html" for i in range(1, TOTAL_PAGES + 1)]
        pages = _fetch_concurrent(urls, session, MAX_WORKERS)
        session.close()

        all_books: list[dict[str, str]] = []
        seen_urls: set[str] = set()
        for page_url in urls:
            html = pages.get(page_url)
            if not html:
                continue
            for book in _parse_listing_page(html):
                if book["url"] not in seen_urls:
                    seen_urls.add(book["url"])
                    all_books.append(book)

        if not all_books:
            raise RuntimeError("Zero books discovered from listing pages")

        # Store raw listing data
        raw_key = get_partitioned_key(
            domain="retail", source="toscrape_books", filename="raw_book_urls.json.gz", date_str=date_str
        )
        upload_json(BUCKET, raw_key, all_books)
        logger.info("Discovered %d unique book URLs", len(all_books))
        return {"raw_key": raw_key, "count": len(all_books), "date": date_str}

    @task
    def extract_details(listing_result: dict[str, Any], **context) -> str:
        """Fetch all book detail pages and parse into landing records."""
        logical_date = context.get("logical_date") or datetime.now(UTC)
        date_str = logical_date.strftime("%Y-%m-%d")
        extracted_at = logical_date.isoformat()

        book_urls_data = download_json(BUCKET, listing_result["raw_key"])
        book_urls = [b["url"] for b in book_urls_data]

        with track_extraction("toscrape_books", DAG_ID, date_str, min_records=100) as metrics:
            session = get_http_session(max_retries=3, pool_size=MAX_WORKERS)

            # Fetch in batches of 100
            all_records: list[dict[str, Any]] = []
            batch_size = 100
            for i in range(0, len(book_urls), batch_size):
                batch = book_urls[i : i + batch_size]
                logger.info(
                    "Fetching detail batch %d/%d",
                    i // batch_size + 1,
                    (len(book_urls) + batch_size - 1) // batch_size,
                )
                pages = _fetch_concurrent(batch, session, MAX_WORKERS)
                for url, html in pages.items():
                    record = _parse_detail_page(html, url, date_str, extracted_at)
                    if record:
                        all_records.append(record)

            session.close()
            metrics.items_extracted = len(book_urls)
            metrics.items_loaded = len(all_records)

            # Deduplicate by UPC
            seen_upc: set[str] = set()
            unique_records: list[dict[str, Any]] = []
            for r in all_records:
                if r["upc"] not in seen_upc:
                    seen_upc.add(r["upc"])
                    unique_records.append(r)

            landing_key = get_partitioned_key(
                domain="retail", source="toscrape_books", filename="books.json.gz", date_str=date_str
            )
            upload_json(BUCKET, landing_key, unique_records)
            logger.info("Stored %d unique books to landing", len(unique_records))

            # Medallion Silver: Validate and upload as staging Parquet file
            valid_records = validate_records(unique_records, BookRecord)
            upload_parquet("datatouille", "landing/stg_toscrape_books.parquet", valid_records)

        return landing_key

    @task
    def load_to_postgres(landing_key: str) -> int:
        """Load books into PostgreSQL via upsert on UPC."""
        records = download_json(BUCKET, landing_key)
        if not records:
            raise RuntimeError("No book records to load")

        with pg_connection() as conn:
            with conn.cursor() as cur:
                insert_toscrape_books(cur, records)

        logger.info("Upserted %d books to PostgreSQL", len(records))
        return len(records)

    bucket = create_bucket()
    listings = extract_listings(bucket)
    details = extract_details(listings)
    load_to_postgres(details)


toscrape_books_dag = toscrape_books_ingestion()
