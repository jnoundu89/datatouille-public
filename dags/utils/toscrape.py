import hashlib
import json
import logging
import re
from typing import Any

from bs4 import BeautifulSoup
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

STAR_MAP = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5}
BASE_URL_BOOKS = "https://books.toscrape.com"


def _parse_listing_page(html: str) -> list[dict[str, str]]:
    """Extract book URLs and basic info from a listing page."""
    soup = BeautifulSoup(html, "lxml")
    books = []
    for article in soup.find_all("article", class_="product_pod"):
        link = article.find("h3").find("a")
        if not link:
            continue
        href = link.get("href", "")
        if href.startswith("catalogue/"):
            url = f"{BASE_URL_BOOKS}/{href}"
        elif href.startswith("../"):
            clean = re.sub(r"^(\.\./)+", "", href)
            url = f"{BASE_URL_BOOKS}/catalogue/{clean}"
        else:
            url = f"{BASE_URL_BOOKS}/catalogue/{href}"
        books.append({"url": url, "title": link.get("title", "")})
    return books


def _parse_detail_page(
    html: str,
    url: str,
    extraction_date: str,
    extracted_at: str,
) -> dict[str, Any] | None:
    """Parse a book detail page and extract all fields."""
    soup = BeautifulSoup(html, "lxml")
    try:
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""

        table = soup.find("table", class_="table-striped")
        info = {}
        if table:
            for row in table.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if th and td:
                    info[th.get_text(strip=True)] = td.get_text(strip=True)

        upc = info.get("UPC", "")
        if not upc:
            return None

        def parse_price(val: str) -> float | None:
            match = re.search(r"[\d.]+", val)
            return float(match.group()) if match else None

        rating_elem = soup.find("p", class_="star-rating")
        star_rating = None
        if rating_elem:
            classes = rating_elem.get("class", [])
            for cls in classes:
                if cls.lower() in STAR_MAP:
                    star_rating = STAR_MAP[cls.lower()]
                    break

        avail_text = info.get("Availability", "")
        stock_match = re.search(r"\((\d+) available\)", avail_text)
        stock_count = int(stock_match.group(1)) if stock_match else 0

        breadcrumbs = soup.find("ul", class_="breadcrumb")
        category = None
        if breadcrumbs:
            crumbs = breadcrumbs.find_all("li")
            if len(crumbs) >= 3:
                category = crumbs[2].get_text(strip=True)

        desc_elem = soup.find("div", id="product_description")
        description = None
        if desc_elem:
            p = desc_elem.find_next_sibling("p")
            if p:
                description = p.get_text(strip=True)[:2000]

        img = soup.find("div", class_="item active")
        image_url = None
        if img:
            img_tag = img.find("img")
            if img_tag:
                src = img_tag.get("src", "")
                clean_src = re.sub(r"^(\.\./)+", "", src)
                image_url = f"{BASE_URL_BOOKS}/{clean_src}"

        return {
            "upc": upc,
            "title": title[:500],
            "price": parse_price(info.get("Price (excl. tax)", "")),
            "price_incl_tax": parse_price(info.get("Price (incl. tax)", "")),
            "tax": parse_price(info.get("Tax", "")),
            "availability": avail_text[:50],
            "stock_count": stock_count,
            "star_rating": star_rating,
            "category": category,
            "description": description,
            "image_url": image_url,
            "product_url": url,
            "extracted_at": extracted_at,
            "extraction_date": extraction_date,
        }
    except (AttributeError, ValueError, TypeError) as e:
        logger.warning("Error parsing detail page %s: %s", url, e)
    return None


def _quote_hash(text: str, author: str) -> str:
    """Generate a deterministic hash for deduplication."""
    return hashlib.sha256(f"{text}|{author}".encode()).hexdigest()[:16]


def _parse_quotes_page(html: str) -> list[dict[str, Any]]:
    """Extract quotes from a page HTML."""
    soup = BeautifulSoup(html, "lxml")
    quotes = []
    for div in soup.find_all("div", class_="quote"):
        text_elem = div.find("span", class_="text")
        author_elem = div.find("small", class_="author")
        if not text_elem or not author_elem:
            continue

        text = text_elem.get_text(strip=True).strip("\u201c\u201d")
        author_name = author_elem.get_text(strip=True)

        author_link = div.find("a", href=re.compile(r"/author/"))
        author_slug = None
        if author_link:
            href = author_link.get("href", "")
            slug_match = re.search(r"/author/([^/]+)", href)
            if slug_match:
                author_slug = slug_match.group(1)
        if not author_slug and author_name:
            author_slug = author_name.replace(" ", "-").replace(".", "")

        tags = [tag.get_text(strip=True) for tag in div.find_all("a", class_="tag")]

        quotes.append(
            {
                "text": text,
                "author_name": author_name,
                "author_slug": author_slug,
                "tags": json.dumps(tags, ensure_ascii=False),
            }
        )
    return quotes


def _parse_author_page(html: str, slug: str) -> dict[str, Any] | None:
    """Extract author biography from detail page."""
    soup = BeautifulSoup(html, "lxml")
    try:
        name_elem = soup.find("h3", class_="author-title")
        name = name_elem.get_text(strip=True) if name_elem else slug

        born_date_elem = soup.find("span", class_="author-born-date")
        born_date = born_date_elem.get_text(strip=True) if born_date_elem else None

        born_loc_elem = soup.find("span", class_="author-born-location")
        born_location = born_loc_elem.get_text(strip=True) if born_loc_elem else None
        if born_location and born_location.startswith("in "):
            born_location = born_location[3:]

        desc_elem = soup.find("div", class_="author-description")
        description = desc_elem.get_text(strip=True)[:2000] if desc_elem else None

        return {
            "slug": slug,
            "name": name[:200],
            "born_date": born_date,
            "born_location": born_location[:500],
            "description": description,
        }
    except (AttributeError, ValueError, TypeError) as e:
        logger.warning("Error parsing author details for %s: %s", slug, e)
    return None


def insert_toscrape_books(cur, books: list[dict[str, Any]]) -> None:
    """Insert structured book details into toscrape_books PostgreSQL table."""
    if not books:
        return

    book_values = [
        (
            r["upc"],
            r["title"],
            r["price"],
            r["price_incl_tax"],
            r["tax"],
            r["availability"],
            r["stock_count"],
            r["star_rating"],
            r["category"],
            r["description"],
            r["image_url"],
            r["product_url"],
            r["extracted_at"],
            r["extraction_date"],
        )
        for r in books
    ]

    execute_values(
        cur,
        """
        INSERT INTO toscrape_books (
            upc, title, price, price_incl_tax, tax, availability, stock_count,
            star_rating, category, description, image_url, product_url,
            extracted_at, extraction_date
        ) VALUES %s
        ON CONFLICT (upc, extraction_date)
        DO UPDATE SET
            title = EXCLUDED.title,
            price = EXCLUDED.price,
            price_incl_tax = EXCLUDED.price_incl_tax,
            tax = EXCLUDED.tax,
            availability = EXCLUDED.availability,
            stock_count = EXCLUDED.stock_count,
            star_rating = EXCLUDED.star_rating,
            category = EXCLUDED.category,
            description = EXCLUDED.description,
            image_url = EXCLUDED.image_url,
            product_url = EXCLUDED.product_url,
            extracted_at = EXCLUDED.extracted_at
        """,
        book_values,
    )


def insert_toscrape_quotes(cur, quotes: list[dict[str, Any]], date_str: str, extracted_at: str) -> None:
    """Insert quotes records into toscrape_quotes PostgreSQL table."""
    if not quotes:
        return

    quote_values = [
        (
            _quote_hash(r["text"], r["author_name"]),
            r["text"],
            r["author_name"],
            r["author_slug"],
            r["tags"],
            extracted_at,
            date_str,
        )
        for r in quotes
    ]

    execute_values(
        cur,
        """
        INSERT INTO toscrape_quotes (
            quote_id, quote_text, author_name, author_slug, tags,
            extracted_at, extraction_date
        ) VALUES %s
        ON CONFLICT (quote_id, extraction_date)
        DO UPDATE SET
            quote_text = EXCLUDED.quote_text,
            author_name = EXCLUDED.author_name,
            author_slug = EXCLUDED.author_slug,
            tags = EXCLUDED.tags,
            extracted_at = EXCLUDED.extracted_at
        """,
        quote_values,
    )


def insert_toscrape_authors(cur, authors: list[dict[str, Any]], date_str: str, extracted_at: str) -> None:
    """Insert author biographies into toscrape_authors PostgreSQL table."""
    if not authors:
        return

    author_values = [
        (
            r["slug"],
            r["name"],
            r["born_date"],
            r["born_location"],
            r["description"],
            extracted_at,
            date_str,
        )
        for r in authors
    ]

    execute_values(
        cur,
        """
        INSERT INTO toscrape_authors (
            author_slug, author_name, born_date, born_location, description,
            extracted_at, extraction_date
        ) VALUES %s
        ON CONFLICT (author_slug, extraction_date)
        DO UPDATE SET
            author_name = EXCLUDED.author_name,
            born_date = EXCLUDED.born_date,
            born_location = EXCLUDED.born_location,
            description = EXCLUDED.description,
            extracted_at = EXCLUDED.extracted_at
        """,
        author_values,
    )
