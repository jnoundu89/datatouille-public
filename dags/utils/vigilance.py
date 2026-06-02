import base64
import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

import requests
from psycopg2.extras import execute_values

from utils.fs import load_json_file

logger = logging.getLogger(__name__)

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Origin": "https://vigilance.meteofrance.fr",
    "Referer": "https://vigilance.meteofrance.fr/",
}


def is_token_valid(token: str) -> bool:
    """Check if JWT token is valid (not expired)."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return False

        payload = parts[1]
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding

        payload_data = json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))
        exp_timestamp = payload_data.get("exp")

        if not exp_timestamp:
            logger.warning("Token has no expiration field")
            return True

        return datetime.now(UTC).timestamp() < (exp_timestamp - 300)
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error("Token validation error: %s", e)
        return False


def extract_jwt_token_scrapling(url: str) -> str | None:
    """Extract JWT token from Meteo France website using Scrapling (Playwright)."""
    try:
        from scrapling import StealthyFetcher

        logger.info("Loading %s with Scrapling StealthyFetcher", url)
        fetcher = StealthyFetcher()
        page = fetcher.fetch(url)

        page_source = page.html_content if hasattr(page, "html_content") else ""
        if not page_source and hasattr(page, "body") and page.body:
            page_source = page.body.decode("utf-8") if isinstance(page.body, bytes) else str(page.body)
        token_match = re.search(
            r"token=([A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)",
            page_source,
        )

        if token_match:
            token = token_match.group(1)
            logger.info("JWT token extracted successfully via Scrapling")
            return token

        logger.warning("No token found in page source")
    except Exception as e:
        logger.error("Scrapling extraction failed: %s", e)
    return None


def fetch_vigilance_alerts(url: str, token: str, params: dict) -> dict | None:
    """Fetch vigilance warnings from Météo France API using Bearer Token."""
    headers = {**REQUEST_HEADERS, "Authorization": f"Bearer {token}"}
    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("API request failed: %s", e)
        return None


def map_alerts_to_stores(
    alerts: list[dict[str, Any]], store_mapping: dict[str, Any], date_str: str, extracted_at: str
) -> list[dict[str, Any]]:
    """Map meteorological alerts to Store / Store locations."""
    dept_to_gie: dict[str, list[str]] = {}
    for store_name, dept_code in store_mapping.items():
        if dept_code not in dept_to_gie:
            dept_to_gie[dept_code] = []
        dept_to_gie[dept_code].append(store_name)

    gie_records = []
    for alert in alerts:
        dept_code = alert["department_code"]
        stores = dept_to_gie.get(dept_code, [])
        for store_name in stores:
            gie_records.append(
                {
                    "store_name": store_name,
                    "department_code": alert["department_code"],
                    "department_name": alert["department_name"],
                    "phenomenon_name": alert["phenomenon_name"],
                    "begin_time": alert["begin_time"],
                    "end_time": alert["end_time"],
                    "alert_level": alert["color_name"],
                    "color_hex": alert["color_hex"],
                    "extracted_at": extracted_at,
                    "extraction_date": date_str,
                }
            )
    return gie_records


def insert_vigilance_alerts(cur, alerts: list[dict[str, Any]]) -> None:
    """Insert warnings into vigilance_alerts PostgreSQL table."""
    if not alerts:
        return

    alert_values = [
        (
            r["department_code"],
            r["department_name"],
            r["phenomenon_id"],
            r["phenomenon_name"],
            r["begin_time"],
            r["end_time"],
            r["color_id"],
            r["color_name"],
            r["color_level"],
            r["color_hex"],
            r["extracted_at"],
            r["extraction_date"],
        )
        for r in alerts
    ]

    execute_values(
        cur,
        """
        INSERT INTO vigilance_alerts (
            department_code, department_name, phenomenon_id, phenomenon_name,
            begin_time, end_time, color_id, color_name, color_level,
            color_hex, extracted_at, extraction_date
        ) VALUES %s
        ON CONFLICT (department_code, phenomenon_id, begin_time, extraction_date)
        DO UPDATE SET
            department_name = EXCLUDED.department_name,
            end_time = EXCLUDED.end_time,
            color_id = EXCLUDED.color_id,
            color_name = EXCLUDED.color_name,
            color_level = EXCLUDED.color_level,
            color_hex = EXCLUDED.color_hex,
            extracted_at = EXCLUDED.extracted_at
        """,
        alert_values,
    )


def insert_store_alerts(cur, store_alerts: list[dict[str, Any]]) -> None:
    """Insert store warnings into vigilance_store_alerts PostgreSQL table."""
    if not store_alerts:
        return

    gie_values = [
        (
            r["store_name"],
            r["department_code"],
            r["department_name"],
            r["phenomenon_name"],
            r["begin_time"],
            r["end_time"],
            r["alert_level"],
            r["color_hex"],
            r["extracted_at"],
            r["extraction_date"],
        )
        for r in store_alerts
    ]

    execute_values(
        cur,
        """
        INSERT INTO vigilance_store_alerts (
            store_name, department_code, department_name, phenomenon_name,
            begin_time, end_time, alert_level, color_hex,
            extracted_at, extraction_date
        ) VALUES %s
        ON CONFLICT (store_name, phenomenon_name, begin_time, extraction_date)
        DO UPDATE SET
            department_code = EXCLUDED.department_code,
            department_name = EXCLUDED.department_name,
            end_time = EXCLUDED.end_time,
            alert_level = EXCLUDED.alert_level,
            color_hex = EXCLUDED.color_hex,
            extracted_at = EXCLUDED.extracted_at
        """,
        gie_values,
    )


def populate_vigilance_metadata(cur, dictionary_path: str) -> None:
    """Populate phenomenons and colors metadata tables (idempotent)."""
    dictionary = load_json_file(dictionary_path) or {}
    for p in dictionary.get("phenomenons", []):
        cur.execute(
            """
            INSERT INTO vigilance_metadata (type, code, name, level, hex_code)
            VALUES ('phenomenon', %s, %s, NULL, NULL)
            ON CONFLICT (type, code) DO UPDATE SET name = EXCLUDED.name
            """,
            (p["id"], p["name"]),
        )
    for c in dictionary.get("colors", []):
        cur.execute(
            """
            INSERT INTO vigilance_metadata (type, code, name, level, hex_code)
            VALUES ('color', %s, %s, %s, %s)
            ON CONFLICT (type, code) DO UPDATE SET
                name = EXCLUDED.name, level = EXCLUDED.level, hex_code = EXCLUDED.hex_code
            """,
            (c["id"], c["name"], c["level"], c["hexaCode"]),
        )
