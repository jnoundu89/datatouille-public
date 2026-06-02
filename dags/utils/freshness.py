"""Data freshness assessment with intelligence gap warnings.

Adapted from worldmonitor data-freshness.ts. Key innovation: don't just say
"source X is down", explain WHAT IS MISSING ("price comparisons may be stale").
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)


class FreshnessState(Enum):
    LIVE = "live"
    DELAYED = "delayed"
    STALE = "stale"
    CRITICAL = "critical"
    UNAVAILABLE = "unavailable"


@dataclass
class SourceDefinition:
    """Defines expected freshness for a data source."""

    table: str
    interval: timedelta
    gap_description: str


# Expected freshness intervals and gap warnings per source
SOURCE_DEFINITIONS: dict[str, SourceDefinition] = {
    "allkeyshop": SourceDefinition(
        table="allkeyshop_top50",
        interval=timedelta(hours=24),
        gap_description="Comparaisons de prix obsoletes",
    ),
    "dropreference": SourceDefinition(
        table="dropreference_products",
        interval=timedelta(hours=12),
        gap_description="Stock PC non fiable",
    ),
    "itad": SourceDefinition(
        table="itad_deals",
        interval=timedelta(hours=24),
        gap_description="Deals potentiellement expires",
    ),
    "anime_catalog": SourceDefinition(
        table="anime_catalog",
        interval=timedelta(days=7),
        gap_description="Nouveaux animes manquants",
    ),
    "anime_planning": SourceDefinition(
        table="anime_releases",
        interval=timedelta(days=7),
        gap_description="Planning hebdo obsolete",
    ),
    "vigilance_meteo": SourceDefinition(
        table="vigilance_alerts",
        interval=timedelta(hours=2),
        gap_description="Alertes meteo potentiellement ratees",
    ),
    "georisques": SourceDefinition(
        table="georisques_commune_profiles",
        interval=timedelta(days=30),
        gap_description="Profils risques pas a jour",
    ),
    "leboncoin": SourceDefinition(
        table="leboncoin_listings",
        interval=timedelta(hours=24),
        gap_description="Nouvelles annonces ratees",
    ),
}


def assess_freshness(
    source: str,
    last_extraction: datetime | None,
    now: datetime | None = None,
) -> dict:
    """Assess freshness state for a source.

    Args:
        source: Source name (must be in SOURCE_DEFINITIONS).
        last_extraction: Timestamp of last extraction (None = never extracted).
        now: Current time (defaults to UTC now).

    Returns:
        Dict with state, gap_description, age_seconds, and threshold info.
    """
    now = now or datetime.now(UTC)
    definition = SOURCE_DEFINITIONS.get(source)

    if definition is None:
        return {
            "source": source,
            "state": FreshnessState.UNAVAILABLE.value,
            "gap_description": f"Source '{source}' not configured",
            "age_seconds": None,
        }

    if last_extraction is None:
        return {
            "source": source,
            "state": FreshnessState.UNAVAILABLE.value,
            "gap_description": definition.gap_description,
            "age_seconds": None,
        }

    # Handle naive datetimes from TIMESTAMP columns (assume UTC)
    if last_extraction.tzinfo is None:
        last_extraction = last_extraction.replace(tzinfo=UTC)

    age = now - last_extraction
    age_seconds = age.total_seconds()
    interval_seconds = definition.interval.total_seconds()

    # Thresholds: <1x = LIVE, <2x = DELAYED, <4x = STALE, >= 4x = CRITICAL
    if age_seconds <= interval_seconds:
        state = FreshnessState.LIVE
    elif age_seconds <= interval_seconds * 2:
        state = FreshnessState.DELAYED
    elif age_seconds <= interval_seconds * 4:
        state = FreshnessState.STALE
    else:
        state = FreshnessState.CRITICAL

    result = {
        "source": source,
        "state": state.value,
        "age_seconds": round(age_seconds),
        "interval_seconds": round(interval_seconds),
        "gap_description": definition.gap_description if state != FreshnessState.LIVE else None,
        "table": definition.table,
    }

    if state in (FreshnessState.STALE, FreshnessState.CRITICAL):
        logger.warning("Source %s is %s (age: %ds, expected: %ds)", source, state.value, age_seconds, interval_seconds)

    return result


def check_all_freshness() -> list[dict]:
    """Check freshness for all configured sources by querying MAX(extracted_at).

    Returns:
        List of freshness assessment dicts.
    """
    from utils.db import pg_connection

    results = []

    with pg_connection(autocommit=True) as conn:
        with conn.cursor() as cur:
            for source, definition in SOURCE_DEFINITIONS.items():
                try:
                    cur.execute(
                        f"SELECT MAX(extracted_at) FROM {definition.table}",  # noqa: S608
                    )
                    row = cur.fetchone()
                    last_extraction = row[0] if row and row[0] else None
                except Exception as e:
                    logger.warning("Failed to check freshness for %s: %s", source, e)
                    last_extraction = None

                results.append(assess_freshness(source, last_extraction))

    return results
