"""
Source Health Monitoring DAG

Runs every 30 minutes to assess the health of all data sources:
1. Freshness check (is data up-to-date?)
2. Circuit breaker status (are fetchers failing?)
3. Composite quality score (A-F grading)

Writes results to monitoring tables for Grafana dashboards.
"""

import logging
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task

from utils.circuit_breaker import get_all_statuses
from utils.db import pg_connection
from utils.freshness import check_all_freshness

logger = logging.getLogger(__name__)


@dag(
    dag_id="source_health_monitoring",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["infra", "monitoring", "health"],
    description="Source health monitoring: freshness, circuit breakers, quality scores",
    default_args={
        "owner": "data-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    doc_md=__doc__,
)
def source_health_monitoring():
    """Source health monitoring DAG."""

    @task
    def check_freshness() -> list[dict]:
        """Assess freshness of all configured sources."""
        results = check_all_freshness()
        now = datetime.now(UTC).isoformat()

        with pg_connection() as conn:
            with conn.cursor() as cur:
                for r in results:
                    cur.execute(
                        """
                        INSERT INTO source_freshness (source, state, age_seconds, interval_seconds, gap_description, checked_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            r["source"],
                            r["state"],
                            r.get("age_seconds"),
                            r.get("interval_seconds"),
                            r.get("gap_description"),
                            now,
                        ),
                    )

        logger.info("Checked freshness for %d sources", len(results))
        return results

    @task
    def check_circuit_breakers() -> list[dict]:
        """Capture current circuit breaker states."""
        statuses = get_all_statuses()
        now = datetime.now(UTC).isoformat()

        if not statuses:
            logger.info("No circuit breakers registered")
            return []

        with pg_connection() as conn:
            with conn.cursor() as cur:
                for s in statuses:
                    cur.execute(
                        """
                        INSERT INTO source_circuit_breaker_status
                            (source, state, failure_count, max_failures, cooldown_seconds, checked_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            s["source"],
                            s["state"],
                            s["failure_count"],
                            s["max_failures"],
                            s["cooldown_seconds"],
                            now,
                        ),
                    )

        logger.info("Recorded %d circuit breaker statuses", len(statuses))
        return statuses

    @task
    def compute_quality_scores(
        freshness_results: list[dict],
        breaker_results: list[dict],
    ) -> list[dict]:
        """Compute composite quality scores from freshness + metrics + breakers."""
        from utils.quality_score import compute_quality_score

        now = datetime.now(UTC).isoformat()
        scores = []

        # Build lookup maps
        freshness_map = {r["source"]: r for r in freshness_results}
        breaker_map = {s["source"]: s for s in breaker_results}

        # Get latest extraction metrics per source
        metrics_map: dict[str, dict] = {}
        with pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (source) source, items_extracted, items_loaded, errors
                    FROM source_extraction_metrics
                    ORDER BY source, run_date DESC
                    """
                )
                for row in cur.fetchall():
                    metrics_map[row[0]] = {
                        "items_extracted": row[1],
                        "items_loaded": row[2],
                        "errors": row[3],
                    }

        for source, freshness in freshness_map.items():
            # Freshness ratio: 1.0 = within interval, 0.0 = 4x+ overdue
            age = freshness.get("age_seconds") or 0
            interval = freshness.get("interval_seconds") or 1
            freshness_ratio = max(0.0, 1.0 - (age / (interval * 4)))

            # Volume ratio (from metrics if available)
            metrics = metrics_map.get(source, {})
            extracted = metrics.get("items_extracted", 0)
            volume_ratio = min(1.0, extracted / 10.0) if extracted else 0.5  # assume OK if no data

            # Error rate
            errors = metrics.get("errors", 0)
            total = extracted + errors if extracted else 1
            error_rate = errors / total if total > 0 else 0.0

            # Schema compliance (1.0 for now, can be enriched with Soda results later)
            schema_compliance = 1.0

            # Penalize if circuit breaker is open
            breaker = breaker_map.get(source, {})
            if breaker.get("state") == "open":
                freshness_ratio *= 0.5

            qs = compute_quality_score(
                source=source,
                freshness_ratio=freshness_ratio,
                volume_ratio=volume_ratio,
                error_rate=error_rate,
                schema_compliance=schema_compliance,
            )
            scores.append(
                {
                    "source": qs.source,
                    "score": qs.score,
                    "grade": qs.grade,
                    "components": qs.components,
                }
            )

        # Write to DB
        with pg_connection() as conn:
            with conn.cursor() as cur:
                for s in scores:
                    cur.execute(
                        """
                        INSERT INTO source_quality_scores
                            (source, score, grade, freshness_score, volume_score, error_score, schema_score, computed_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            s["source"],
                            s["score"],
                            s["grade"],
                            s["components"]["freshness"],
                            s["components"]["volume"],
                            s["components"]["errors"],
                            s["components"]["schema"],
                            now,
                        ),
                    )

        logger.info("Computed quality scores for %d sources", len(scores))
        return scores

    freshness = check_freshness()
    breakers = check_circuit_breakers()
    compute_quality_scores(freshness, breakers)


source_health_dag = source_health_monitoring()
