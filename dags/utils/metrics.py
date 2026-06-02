"""Extraction metrics tracking per source.

Records items extracted, loaded, errors, and duration for each DAG run.
Writes to source_extraction_metrics table for monitoring.
"""

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


@dataclass
class ExtractionMetrics:
    """Tracks extraction metrics for a single DAG run."""

    source: str
    dag_id: str
    run_date: str
    items_extracted: int = 0
    items_loaded: int = 0
    errors: int = 0
    duration_seconds: float = 0.0
    started_at: str = ""
    finished_at: str = ""
    _extra: dict = field(default_factory=dict, init=False)

    def record(self) -> None:
        """Write metrics to source_extraction_metrics table."""
        from utils.db import pg_connection

        with pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO source_extraction_metrics (
                        source, dag_id, run_date, items_extracted, items_loaded,
                        errors, duration_seconds, started_at, finished_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, dag_id, run_date) DO UPDATE SET
                        items_extracted = EXCLUDED.items_extracted,
                        items_loaded = EXCLUDED.items_loaded,
                        errors = EXCLUDED.errors,
                        duration_seconds = EXCLUDED.duration_seconds,
                        started_at = EXCLUDED.started_at,
                        finished_at = EXCLUDED.finished_at
                    """,
                    (
                        self.source,
                        self.dag_id,
                        self.run_date,
                        self.items_extracted,
                        self.items_loaded,
                        self.errors,
                        round(self.duration_seconds, 2),
                        self.started_at or None,
                        self.finished_at or None,
                    ),
                )

        logger.info(
            "Recorded metrics for %s/%s: %d extracted, %d loaded, %d errors, %.1fs",
            self.source,
            self.dag_id,
            self.items_extracted,
            self.items_loaded,
            self.errors,
            self.duration_seconds,
        )


@contextmanager
def track_extraction(source: str, dag_id: str, run_date: str, *, min_records: int = 0):
    """Context manager that auto-tracks extraction timing and records metrics.

    Usage:
        with track_extraction("allkeyshop", "pricing_ingestion", "2026-01-15", min_records=1) as metrics:
            metrics.items_extracted = 50
            metrics.items_loaded = 48
            metrics.errors = 2

    The duration and timestamps are set automatically.

    Args:
        min_records: Minimum items_loaded required. If the block completes
            normally but items_loaded < min_records, a RuntimeError is raised
            AFTER recording metrics (so the zero-records run is still tracked).
    """
    metrics = ExtractionMetrics(
        source=source,
        dag_id=dag_id,
        run_date=run_date,
        started_at=datetime.now(UTC).isoformat(),
    )
    start = time.monotonic()

    error_raised = False
    try:
        yield metrics
    except Exception:
        error_raised = True
        metrics.errors += 1
        raise
    finally:
        metrics.duration_seconds = time.monotonic() - start
        metrics.finished_at = datetime.now(UTC).isoformat()
        try:
            metrics.record()
        except Exception as e:
            logger.warning("Failed to record extraction metrics: %s", e)

        # Guard: fail loudly when extraction yields fewer records than expected.
        # Must be in finally — code after finally is unreachable when the caller
        # does `return` inside the `with` block (GeneratorExit closes the generator).
        if not error_raised and min_records > 0 and metrics.items_loaded < min_records:
            raise RuntimeError(
                f"[{source}/{dag_id}] Extraction returned {metrics.items_loaded} records "
                f"(minimum: {min_records}) for {run_date}"
            )
