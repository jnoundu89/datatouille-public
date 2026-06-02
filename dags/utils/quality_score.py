"""Composite quality score per source.

Adapted from worldmonitor country-instability.ts weighted scoring.
Combines freshness, volume, error rate, and schema compliance into
a single 0-100 score with A-F grading.
"""

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Weights must sum to 1.0
WEIGHTS = {
    "freshness": 0.30,
    "volume": 0.25,
    "errors": 0.25,
    "schema": 0.20,
}

GRADE_THRESHOLDS = [
    (90, "A"),
    (80, "B"),
    (70, "C"),
    (50, "D"),
    (0, "F"),
]


@dataclass
class QualityScore:
    """Composite quality score for a data source."""

    source: str
    score: float
    grade: str
    components: dict[str, float] = field(default_factory=dict)


def _score_to_grade(score: float) -> str:
    for threshold, grade in GRADE_THRESHOLDS:
        if score >= threshold:
            return grade
    return "F"


def compute_quality_score(
    source: str,
    freshness_ratio: float = 1.0,
    volume_ratio: float = 1.0,
    error_rate: float = 0.0,
    schema_compliance: float = 1.0,
) -> QualityScore:
    """Compute weighted quality score for a source.

    All inputs should be 0.0-1.0 ratios:
    - freshness_ratio: 1.0 = within expected interval, 0.0 = critically stale
    - volume_ratio: 1.0 = expected volume, lower = fewer items than expected
    - error_rate: 0.0 = no errors, 1.0 = all errors
    - schema_compliance: 1.0 = all fields valid, 0.0 = all invalid

    Returns:
        QualityScore with 0-100 score and A-F grade.
    """
    freshness_score = max(0.0, min(1.0, freshness_ratio)) * 100
    volume_score = max(0.0, min(1.0, volume_ratio)) * 100
    error_score = max(0.0, min(1.0, 1.0 - error_rate)) * 100
    schema_score = max(0.0, min(1.0, schema_compliance)) * 100

    weighted = (
        freshness_score * WEIGHTS["freshness"]
        + volume_score * WEIGHTS["volume"]
        + error_score * WEIGHTS["errors"]
        + schema_score * WEIGHTS["schema"]
    )

    score = round(weighted, 1)
    grade = _score_to_grade(score)

    if score < 40:
        logger.warning("Quality score for %s: %.1f (%s) - CRITICAL", source, score, grade)

    return QualityScore(
        source=source,
        score=score,
        grade=grade,
        components={
            "freshness": round(freshness_score, 1),
            "volume": round(volume_score, 1),
            "errors": round(error_score, 1),
            "schema": round(schema_score, 1),
        },
    )
