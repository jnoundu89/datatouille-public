"""Data validation helpers and Pydantic schemas for the Medallion Architecture."""

import json
import logging
from typing import Any

from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)


def log_schema_drift(pipeline_name: str, expected_fields: set[str], actual_fields: set[str]) -> None:
    """Detect and log schema changes or drifts into S3/MinIO metadata."""
    import datetime

    from utils.s3 import download_json, ensure_bucket, upload_json

    extra = actual_fields - expected_fields
    missing = expected_fields - actual_fields
    if not extra and not missing:
        return

    try:
        ensure_bucket("datatouille")
        key = "metadata/schema_evolution_log.json"

        # Download existing log
        try:
            log_data = download_json("datatouille", key)
        except Exception:
            log_data = []

        entry = {
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "pipeline": pipeline_name,
            "extra_fields": list(extra),
            "missing_fields": list(missing),
        }
        log_data.append(entry)
        upload_json("datatouille", key, log_data)
        logger.info("Recorded schema evolution/drift entry for %s: %s", pipeline_name, entry)

        # Trigger Slack warning alert for drift
        from utils.notifications import send_slack_alert

        msg = f"Schema drift/evolution detected in pipeline '{pipeline_name}'. Extra fields: {list(extra)}. Missing fields: {list(missing)}."
        send_slack_alert(pipeline_name, msg, severity="warning")
    except Exception as e:
        logger.error("Failed to log schema drift for %s: %s", pipeline_name, e)


def log_validation_metrics(pipeline_name: str, total: int, valid: int, errors: int, pass_rate: float) -> None:
    """Log validation run statistics to a structured JSON file on S3 for quality dashboarding."""
    import datetime

    from utils.s3 import download_json, ensure_bucket, upload_json

    try:
        ensure_bucket("datatouille")
        key = "metadata/quality_runs.json"
        try:
            log_data = download_json("datatouille", key)
        except Exception:
            log_data = []

        entry = {
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "pipeline": pipeline_name,
            "total_records": total,
            "valid_records": valid,
            "error_records": errors,
            "pass_rate": pass_rate,
        }
        log_data.append(entry)
        # Keep only the last 1000 runs to avoid file size bloat
        upload_json("datatouille", key, log_data[-1000:])
    except Exception as e:
        logger.error("Failed to log validation metrics for %s: %s", pipeline_name, e)


def validate_records(
    records: list[dict[str, Any]],
    model: type[BaseModel],
    min_pass_rate: float = 0.90,
    pipeline_name: str = "unknown_pipeline",
) -> list[dict[str, Any]]:
    """Validate a list of records against a Pydantic model.

    If the proportion of valid records is below min_pass_rate, raises ValueError to block pipeline progression.
    Also handles dynamic extra key metadata capturing and drift detection logging.
    """
    valid_records = []
    errors = 0

    expected_fields = set(model.model_fields.keys())
    has_metadata_json = "metadata_json" in expected_fields
    fields_to_check = expected_fields - {"metadata_json"}

    for r in records:
        try:
            actual_keys = set(r.keys())

            # Detect extra fields and map them to metadata_json
            extra_keys = actual_keys - fields_to_check
            if extra_keys:
                log_schema_drift(pipeline_name, fields_to_check, actual_keys)
                if has_metadata_json:
                    r["metadata_json"] = json.dumps({k: r[k] for k in extra_keys}, ensure_ascii=False)
            elif has_metadata_json and "metadata_json" not in r:
                r["metadata_json"] = None

            # Instantiate model to perform schema/type checks
            validated = model(**r)
            valid_records.append(validated.model_dump())
        except ValidationError as e:
            errors += 1
            if errors <= 5:
                logger.warning("Validation error on record: %s\nError details: %s", r, e)

    total = len(records)
    if total == 0:
        raise ValueError("Cannot validate empty records list")

    pass_rate = len(valid_records) / total
    logger.info(
        "Validation completed: %d valid, %d errors out of %d total (Pass rate: %.2f%%)",
        len(valid_records),
        errors,
        total,
        pass_rate * 100,
    )

    # Log metrics to S3 for dashboarding
    log_validation_metrics(pipeline_name, total, len(valid_records), errors, pass_rate)

    if pass_rate < min_pass_rate:
        from utils.notifications import send_slack_alert

        msg = f"Schema quality gate failed: pass rate {pass_rate * 100:.2f}% is below threshold {min_pass_rate * 100:.2f}%"
        send_slack_alert(pipeline_name, msg, severity="error")
        raise ValueError(
            f"Schema quality gate failed: pass rate {pass_rate:.2f} is below threshold {min_pass_rate:.2f}"
        )

    return valid_records


class BookRecord(BaseModel):
    upc: str = Field(..., min_length=2)
    title: str
    category: str
    star_rating: int = Field(..., ge=1, le=5)
    price: float = Field(..., ge=0.0)
    price_incl_tax: float = Field(..., ge=0.0)
    tax: float = Field(..., ge=0.0)
    availability: str
    stock_count: int = Field(..., ge=0)
    description: str | None = None
    image_url: str
    product_url: str
    extraction_date: str
    extracted_at: str
    metadata_json: str | None = None


class QuoteRecord(BaseModel):
    quote_hash: str
    text: str
    author_name: str
    author_slug: str | None = None
    tags: str
    extraction_date: str
    extracted_at: str
    metadata_json: str | None = None


class AuthorRecord(BaseModel):
    slug: str
    name: str
    born_date: str | None = None
    born_location: str | None = None
    description: str | None = None
    extraction_date: str
    extracted_at: str
    metadata_json: str | None = None


class VigilanceAlertRecord(BaseModel):
    department_code: str
    department_name: str
    phenomenon_id: int
    phenomenon_name: str
    begin_time: str | None = None
    end_time: str | None = None
    color_id: int
    color_name: str
    color_level: int
    color_hex: str
    extraction_date: str
    extracted_at: str
    metadata_json: str | None = None


class GieAlertRecord(BaseModel):
    gie_name: str
    department_code: str
    department_name: str
    phenomenon_id: int
    phenomenon_name: str
    begin_time: str | None = None
    end_time: str | None = None
    color_id: int
    color_name: str
    color_level: int
    color_hex: str
    extraction_date: str
    extracted_at: str
    metadata_json: str | None = None


class CommuneProfileRecord(BaseModel):
    insee_code: str
    commune: str
    code_postal: str
    total_risks: int
    seisme_present: bool = False
    seisme_severity: int = 0
    rga_present: bool = False
    rga_severity: int = 0
    rga_statut: str | None = ""
    radon_present: bool = False
    radon_severity: int = 0
    inondation_present: bool = False
    inondation_severity: int = 0
    icpe_present: bool = False
    nucleaire_present: bool = False
    department_code: str
    department_name: str
    region: str
    extraction_date: str
    extracted_at: str
    metadata_json: str | None = None


class GieRiskRecord(BaseModel):
    gie_name: str
    insee_code: str
    commune: str
    postal_code: str
    department_code: str
    department_name: str
    region: str
    naturels_count: int
    technologiques_count: int
    total_risks: int
    max_severity_naturels: int
    max_severity_technologiques: int
    seisme_present: bool = False
    seisme_severity: int = 0
    rga_present: bool = False
    rga_severity: int = 0
    rga_statut: str | None = ""
    radon_present: bool = False
    radon_severity: int = 0
    inondation_present: bool = False
    inondation_severity: int = 0
    icpe_present: bool = False
    nucleaire_present: bool = False
    extraction_date: str
    extracted_at: str
    metadata_json: str | None = None


class RiskDetailRecord(BaseModel):
    insee_code: str
    key: str
    libelle: str
    risk_libelle: str | None = ""
    present: bool
    severity_score: int
    statut_commune: str | None = ""
    statut_adresse: str | None = ""
    extraction_date: str
    extracted_at: str
    metadata_json: str | None = None


class MarmitonRecipeRecord(BaseModel):
    recipe_id: str
    title: str
    recipe_url: str
    image_url: str | None = None
    prep_time_minutes: int | None = None
    cook_time_minutes: int | None = None
    total_time_minutes: int | None = None
    recipe_yield: str | None = None
    difficulty: str | None = None
    budget: str | None = None
    rating: float | None = None
    review_count: int | None = None
    author: str | None = None
    ingredients: str | None = None
    extraction_date: str
    extracted_at: str
    metadata_json: str | None = None


class GogDealRecord(BaseModel):
    gog_id: str
    title: str
    slug: str | None = None
    price: float | None = None
    original_price: float | None = None
    discount_pct: float | None = None
    developer: str | None = None
    publisher: str | None = None
    release_date: str | None = None
    rating: float | None = None
    genres: str | None = None
    features: str | None = None
    cover_url: str | None = None
    is_in_development: bool | None = None
    extraction_date: str
    extracted_at: str
    metadata_json: str | None = None


class DealabsDealRecord(BaseModel):
    deal_id: str
    title: str
    description: str | None = None
    price: float | None = None
    original_price: float | None = None
    discount_pct: float | None = None
    merchant: str | None = None
    category: str | None = None
    deal_url: str
    image_url: str | None = None
    temperature: int | None = None
    comment_count: int | None = None
    expires_at: str | None = None
    extraction_date: str
    extracted_at: str
    metadata_json: str | None = None


def detect_anomalies(
    df: Any,
    numeric_columns: list[str],
    z_threshold: float = 3.0,
) -> Any:
    """Detect anomalies in numeric columns of a pandas DataFrame using Z-score.

    Adds 'is_anomaly' (boolean) and 'anomaly_reasons' (list of strings) to the DataFrame.
    """
    import numpy as np
    import pandas as pd

    df = df.copy()
    df["is_anomaly"] = False
    # Use object type for list column to avoid pandas setting errors
    df["anomaly_reasons"] = pd.Series([[] for _ in range(len(df))], dtype=object)

    for col in numeric_columns:
        if col not in df.columns:
            continue

        # Coerce to float
        series = pd.to_numeric(df[col], errors="coerce")
        mean = series.mean()
        std = series.std()

        if pd.isna(mean) or pd.isna(std) or std == 0:
            continue

        z_scores = (series - mean) / std
        outliers = np.abs(z_scores) > z_threshold

        for idx in df[outliers].index:
            reason = f"{col} has outlier Z-score ({z_scores[idx]:.2f} > {z_threshold})"
            df.at[idx, "is_anomaly"] = True
            current_reasons = df.at[idx, "anomaly_reasons"]
            if isinstance(current_reasons, list):
                df.at[idx, "anomaly_reasons"] = current_reasons + [reason]
            else:
                df.at[idx, "anomaly_reasons"] = [reason]

    return df
