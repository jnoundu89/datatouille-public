"""
Vigilance Meteo France Ingestion DAG (Modularized Version)

Extracts weather alerts from Meteo France API and maps them to GIE locations.
Pipeline: create_bucket -> extract_vigilance -> transform_alerts -> transform_gie -> load_to_postgres

Authentication: JWT token extracted automatically via Scrapling from vigilance.meteofrance.fr
"""

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from botocore.exceptions import ClientError, EndpointConnectionError

from dag_config import get_meteo_config
from utils.db import pg_connection
from utils.fs import load_json_file
from utils.s3 import download_json, ensure_bucket, get_partitioned_key, get_s3_client, upload_json, upload_parquet
from utils.validation import GieAlertRecord, VigilanceAlertRecord, validate_records
from utils.vigilance import (
    extract_jwt_token_scrapling,
    fetch_vigilance_alerts,
    insert_gie_alerts,
    insert_vigilance_alerts,
    is_token_valid,
    map_alerts_to_stores,
    populate_vigilance_metadata,
)

logger = logging.getLogger(__name__)

INPUTS_PATH = "/opt/airflow/dags/inputs/vigilance_meteo"


@dag(
    dag_id="vigilance_meteo_ingestion",
    default_args={
        "owner": "data-engineer-airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    description="Vigilance Meteo France warnings mapped to GIE stores",
    schedule="*/15 * * * *",  # Every 15 minutes (Real-time monitoring)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["meteo", "vigilance", "ingestion"],
)
def vigilance_meteo_ingestion():
    @task
    def create_bucket() -> str:
        """Ensure MinIO landing bucket exists."""
        cfg = get_meteo_config()
        bucket_name = cfg["vigilance_bucket_name"]
        ensure_bucket(bucket_name)
        return bucket_name

    @task
    def extract_vigilance() -> dict[str, Any]:
        """Extract warnings from Météo France API using Playwright/Scrapling auth."""
        cfg = get_meteo_config()
        bucket_name = cfg["vigilance_bucket_name"]
        website_url = cfg["vigilance_website_url"]
        api_url = cfg["meteo_api_url"]
        api_params = cfg["api_params"]

        date_str = datetime.now(UTC).strftime("%Y-%m-%d")
        token_s3_key = "config/jwt_token.txt"

        # Try to retrieve existing token from S3
        token = None
        s3 = get_s3_client()
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=token_s3_key)
            token = obj["Body"].read().decode("utf-8").strip()
            logger.info("Found cached JWT token in S3")
        except ClientError:
            logger.info("No cached JWT token found in S3")

        # Validate token freshness
        if not token or not is_token_valid(token):
            logger.info("Token expired or missing. Fetching fresh token via Scrapling...")
            token = extract_jwt_token_scrapling(website_url)
            if not token:
                raise ValueError("Authentication failed: Unable to extract fresh JWT token")

            # Cache the new token in S3
            s3.put_object(Bucket=bucket_name, Key=token_s3_key, Body=token.encode("utf-8"))
            logger.info("Cached fresh JWT token to s3://%s/%s", bucket_name, token_s3_key)

        # Call API
        data = fetch_vigilance_alerts(api_url, token, api_params)
        if not data:
            raise ConnectionError("Meteo France API request failed")

        # Save raw extraction batch to MinIO
        landing_key = get_partitioned_key(
            domain="meteo", source="vigilance", filename="vigilance_raw.json.gz", date_str=date_str
        )
        upload_json(bucket_name, landing_key, data)

        return {
            "landing_key": landing_key,
            "date": date_str,
        }

    @task
    def transform_alerts(extract_result: dict[str, Any]) -> dict[str, Any]:
        """Clean and normalize raw alerts JSON."""
        cfg = get_meteo_config()
        bucket_name = cfg["vigilance_bucket_name"]
        date_str = extract_result["date"]
        extracted_at = datetime.now(UTC).isoformat()

        # Load raw data from MinIO
        try:
            raw_data_raw = download_json(bucket_name, extract_result["landing_key"])
            raw_data: dict[str, Any] = raw_data_raw if isinstance(raw_data_raw, dict) else {}
        except (ClientError, json.JSONDecodeError) as e:
            logger.error("Failed to load raw alerts: %s", e)
            raise

        # Load dictionary mapping
        dictionary = load_json_file(f"{INPUTS_PATH}/dictionary.json") or {}
        phenoms = {str(p["id"]): p["name"] for p in dictionary.get("phenomenons", [])}
        colors = {str(c["id"]): c for c in dictionary.get("colors", [])}

        # Load department helper
        departments = load_json_file(f"{INPUTS_PATH}/departments.json") or {}

        # Parse alert records
        alerts_list = []
        timelaps = raw_data.get("timelaps", [])
        for entry in timelaps:
            dept_code = entry.get("domain_id", "")
            dept_info = departments.get(dept_code) or {}
            dept_name = dept_info.get("name", "inconnu")

            for item in entry.get("timelaps_items", []):
                phenom_id = str(item.get("phenomenon_id", ""))
                color_id = str(item.get("color_id", ""))

                color_info = colors.get(color_id) or {}
                begin_time = item.get("begin_time", "")
                end_time = item.get("end_time", "")

                # Convert timestamps
                try:
                    begin_dt = datetime.fromtimestamp(int(begin_time) / 1000, UTC).isoformat()
                    end_dt = datetime.fromtimestamp(int(end_time) / 1000, UTC).isoformat()
                except (ValueError, TypeError):
                    begin_dt, end_dt = "", ""

                alerts_list.append(
                    {
                        "department_code": dept_code,
                        "department_name": dept_name,
                        "phenomenon_id": int(phenom_id) if phenom_id.isdigit() else 0,
                        "phenomenon_name": phenoms.get(phenom_id, "autre"),
                        "begin_time": begin_dt,
                        "end_time": end_dt,
                        "color_id": int(color_id) if color_id.isdigit() else 0,
                        "color_name": color_info.get("name", "vert"),
                        "color_level": color_info.get("level", 1),
                        "color_hex": color_info.get("hexaCode", "#31a354"),
                        "extracted_at": extracted_at,
                        "extraction_date": date_str,
                    }
                )

        # Save structured alerts table to MinIO
        landing_key = get_partitioned_key(
            domain="meteo", source="vigilance", filename="vigilance_alerts.json.gz", date_str=date_str
        )
        upload_json(bucket_name, landing_key, alerts_list)

        # Medallion Silver: Validate and upload as staging Parquet
        if alerts_list:
            valid_alerts = validate_records(alerts_list, VigilanceAlertRecord, pipeline_name="vigilance_alerts")
            upload_parquet("datatouille", "landing/stg_vigilance_alerts.parquet", valid_alerts)

        logger.info("Transformed %d structured alerts", len(alerts_list))

        return {
            "landing_key": landing_key,
            "alert_count": len(alerts_list),
            "date": date_str,
        }

    @task
    def transform_gie(alerts_result: dict[str, Any], **context) -> dict[str, Any]:
        """Map warnings to GIE / Store locations."""
        cfg = get_meteo_config()
        bucket_name = cfg["vigilance_bucket_name"]
        date_str = alerts_result["date"]
        extracted_at = datetime.now(UTC).isoformat()

        # Load alerts
        try:
            alerts_raw = download_json(bucket_name, alerts_result["landing_key"])
            alerts: list[dict[str, Any]] = alerts_raw if isinstance(alerts_raw, list) else []
        except (ClientError, json.JSONDecodeError) as e:
            logger.error("Failed to load transformed alerts: %s", e)
            raise

        # Load GIE mapping
        gie_mapping_raw = load_json_file(f"{INPUTS_PATH}/gie_mapping.json")
        gie_mapping: dict[str, Any] = gie_mapping_raw if isinstance(gie_mapping_raw, dict) else {}

        # Map alerts
        gie_records = map_alerts_to_stores(alerts, gie_mapping, date_str, extracted_at)

        # Store GIE alerts
        gie_landing_key = get_partitioned_key(
            domain="meteo", source="vigilance", filename="gie_alerts.json.gz", date_str=date_str
        )
        try:
            upload_json(bucket_name, gie_landing_key, gie_records)
        except (ClientError, EndpointConnectionError) as e:
            logger.error("Failed to upload GIE alerts to S3 (key=%s): %s", gie_landing_key, e)
            raise

        # Medallion Silver: Validate and upload as staging Parquet
        if gie_records:
            valid_gie = validate_records(gie_records, GieAlertRecord, pipeline_name="gie_alerts")
            upload_parquet("datatouille", "landing/stg_gie_alerts.parquet", valid_gie)

        logger.info("Mapped to %d GIE alerts to s3://%s/%s", len(gie_records), bucket_name, gie_landing_key)
        return {
            "bucket": bucket_name,
            "gie_landing_key": gie_landing_key,
            "gie_alert_count": len(gie_records),
            "date": date_str,
            "alerts_landing_key": alerts_result["landing_key"],
        }

    @task
    def load_to_postgres(gie_result: dict[str, Any]) -> dict[str, Any]:
        """Load alerts and GIE alerts to PostgreSQL tables."""
        cfg = get_meteo_config()
        bucket_name = cfg["vigilance_bucket_name"]

        # Load alerts data
        try:
            alerts_raw = download_json(bucket_name, gie_result["alerts_landing_key"])
            alerts: list[dict[str, Any]] = alerts_raw if isinstance(alerts_raw, list) else []
        except (ClientError, json.JSONDecodeError) as e:
            logger.error("Failed to load alerts from S3: %s", e)
            raise

        # Load GIE alerts data
        try:
            gie_alerts_raw = download_json(bucket_name, gie_result["gie_landing_key"])
            gie_alerts: list[dict[str, Any]] = gie_alerts_raw if isinstance(gie_alerts_raw, list) else []
        except (ClientError, json.JSONDecodeError) as e:
            logger.error("Failed to load GIE alerts from S3: %s", e)
            raise

        with pg_connection() as conn:
            with conn.cursor() as cur:
                insert_vigilance_alerts(cur, alerts)
                insert_gie_alerts(cur, gie_alerts)
                populate_vigilance_metadata(cur, f"{INPUTS_PATH}/dictionary.json")

        logger.info("Loaded %d alerts and %d GIE alerts to PostgreSQL", len(alerts), len(gie_alerts))
        return {
            "alerts_loaded": len(alerts),
            "gie_alerts_loaded": len(gie_alerts),
            "date": gie_result["date"],
        }

    # DAG flow
    bucket = create_bucket()
    raw = extract_vigilance()
    alerts = transform_alerts(raw)
    gie = transform_gie(alerts)
    postgres = load_to_postgres(gie)

    bucket >> raw >> alerts >> gie >> postgres


vigilance_meteo_dag = vigilance_meteo_ingestion()
