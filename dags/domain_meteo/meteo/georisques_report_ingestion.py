"""
GeoRisques Report Ingestion DAG (Modularized Version)

Extracts natural and technological risk data from GeoRisques API and maps them to Store locations.
Pipeline: create_bucket -> extract_risks -> transform_risks -> transform_store -> load_to_postgres

API Documentation: https://www.georisques.gouv.fr/doc-api
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from botocore.exceptions import ClientError

from dag_config import get_meteo_config
from utils.db import pg_connection
from utils.fs import load_json_file
from utils.georisques import (
    fetch_risk_report,
    insert_commune_profiles,
    insert_store_risks,
    insert_risk_details,
    normalize_risk_report,
)
from utils.s3 import download_json, ensure_bucket, get_partitioned_key, upload_json

logger = logging.getLogger(__name__)

INPUTS_PATH = "/opt/airflow/dags/inputs/georisques"


@dag(
    dag_id="georisques_report_ingestion",
    default_args={
        "owner": "data-engineer-airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    description="Risk data ingestion from GeoRisques API for Store locations",
    schedule="0 4 * * 1",  # Weekly (Mondays at 04:00)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["meteo", "georisques", "ingestion", "risks"],
)
def georisques_report_ingestion():
    @task
    def create_bucket() -> str:
        """Ensure MinIO landing bucket exists."""
        cfg = get_meteo_config()
        bucket_name = cfg["georisques_bucket_name"]
        ensure_bucket(bucket_name)
        return bucket_name

    @task
    def extract_risks() -> dict[str, Any]:
        """Extract risk data from GeoRisques API for all Store communes (parallel)."""
        cfg = get_meteo_config()
        bucket_name = cfg["georisques_bucket_name"]
        georisques_url = cfg["georisques_rapport_url"]
        ban_url = cfg["ban_geocode_url"]
        request_timeout = cfg["request_timeout_read"]
        rate_limit_delay = cfg["rate_limit_delay"]
        max_workers = cfg["max_workers"]

        date_str = datetime.now(UTC).strftime("%Y-%m-%d")

        # Load Store mapping to get INSEE codes with commune info for fallback
        store_mapping_raw = load_json_file(f"{INPUTS_PATH}/store_mapping.json")
        store_mapping: dict[str, Any] = store_mapping_raw if isinstance(store_mapping_raw, dict) else {}

        # Dedup INSEE codes to minimize API requests
        insee_to_commune = {}
        for gie_data in store_mapping.values():
            if isinstance(gie_data, dict) and gie_data.get("insee_code"):
                insee = gie_data["insee_code"]
                if insee not in insee_to_commune:
                    insee_to_commune[insee] = {
                        "commune": gie_data.get("commune", ""),
                        "postal_code": gie_data.get("postal_code", ""),
                    }

        logger.info("Starting risk report extraction for %d unique INSEE codes", len(insee_to_commune))
        raw_reports = {}

        # Fetch in parallel with rate limiting
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_insee = {
                executor.submit(
                    fetch_risk_report,
                    insee_code=insee,
                    georisques_url=georisques_url,
                    ban_url=ban_url,
                    request_timeout=request_timeout,
                    rate_limit_delay=rate_limit_delay,
                    commune_name=info["commune"],
                    postal_code=info["postal_code"],
                ): insee
                for insee, info in insee_to_commune.items()
            }

            for future in as_completed(future_to_insee):
                insee = future_to_insee[future]
                try:
                    report = future.result()
                    if report:
                        raw_reports[insee] = report
                except Exception as e:
                    logger.error("Thread execution failed for INSEE %s: %s", insee, e)

        # Upload raw extraction batch to MinIO
        landing_key = get_partitioned_key(
            domain="meteo", source="georisques", filename="georisques_raw.json.gz", date_str=date_str
        )
        upload_json(bucket_name, landing_key, raw_reports)

        logger.info("Extracted %d risk reports successfully out of %d", len(raw_reports), len(insee_to_commune))

        return {
            "landing_key": landing_key,
            "report_count": len(raw_reports),
            "date": date_str,
        }

    @task
    def transform_risks(extract_result: dict[str, Any]) -> dict[str, Any]:
        """Parse, validate and normalize raw GéoRisques reports."""
        cfg = get_meteo_config()
        bucket_name = cfg["georisques_bucket_name"]
        date_str = extract_result["date"]
        extracted_at = datetime.now(UTC).isoformat()

        # Load raw data from MinIO
        try:
            raw_data_raw = download_json(bucket_name, extract_result["landing_key"])
            raw_data: dict[str, Any] = raw_data_raw if isinstance(raw_data_raw, dict) else {}
        except (ClientError, json.JSONDecodeError) as e:
            logger.error("Failed to load raw reports: %s", e)
            raise

        commune_profiles = []
        risk_details = []

        # Load department configuration
        dept_mapping_raw = load_json_file(f"{INPUTS_PATH}/departments.json")
        dept_mapping: dict[str, Any] = dept_mapping_raw if isinstance(dept_mapping_raw, dict) else {}

        # Load risk types configuration
        risk_types_raw = load_json_file(f"{INPUTS_PATH}/risk_types.json")
        risk_types: dict[str, Any] = risk_types_raw if isinstance(risk_types_raw, dict) else {}

        for insee, report in raw_data.items():
            profile = normalize_risk_report(report, insee)
            profile["extracted_at"] = extracted_at
            profile["extraction_date"] = date_str

            # Attach department/region info
            dept_code = profile["code_postal"][:2] if profile["code_postal"] else ""
            dept_info = dept_mapping.get(dept_code) or {}
            profile["department_code"] = dept_code
            profile["department_name"] = dept_info.get("name", "inconnu")
            profile["region"] = dept_info.get("region", "inconnu")

            # Format seisme, rga, radon, inondation, icpe, nucleaire indicators for fast querying
            for r in profile["risks"]:
                risk_key = r["key"]
                severity = r["severity_score"]
                present = r["present"]

                if risk_key == "seisme":
                    profile["seisme_present"] = present
                    profile["seisme_severity"] = severity
                elif risk_key == "retraitGonflementArgile":
                    profile["rga_present"] = present
                    profile["rga_severity"] = severity
                    profile["rga_statut"] = r["statut_commune"] or r["statut_adresse"]
                elif risk_key == "radon":
                    profile["radon_present"] = present
                    profile["radon_severity"] = severity
                elif risk_key == "inondation":
                    profile["inondation_present"] = present
                    profile["inondation_severity"] = severity
                elif risk_key == "icpe":
                    profile["icpe_present"] = present
                elif risk_key == "nucleaire":
                    profile["nucleaire_present"] = present

                # Attach detail record
                details = {**r, "insee_code": insee, "extracted_at": extracted_at, "extraction_date": date_str}
                details["risk_libelle"] = risk_types.get(risk_key, r["libelle"])
                risk_details.append(details)

            profile["total_risks"] = profile["metrics"]["naturels_count"] + profile["metrics"]["technologiques_count"]
            # Exclude raw risks array from parent record before saving
            profile_record = {k: v for k, v in profile.items() if k != "risks"}
            commune_profiles.append(profile_record)

        # Upload structured tables to MinIO (landing parquet zone ready)
        landing_data = {
            "commune_profiles": commune_profiles,
            "risk_details": risk_details,
        }
        landing_key = get_partitioned_key(
            domain="meteo", source="georisques", filename="commune_profiles.json.gz", date_str=date_str
        )
        upload_json(bucket_name, landing_key, landing_data)

        logger.info(
            "Transformed %d commune profiles with %d nested risk details",
            len(commune_profiles),
            len(risk_details),
        )

        return {
            "landing_key": landing_key,
            "profile_count": len(commune_profiles),
            "detail_count": len(risk_details),
            "date": date_str,
        }

    @task
    def transform_gie(transform_result: dict[str, Any], **context) -> dict[str, Any]:
        """Map commune risks to Store locations."""
        cfg = get_meteo_config()
        bucket_name = cfg["georisques_bucket_name"]
        date_str = transform_result["date"]
        extracted_at = datetime.now(UTC).isoformat()

        # Load profiles
        try:
            landing_data_raw = download_json(bucket_name, transform_result["landing_key"])
            landing_data: dict[str, Any] = landing_data_raw if isinstance(landing_data_raw, dict) else {}
            profiles = landing_data.get("commune_profiles", [])
            insee_to_profile = {p["insee_code"]: p for p in profiles}
        except (ClientError, json.JSONDecodeError) as e:
            logger.error("Failed to load transformed profiles: %s", e)
            raise

        # Load Store mapping
        store_mapping_raw = load_json_file(f"{INPUTS_PATH}/store_mapping.json")
        store_mapping: dict[str, Any] = store_mapping_raw if isinstance(store_mapping_raw, dict) else {}

        # Map Stores
        gie_records = []
        for store_name, gie_data in store_mapping.items():
            if not isinstance(gie_data, dict):
                continue

            insee_code = gie_data.get("insee_code", "")
            profile = insee_to_profile.get(insee_code) or {}

            gie_records.append(
                {
                    "store_name": store_name,
                    "insee_code": insee_code,
                    "commune": gie_data.get("commune", profile.get("commune", "")),
                    "postal_code": gie_data.get("postal_code", profile.get("code_postal", "")),
                    "department_code": profile.get("department_code", gie_data.get("postal_code", "")[:2]),
                    "department_name": profile.get("department_name", "inconnu"),
                    "region": profile.get("region", "inconnu"),
                    "naturels_count": profile.get("metrics", {}).get("naturels_count", 0),
                    "technologiques_count": profile.get("metrics", {}).get("technologiques_count", 0),
                    "total_risks": profile.get("total_risks", 0),
                    "max_severity_naturels": profile.get("metrics", {}).get("max_severity_naturels", 0),
                    "max_severity_technologiques": profile.get("metrics", {}).get("max_severity_technologiques", 0),
                    "seisme_present": profile.get("seisme_present", False),
                    "seisme_severity": profile.get("seisme_severity", 0),
                    "rga_present": profile.get("rga_present", False),
                    "rga_severity": profile.get("rga_severity", 0),
                    "rga_statut": profile.get("rga_statut", ""),
                    "radon_present": profile.get("radon_present", False),
                    "radon_severity": profile.get("radon_severity", 0),
                    "inondation_present": profile.get("inondation_present", False),
                    "inondation_severity": profile.get("inondation_severity", 0),
                    "icpe_present": profile.get("icpe_present", False),
                    "nucleaire_present": profile.get("nucleaire_present", False),
                    "extracted_at": extracted_at,
                    "extraction_date": date_str,
                }
            )

        # Store Store data
        gie_key = get_partitioned_key(
            domain="meteo", source="georisques", filename="store_risks.json.gz", date_str=date_str
        )
        upload_json(bucket_name, gie_key, gie_records)
        logger.info("Mapped %d Store risk profiles", len(gie_records))

        return {
            "gie_key": gie_key,
            "gie_count": len(gie_records),
            "date": date_str,
            "commune_landing_key": transform_result["landing_key"],
        }

    @task
    def load_to_postgres(gie_result: dict[str, Any]) -> dict[str, Any]:
        """Load risk data to PostgreSQL tables."""
        cfg = get_meteo_config()
        bucket_name = cfg["georisques_bucket_name"]

        # Load data
        try:
            commune_data_raw = download_json(bucket_name, gie_result["commune_landing_key"])
            commune_data: dict[str, Any] = commune_data_raw if isinstance(commune_data_raw, dict) else {}
        except (ClientError, json.JSONDecodeError) as e:
            logger.error("Failed to load commune data: %s", e)
            raise

        try:
            gie_records_raw = download_json(bucket_name, gie_result["gie_key"])
            gie_records: list[dict[str, Any]] = gie_records_raw if isinstance(gie_records_raw, list) else []
        except (ClientError, json.JSONDecodeError) as e:
            logger.error("Failed to load Store data: %s", e)
            raise

        commune_profiles = commune_data.get("commune_profiles", [])
        risk_details = commune_data.get("risk_details", [])

        with pg_connection() as conn:
            with conn.cursor() as cur:
                insert_commune_profiles(cur, commune_profiles)
                insert_store_risks(cur, gie_records)
                insert_risk_details(cur, risk_details)

        logger.info(
            "Loaded %d commune profiles, %d Store risks, %d risk details to PostgreSQL",
            len(commune_profiles),
            len(gie_records),
            len(risk_details),
        )

        return {
            "commune_profiles_loaded": len(commune_profiles),
            "store_risks_loaded": len(gie_records),
            "risk_details_loaded": len(risk_details),
            "date": gie_result["date"],
        }

    # DAG flow
    bucket = create_bucket()
    raw = extract_risks()
    transformed = transform_risks(raw)
    store = transform_gie(transformed)
    postgres = load_to_postgres(store)

    bucket >> raw >> transformed >> store >> postgres


GEORISQUES_REPORT_DAG = georisques_report_ingestion()
