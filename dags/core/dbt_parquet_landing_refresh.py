"""dbt Parquet Landing Refresh (Public Test Version)

Snapshots the dual_ref-enabled staging views (toscrape, meteo) to Parquet on MinIO,
so DuckDB marts can read directly from columnar storage via `read_parquet()`
instead of hitting Postgres through postgres_scanner.

Runs hourly; each run OVERWRITES the snapshot at
  s3://datatouille/landing/{model_name}.parquet
"""

import logging
import os
from datetime import datetime

import boto3
import psycopg2
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

DBT_DIR = "/opt/airflow/dbt"
DUCKDB_STATE_DIR = "/tmp/dbt_duckdb"
LANDING_BUCKET = "datatouille"
DOCKER_STAGING_SCHEMA = "dbt_docker_staging"
DOCKER_MARTS_SCHEMA = "dbt_docker_marts"

# Canonical rpt tables written back to Postgres by the duckdb target's
# postgres_writeback materialization.
CANONICAL_MARTS: list[tuple[str, str]] = [
    ("rpt_store_risk_summary", "generated_at"),
    ("rpt_toscrape_book_stats", "generated_at"),
    ("rpt_toscrape_quote_stats", "generated_at"),
]
FRESHNESS_MAX_DAYS = 2

STAGING_MODELS = [
    "stg_toscrape_books",
    "stg_toscrape_quotes",
    "stg_vigilance_alerts",
    "stg_georisques_commune_profiles",
    "stg_georisques_risk_details",
]

# Models exported as hive-partitioned Parquet by `extraction_date`.
PARTITIONED_STAGING_MODELS = [
    "stg_vigilance_alerts",
    "stg_georisques_commune_profiles",
]

# Marts built on the duckdb target.
DUCKDB_MARTS = [
    "dim_communes",
    "fct_vigilance_alerts",
    "rpt_store_risk_summary",
    "dim_toscrape_books",
    "dim_toscrape_authors",
    "fct_toscrape_quotes",
    "rpt_toscrape_book_stats",
    "rpt_toscrape_quote_stats",
]

DBT_ENV: dict[str, str] = {
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "airflow",
    "POSTGRES_USER": "airflow",
    "POSTGRES_PASSWORD": "airflow",
    "MINIO_ENDPOINT": "minio:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "DUCKDB_PATH": f"{DUCKDB_STATE_DIR}/analytics.duckdb",
}


@dag(
    dag_id="dbt_parquet_landing_refresh",
    start_date=datetime(2026, 4, 17),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "duckdb", "parquet", "gold"],
    description="Snapshot dual_ref staging views to Parquet on MinIO (hourly).",
    doc_md=__doc__,
    default_args={
        "owner": "data-platform",
        "retries": 2,
    },
)
def dbt_parquet_landing_refresh() -> None:
    """Refresh Parquet snapshots used by the DuckDB analytical path."""

    @task
    def ensure_prereqs() -> dict[str, str]:
        """Create the DuckDB tmp dir, MinIO bucket, and Postgres schemas."""
        os.makedirs(DUCKDB_STATE_DIR, exist_ok=True)

        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        )
        try:
            s3.head_bucket(Bucket=LANDING_BUCKET)
            logger.info("Bucket %s already exists", LANDING_BUCKET)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
                s3.create_bucket(Bucket=LANDING_BUCKET)
                logger.info("Created bucket %s", LANDING_BUCKET)
            else:
                raise

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "airflow"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        )
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                for schema in (DOCKER_STAGING_SCHEMA, DOCKER_MARTS_SCHEMA):
                    cur.execute(f'create schema if not exists "{schema}"')
                    logger.info("Ensured Postgres schema %s", schema)
        finally:
            conn.close()

        return {"duckdb_dir": DUCKDB_STATE_DIR, "bucket": LANDING_BUCKET}

    build_staging = BashOperator(
        task_id="build_staging",
        bash_command=(
            f"dbt run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} --target docker -s {' '.join(STAGING_MODELS)}"
        ),
        env=DBT_ENV,
        append_env=True,
    )

    models_json = "[" + ", ".join(f'"{m}"' for m in STAGING_MODELS) + "]"
    partitioned_json = "[" + ", ".join(f'"{m}"' for m in PARTITIONED_STAGING_MODELS) + "]"
    args_json = (
        '{"models": '
        + models_json
        + ', "partitioned_models": '
        + partitioned_json
        + f', "src_schema": "{DOCKER_STAGING_SCHEMA}"'
        + "}"
    )

    export_parquet_landing = BashOperator(
        task_id="export_parquet_landing",
        bash_command=(
            "dbt run-operation export_parquet_landing "
            f"--profiles-dir {DBT_DIR} --project-dir {DBT_DIR} "
            "--target duckdb "
            f"--args '{args_json}'"
        ),
        env=DBT_ENV,
        append_env=True,
    )

    mart_vars = (
        '{"duckdb_source_parquet": true, '
        '"duckdb_parquet_base": "s3://datatouille/landing", '
        '"duckdb_partitioned_models": ' + partitioned_json + ", "
        f'"postgres_writeback_schema": "{DOCKER_MARTS_SCHEMA}"'
        "}"
    )
    build_marts = BashOperator(
        task_id="build_marts",
        bash_command=(
            "dbt run "
            f"--profiles-dir {DBT_DIR} --project-dir {DBT_DIR} "
            "--target duckdb "
            f"-s {' '.join(DUCKDB_MARTS)} "
            f"--vars '{mart_vars}'"
        ),
        env=DBT_ENV,
        append_env=True,
    )

    @task
    def check_marts_quality() -> dict[str, int]:
        """In-band quality gate on the canonical rpt tables."""
        from datetime import date, datetime

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "airflow"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        )
        today = date.today()
        errors: list[str] = []
        row_counts: dict[str, int] = {}
        try:
            with conn.cursor() as cur:
                for table, date_col in CANONICAL_MARTS:
                    cur.execute(f'SELECT COUNT(*), MAX("{date_col}") FROM "{DOCKER_MARTS_SCHEMA}"."{table}"')
                    n, last = cur.fetchone()
                    row_counts[table] = n or 0
                    if not n:
                        errors.append(f"{table}: empty")
                        continue
                    if last is None:
                        errors.append(f"{table}: no max({date_col})")
                        continue
                    if isinstance(last, datetime):
                        last = last.date()
                    age = (today - last).days
                    if age > FRESHNESS_MAX_DAYS:
                        errors.append(f"{table}: stale — last {date_col}={last} ({age}d old)")
                    logger.info("quality: %s rows=%d last_%s=%s", table, n, date_col, last)
        finally:
            conn.close()

        if errors:
            raise RuntimeError("Mart quality gate failed:\n  - " + "\n  - ".join(errors))
        return row_counts

    (ensure_prereqs() >> build_staging >> export_parquet_landing >> build_marts >> check_marts_quality())


dbt_parquet_landing_refresh()
