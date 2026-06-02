"""Airflow metadata DB cleanup — weekly housekeeping.

Retires rows older than 90 days from the high-churn Airflow metadata tables
(dag_run, task_instance, job, log, etc.). Prevents unbounded growth on
long-lived hourly DAGs like `dbt_parquet_landing_refresh` whose history would
otherwise accumulate ~8,760 rows/year per table.

Runs weekly, single-instance (max_active_runs=1), late Sunday night so it
never races with the regular hourly work. The `airflow db clean` CLI drives
the deletion; no custom SQL, no risk of orphaning referential data.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

RETENTION_DAYS = 90


@dag(
    dag_id="metadata_cleanup",
    start_date=datetime(2026, 4, 20),
    schedule="0 3 * * 0",
    catchup=False,
    max_active_runs=1,
    tags=["infra", "maintenance"],
    description=f"Prune Airflow metadata older than {RETENTION_DAYS} days (weekly).",
    doc_md=__doc__,
    default_args={
        "owner": "data-platform",
        "retries": 1,
    },
)
def metadata_cleanup() -> None:
    """Single task wrapping `airflow db clean`."""
    BashOperator(
        task_id="airflow_db_clean",
        bash_command=(
            "airflow db clean --clean-before-timestamp "
            f"\"$(date -u -d '{RETENTION_DAYS} days ago' +'%Y-%m-%dT%H:%M:%S+00:00')\" "
            "--yes --verbose"
        ),
    )


metadata_cleanup()
