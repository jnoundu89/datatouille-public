"""PostgreSQL connection and bulk upsert utilities.

Replaces the duplicated psycopg2.connect(...) + execute_values pattern
found in 14+ DAGs.
"""

import logging
import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


@contextmanager
def pg_connection(autocommit: bool = False):
    """Context manager for PostgreSQL connections.

    Commits on success, rolls back on error, always closes.

    Args:
        autocommit: If True, set connection to autocommit mode.

    Yields:
        psycopg2 connection object.
    """
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )
    if autocommit:
        conn.autocommit = True
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def bulk_upsert(
    table: str,
    columns: list[str],
    values: list[tuple],
    conflict_columns: list[str],
    update_columns: list[str] | None = None,
) -> int:
    """Generic bulk upsert using execute_values with ON CONFLICT.

    Args:
        table: Target table name.
        columns: List of column names.
        values: List of tuples with row data.
        conflict_columns: Columns for ON CONFLICT clause.
        update_columns: Columns to update on conflict. If None, updates
            all non-conflict columns.

    Returns:
        Number of rows processed.
    """
    if not values:
        return 0

    if update_columns is None:
        update_columns = [c for c in columns if c not in conflict_columns]

    cols_str = ", ".join(columns)
    conflict_str = ", ".join(conflict_columns)
    update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)

    sql = f"INSERT INTO {table} ({cols_str}) VALUES %s"
    if update_columns:
        sql += f" ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
    else:
        sql += f" ON CONFLICT ({conflict_str}) DO NOTHING"

    with pg_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)

    logger.info("Upserted %d rows into %s", len(values), table)
    return len(values)
