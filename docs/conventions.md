# Engineering Standards & Conventions

This document outlines the architectural standards, code design principles, and engineering practices applied across the **datatouille** platform.

---

## 🏗️ Medallion Pipeline Pattern

Every ingestion DAG strictly implements the following task flow to ensure modularity and ease of troubleshooting:

```
create_bucket ──► extract_raw ──► transform_landing ──► load_to_postgres
```

### 1. Storage Layers
* **Raw Layer**: Unmodified, original source data in its raw format (typically JSON or CSV). It is stored on MinIO at the path: `{bucket}/raw/{date}/data.json`.
* **Landing Layer**: Cleansed, normalized, typed, and structured data, stored as Parquet/JSON on MinIO at: `{bucket}/landing/{date}/data.json`.
* **Gold Layer**: High-value business analytical tables loaded into PostgreSQL and modeled using dbt-core and DuckDB for presentation and reporting.

### 2. Design Principles
* **Idempotence**: Re-running a pipeline for a specific logical date must produce the exact same target state without duplicating rows or creating side effects.
* **Storage Overwrite**: Always use deterministic keys and overwrite target files in the raw/landing buckets on subsequent runs (avoid appending).
* **Upserts on Load**: Database loads (`load_to_postgres`) must use `INSERT ... ON CONFLICT DO UPDATE` (upserts) or truncate-load schemes to preserve idempotency.

---

## 🐍 Code & Quality Standards

### Python Development
* **Type Hints**: Mandatory type annotations on all public functions, classes, and DAG definition signatures.
* **Linting & Formatting**: Enforced via `ruff` with specific formatting and import sorting rules.
* **Testing**: Python modules must have associated unit tests under `tests/` leveraging Pytest.

### Airflow 3.x Guidelines
* **SDK Decorators**: Use the modern Airflow 3.x SDK style decorators:
  ```python
  from airflow.sdk import dag, task
  ```
  Avoid the legacy `airflow.decorators` package.
* **Date Variables**: Reference pipeline dates using `logical_date` instead of the deprecated `execution_date`.
* **Scheduling**: Define DAG execution schedules using `schedule=` instead of the deprecated `schedule_interval=`.
* **Standard Logging**: Instantiate standard logging structures for observability:
  ```python
  import logging
  logger = logging.getLogger(__name__)
  ```

---

## 🧪 Data Quality Gates

To prevent corrupted or incomplete source data from polluting downstream dashboards:
* **Soda Core Integration**: Quality scans evaluate datasets against predefined YAML rules (null checks, range limits, type verification) during pipeline execution.
* **Blocker Tasks**: Quality validation tasks act as pipeline gates. If a check fails, the pipeline halts immediately before transforming or writing back to the analytical database.
