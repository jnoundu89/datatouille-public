{#
    dual_ref: target-aware source resolver for the DuckDB pipeline.

    Three resolution modes, driven by target + vars:

    1. postgres target -> {{ ref(model_name) }} (unchanged).
    2. duckdb target, default -> attached Postgres via postgres_scanner
       (alias pg_src), reading the already-materialized staging view.
    3. duckdb target + var('duckdb_source_parquet', true) -> direct
       read_parquet() from MinIO. Prerequisite: run the
       export_parquet_landing operation first to snapshot staging to MinIO.

    In Parquet mode, two layouts are supported per-model:

    * Default: `{base}/{model}.parquet` (single file).
    * If `model_name in var('duckdb_partitioned_models', [])`:
      `{base}/{model}/**/*.parquet` with `hive_partitioning=true` so
      `extraction_date` is exposed as a virtual column and DuckDB can
      predicate-push-down on it.

    Required vars when using the Parquet mode:
        duckdb_source_parquet: true
        duckdb_parquet_base:   's3://datatouille/landing'     (default)
        duckdb_partitioned_models: ['stg_a', 'stg_b']         (optional)
#}
{% macro dual_ref(model_name) %}
  {%- if target.type == 'duckdb' -%}
    {%- if var('duckdb_source_parquet', false) -%}
      {%- set base = var('duckdb_parquet_base', 's3://datatouille/landing') -%}
      {%- set partitioned = var('duckdb_partitioned_models', []) -%}
      {%- if model_name in partitioned -%}
        read_parquet('{{ base }}/{{ model_name }}/**/*.parquet', hive_partitioning=true)
      {%- else -%}
        read_parquet('{{ base }}/{{ model_name }}.parquet')
      {%- endif -%}
    {%- else -%}
      {%- set src_schema = var('postgres_source_schema', 'dbt_dev_staging') -%}
      pg_src."{{ src_schema }}"."{{ model_name }}"
    {%- endif -%}
  {%- else -%}
    {{ ref(model_name) }}
  {%- endif -%}
{% endmacro %}
