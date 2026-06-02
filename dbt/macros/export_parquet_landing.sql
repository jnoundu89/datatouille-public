{#
    export_parquet_landing: snapshot dbt-managed Postgres staging views to
    Parquet files on MinIO, so the DuckDB target can skip the postgres_scanner
    over-wire read and materialize marts directly from columnar storage.

    Two layouts, selected per model:

    * Single-file (default):
          s3://<base>/<model>.parquet
      Used for reference/registry views without a date dimension.

    * Hive-partitioned by a date column:
          s3://<base>/<model>/extraction_date=YYYY-MM-DD/data_0.parquet
      Used for append-only fact staging (listings, offers, products) so
      incremental reads can prune old partitions and daily re-dumps
      rewrite a single partition rather than the whole table.

    Usage (dbt-duckdb target only):
        dbt run-operation export_parquet_landing \
          --target duckdb \
          --args '{
             "models": ["stg_leboncoin_listings", "stg_leboncoin_search_registry"],
             "partitioned_models": ["stg_leboncoin_listings"],
             "partition_column": "extraction_date"
          }'

    Requires: profile target=duckdb with `attach` pg_src + httpfs MinIO settings.
#}

{% macro export_parquet_landing(
    models,
    src_schema=None,
    s3_base=None,
    partitioned_models=None,
    partition_column='extraction_date'
) %}
  {%- if target.type != 'duckdb' -%}
    {{ exceptions.raise_compiler_error(
      "export_parquet_landing must run against the duckdb target"
    ) }}
  {%- endif -%}

  {%- set src_schema = src_schema or var('postgres_source_schema', 'dbt_dev_staging') -%}
  {%- set s3_base = s3_base or var('duckdb_parquet_base', 's3://datatouille/landing') -%}
  {%- set partitioned_set = (partitioned_models or []) | list -%}

  {%- for model_name in models -%}
    {%- if model_name in partitioned_set -%}
      {# Hive-partitioned: COPY to a directory, one subdir per partition #}
      {%- set path = s3_base ~ '/' ~ model_name -%}
      {% set sql %}
        copy (select * from pg_src."{{ src_schema }}"."{{ model_name }}")
        to '{{ path }}'
        (format parquet, partition_by ({{ partition_column }}), overwrite true)
      {% endset %}
      {{ log(
        "export_parquet_landing: " ~ model_name
        ~ " -> " ~ path ~ "/ (partitioned by " ~ partition_column ~ ")",
        info=True
      ) }}
    {%- else -%}
      {%- set path = s3_base ~ '/' ~ model_name ~ '.parquet' -%}
      {% set sql %}
        copy (select * from pg_src."{{ src_schema }}"."{{ model_name }}")
        to '{{ path }}' (format parquet, overwrite true)
      {% endset %}
      {{ log("export_parquet_landing: " ~ model_name ~ " -> " ~ path, info=True) }}
    {%- endif -%}
    {% do run_query(sql) %}
  {%- endfor -%}
{% endmacro %}
