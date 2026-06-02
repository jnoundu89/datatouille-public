{#
    Custom materialization: postgres_writeback (dbt-duckdb only).

    Computes the model in DuckDB (fast columnar execution), then replaces the
    corresponding table in the attached Postgres database (alias pg_src) so
    that Grafana and other Postgres-facing tools see the fresh result without
    any plugin or dashboard change.

    Required model config:
        postgres_schema: str   # e.g. 'dbt_dev_marts'
        postgres_table:  str   # e.g. 'rpt_listing_price_trends_duckdb'

    Write strategy: atomic DROP + CREATE AS SELECT through postgres_scanner.
    Readers during the swap see either the old table (before DROP commits) or
    the new one (after CREATE commits). For POC volumes this is acceptable;
    a rename-swap pattern can be added later if zero-downtime becomes critical.
#}

{% materialization postgres_writeback, adapter='duckdb' %}
  {%- set target_schema = config.get('postgres_schema') -%}
  {%- set target_table = config.get('postgres_table', model['alias']) -%}

  {% if not target_schema %}
    {{ exceptions.raise_compiler_error(
      "postgres_writeback materialization requires config 'postgres_schema'"
    ) }}
  {% endif %}

  {%- set local_relation = this.incorporate(type='table') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- 1. Materialize locally in DuckDB (standard columnar build)
  {% call statement('main') -%}
    create or replace table {{ local_relation }} as (
      {{ sql }}
    )
  {%- endcall %}

  -- DuckDB refuses cross-database writes inside a single transaction.
  -- Commit the local build before reaching for the attached Postgres.
  {% do adapter.commit() %}

  -- 2. Ensure the target Postgres schema exists (first run on a fresh DB
  -- would otherwise fail on the CREATE TABLE below with
  -- "Binder Error: Schema ... not found").
  {% call statement('writeback_ensure_schema') -%}
    create schema if not exists pg_src."{{ target_schema }}"
  {%- endcall %}
  {% do adapter.commit() %}

  -- 3. Replace the Postgres table via postgres_scanner (each statement = its own TX)
  {% call statement('writeback_drop') -%}
    drop table if exists pg_src."{{ target_schema }}"."{{ target_table }}"
  {%- endcall %}
  {% do adapter.commit() %}

  {% call statement('writeback_create') -%}
    create table pg_src."{{ target_schema }}"."{{ target_table }}" as
    select * from {{ local_relation }}
  {%- endcall %}
  {% do adapter.commit() %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [local_relation]}) }}
{% endmaterialization %}
