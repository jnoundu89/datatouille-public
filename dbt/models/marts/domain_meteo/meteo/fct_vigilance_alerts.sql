{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key='alert_key',
        incremental_strategy='merge'
    )
}}

/*
    Fact table for vigilance weather alerts.
    Incremental load - one row per (department, phenomenon, begin_time, extraction_date).
*/

with alerts as (
    select
        department_code,
        department_name,
        phenomenon_id,
        phenomenon_name,
        begin_time,
        end_time,
        color_id,
        color_name,
        color_level,
        color_hex,
        extracted_at,
        extraction_date

    from {{ dual_ref('stg_vigilance_alerts') }}

    {% if is_incremental() %}
    where extraction_date > (select max(extraction_date) from {{ this }})
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'department_code',
        'phenomenon_id',
        'begin_time',
        'extraction_date'
    ]) }} as alert_key,

    -- Dimension FKs
    {{ dbt_utils.generate_surrogate_key(['department_code']) }} as department_key,

    -- Degenerate dimensions
    department_code,
    department_name,
    phenomenon_id,
    phenomenon_name,
    color_name,

    -- Measures
    color_level,
    color_hex,
    extract(epoch from (end_time - begin_time)) / 3600.0 as duration_hours,

    -- Time
    begin_time,
    end_time,
    extraction_date,
    extracted_at

from alerts
