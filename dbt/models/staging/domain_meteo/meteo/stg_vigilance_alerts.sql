{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for Météo France vigilance alerts.
    Cleans and standardizes weather alert data by department.
*/

with source as (
    select * from {{ source('meteo', 'vigilance_alerts') }}
),

cleaned as (
    select
        id as alert_row_id,
        trim(department_code) as department_code,
        trim(department_name) as department_name,
        phenomenon_id,
        trim(phenomenon_name) as phenomenon_name,
        begin_time,
        end_time,
        color_id,
        trim(color_name) as color_name,
        coalesce(color_level, 0) as color_level,
        trim(color_hex) as color_hex,
        extracted_at,
        extraction_date,
        created_at

    from source
    where department_code is not null
      and phenomenon_name is not null
)

select * from cleaned
