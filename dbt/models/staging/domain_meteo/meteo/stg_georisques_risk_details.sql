{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for GeoRisques individual risk details.
    One row per (commune, risk_type, extraction_date).
*/

with source as (
    select * from {{ source('meteo', 'georisques_risk_details') }}
),

cleaned as (
    select
        id as detail_row_id,
        trim(insee_code) as insee_code,
        trim(risk_category) as risk_category,
        trim(risk_key) as risk_key,
        trim(risk_libelle) as risk_libelle,
        coalesce(present, false) as present,
        trim(statut_commune) as statut_commune,
        trim(statut_adresse) as statut_adresse,
        trim(severity_label) as severity_label,
        coalesce(severity_score, 0) as severity_score,
        extracted_at,
        extraction_date,
        created_at

    from source
    where insee_code is not null
      and risk_key is not null
)

select * from cleaned
