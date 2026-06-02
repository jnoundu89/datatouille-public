{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for GeoRisques commune risk profiles.
    Cleans and standardizes natural/technological risk data.
*/

with source as (
    select * from {{ source('meteo', 'georisques_commune_profiles') }}
),

cleaned as (
    select
        id as profile_row_id,
        trim(insee_code) as insee_code,
        trim(commune) as commune,
        trim(code_postal) as code_postal,
        trim(department_code) as department_code,
        trim(department_name) as department_name,
        trim(region) as region,

        -- Risk counts
        coalesce(naturels_count, 0) as naturels_count,
        coalesce(technologiques_count, 0) as technologiques_count,
        coalesce(total_risks, 0) as total_risks,
        coalesce(max_severity_naturels, 0) as max_severity_naturels,
        coalesce(max_severity_technologiques, 0) as max_severity_technologiques,

        -- Specific risks
        coalesce(seisme_present, false) as seisme_present,
        coalesce(seisme_severity, 0) as seisme_severity,
        coalesce(rga_present, false) as rga_present,
        coalesce(rga_severity, 0) as rga_severity,
        coalesce(radon_present, false) as radon_present,
        coalesce(radon_severity, 0) as radon_severity,
        coalesce(inondation_present, false) as inondation_present,
        coalesce(inondation_severity, 0) as inondation_severity,
        coalesce(icpe_present, false) as icpe_present,
        coalesce(nucleaire_present, false) as nucleaire_present,

        -- Timestamps
        extracted_at,
        extraction_date,
        created_at

    from source
    where insee_code is not null
)

select * from cleaned
