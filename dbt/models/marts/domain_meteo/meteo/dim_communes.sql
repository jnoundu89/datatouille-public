{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
    Commune dimension table.
    One row per INSEE code with latest risk profile from GeoRisques.
*/

with latest_extraction as (
    select max(extraction_date) as max_date
    from {{ dual_ref('stg_georisques_commune_profiles') }}
),

latest_profiles as (
    select
        cp.*
    from {{ dual_ref('stg_georisques_commune_profiles') }} cp
    cross join latest_extraction le
    where cp.extraction_date = le.max_date
),

risk_summary as (
    select
        insee_code,
        count(*) filter (where present and risk_category = 'naturel') as active_natural_risks,
        count(*) filter (where present and risk_category = 'technologique') as active_techno_risks,
        string_agg(
            case when present then risk_libelle end,
            ', ' order by risk_category, risk_key
        ) as active_risks_list
    from {{ dual_ref('stg_georisques_risk_details') }}
    cross join latest_extraction le
    where extraction_date = le.max_date
    group by insee_code
),

extraction_history as (
    select
        insee_code,
        min(extraction_date) as first_extraction,
        max(extraction_date) as last_extraction,
        count(distinct extraction_date) as extraction_count
    from {{ dual_ref('stg_georisques_commune_profiles') }}
    group by insee_code
)

select
    {{ dbt_utils.generate_surrogate_key(['lp.insee_code']) }} as commune_id,
    lp.insee_code,
    lp.commune,
    lp.code_postal,
    lp.department_code,
    lp.department_name,
    lp.region,

    -- Risk counts
    lp.naturels_count,
    lp.technologiques_count,
    lp.total_risks,
    lp.max_severity_naturels,
    lp.max_severity_technologiques,

    -- Key risk indicators
    lp.seisme_present,
    lp.seisme_severity,
    lp.rga_present,
    lp.rga_severity,
    lp.radon_present,
    lp.radon_severity,
    lp.inondation_present,
    lp.inondation_severity,
    lp.icpe_present,
    lp.nucleaire_present,

    -- Derived
    rs.active_risks_list,
    greatest(lp.max_severity_naturels, lp.max_severity_technologiques) as overall_max_severity,

    -- History
    eh.first_extraction,
    eh.last_extraction,
    eh.extraction_count

from latest_profiles lp
left join risk_summary rs on lp.insee_code = rs.insee_code
left join extraction_history eh on lp.insee_code = eh.insee_code
