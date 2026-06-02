{{
    config(
        materialized='postgres_writeback',
        postgres_schema=var('postgres_writeback_schema', 'dbt_dev_marts'),
        postgres_table='rpt_store_risk_summary'
    )
}}

/*
    Store location risk summary.
    Combines latest GeoRisques profile with recent vigilance alerts
    for a unified risk view per department.
*/

with latest_vigilance as (
    select max(extraction_date) as max_date
    from {{ ref('fct_vigilance_alerts') }}
),

dept_alert_summary as (
    select
        va.department_code,
        va.department_name,
        count(*) as active_alert_count,
        max(va.color_level) as max_alert_level,
        string_agg(
            distinct va.phenomenon_name || ' (' || va.color_name || ')',
            ', ' order by va.phenomenon_name || ' (' || va.color_name || ')'
        ) as active_alerts_list,
        count(*) filter (where va.color_level >= 3) as orange_red_count
    from {{ ref('fct_vigilance_alerts') }} va
    cross join latest_vigilance lv
    where va.extraction_date = lv.max_date
    group by va.department_code, va.department_name
),

dept_risk_summary as (
    select
        department_code,
        count(*) as commune_count,
        round(avg(total_risks)::numeric, 1) as avg_risks_per_commune,
        max(total_risks) as max_risks_in_dept,
        round(avg(max_severity_naturels)::numeric, 1) as avg_severity_naturels,
        round(avg(max_severity_technologiques)::numeric, 1) as avg_severity_technologiques,
        count(*) filter (where seisme_present) as communes_with_seisme,
        count(*) filter (where inondation_present) as communes_with_inondation,
        count(*) filter (where radon_present) as communes_with_radon,
        count(*) filter (where icpe_present) as communes_with_icpe,
        count(*) filter (where nucleaire_present) as communes_with_nucleaire
    from {{ ref('dim_communes') }}
    group by department_code
)

select
    coalesce(drs.department_code, das.department_code) as department_code,
    coalesce(das.department_name, c.department_name) as department_name,
    c.region,

    -- GeoRisques (structural risks)
    coalesce(drs.commune_count, 0) as monitored_communes,
    coalesce(drs.avg_risks_per_commune, 0) as avg_risks_per_commune,
    coalesce(drs.max_risks_in_dept, 0) as max_risks_in_dept,
    drs.avg_severity_naturels,
    drs.avg_severity_technologiques,
    coalesce(drs.communes_with_seisme, 0) as communes_with_seisme,
    coalesce(drs.communes_with_inondation, 0) as communes_with_inondation,
    coalesce(drs.communes_with_radon, 0) as communes_with_radon,
    coalesce(drs.communes_with_icpe, 0) as communes_with_icpe,
    coalesce(drs.communes_with_nucleaire, 0) as communes_with_nucleaire,

    -- Vigilance (weather alerts)
    coalesce(das.active_alert_count, 0) as active_alert_count,
    coalesce(das.max_alert_level, 0) as max_alert_level,
    das.active_alerts_list,
    coalesce(das.orange_red_count, 0) as orange_red_count,

    current_timestamp as generated_at

from dept_risk_summary drs
full outer join dept_alert_summary das
    on drs.department_code = das.department_code
left join (
    select distinct department_code, department_name, region
    from {{ ref('dim_communes') }}
) c on coalesce(drs.department_code, das.department_code) = c.department_code

order by coalesce(das.max_alert_level, 0) desc, coalesce(drs.max_risks_in_dept, 0) desc
