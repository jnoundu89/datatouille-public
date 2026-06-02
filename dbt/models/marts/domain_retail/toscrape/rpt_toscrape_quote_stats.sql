{{
    config(
        materialized='postgres_writeback',
        postgres_schema=var('postgres_writeback_schema', 'dbt_dev_marts'),
        postgres_table='rpt_toscrape_quote_stats',
        tags=['marts', 'toscrape', 'quotes']
    )
}}

/*
    Quotes stats report.
    Aggregates quote counts and lengths by author.
    Writes back to Postgres schema.
*/

with quotes as (
    select * from {{ ref('fct_toscrape_quotes') }}
),

authors as (
    select * from {{ ref('dim_toscrape_authors') }}
)

select
    a.author_name,
    a.author_born_date,
    a.author_born_location,
    count(q.quote_key) as quote_count,
    round(avg(q.quote_length)::numeric, 1) as avg_quote_length,
    min(q.quote_length) as min_quote_length,
    max(q.quote_length) as max_quote_length,
    current_timestamp as generated_at
from authors a
left join quotes q on a.author_key = q.author_key
group by a.author_name, a.author_born_date, a.author_born_location
