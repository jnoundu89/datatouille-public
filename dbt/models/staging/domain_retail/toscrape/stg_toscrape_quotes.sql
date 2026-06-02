{{
    config(
        materialized='view',
        tags=['staging', 'toscrape', 'quotes']
    )
}}

/*
    Staging Model: stg_toscrape_quotes

    Purpose: Clean and standardize quote data from landing zone
    Source: toscrape_quotes + toscrape_authors (populated by toscrape_quotes_ingestion DAG)
    Grain: One row per unique quote (by quote_hash)
*/

with quotes as (
    select * from {{ source('landing', 'toscrape_quotes') }}
),

authors as (
    select * from {{ source('landing', 'toscrape_authors') }}
),

cleaned as (
    select
        -- Primary Key
        q.id as quote_id,

        -- Identifiers
        q.quote_hash,
        q.author_slug,

        -- Core attributes
        trim(q.text) as quote_text,
        trim(q.author_name) as author_name,
        q.tags as tags_json,

        -- Author enrichment
        a.born_date as author_born_date,
        a.born_location as author_born_location,

        -- Derived
        length(q.text) as quote_length,
        case
            when length(q.text) < 100 then 'Short'
            when length(q.text) < 250 then 'Medium'
            else 'Long'
        end as quote_length_category,

        -- Extraction metadata
        q.extracted_at,
        q.extraction_date,
        q.created_at,
        current_timestamp as _dbt_loaded_at

    from quotes q
    left join authors a on q.author_slug = a.slug
    where q.quote_hash is not null
)

select * from cleaned
