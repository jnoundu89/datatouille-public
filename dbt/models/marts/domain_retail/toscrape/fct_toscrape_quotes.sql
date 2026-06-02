{{
    config(
        materialized='table',
        schema='marts',
        tags=['marts', 'toscrape', 'quotes']
    )
}}

/*
    Quotes fact table.
    One row per unique quote hash, linking to the author dimension.
*/

with quotes as (
    select * from {{ dual_ref('stg_toscrape_quotes') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['q.quote_hash']) }} as quote_key,
    q.quote_hash,
    {{ dbt_utils.generate_surrogate_key(['q.author_slug']) }} as author_key,
    q.quote_text,
    q.tags_json,
    q.quote_length,
    q.quote_length_category,
    q.extracted_at,
    q.extraction_date
from quotes q
