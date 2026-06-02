{{
    config(
        materialized='table',
        schema='marts',
        tags=['marts', 'toscrape', 'quotes']
    )
}}

/*
    Author dimension table.
    One row per unique author slug.
*/

with source as (
    select distinct
        author_slug,
        author_name,
        author_born_date,
        author_born_location
    from {{ dual_ref('stg_toscrape_quotes') }}
    where author_slug is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['author_slug']) }} as author_key,
    author_slug,
    author_name,
    author_born_date,
    author_born_location
from source
