{{
    config(
        materialized='table',
        schema='marts',
        tags=['marts', 'toscrape', 'books']
    )
}}

/*
    Book dimension table.
    One row per unique book UPC.
*/

with source as (
    select * from {{ dual_ref('stg_toscrape_books') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['upc']) }} as book_key,
    upc,
    title,
    category,
    star_rating,
    price,
    price_incl_tax,
    tax,
    availability_text,
    stock_count,
    is_in_stock,
    price_tier,
    rating_category,
    product_url,
    image_url,
    extracted_at,
    extraction_date
from source
