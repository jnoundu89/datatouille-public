{{
    config(
        materialized='view',
        tags=['staging', 'toscrape', 'books']
    )
}}

/*
    Staging Model: stg_toscrape_books

    Purpose: Clean and standardize book catalog data from landing zone
    Source: toscrape_books table (populated by toscrape_books_ingestion DAG)
    Grain: One row per unique book (by UPC)
*/

with source as (
    select * from {{ source('landing', 'toscrape_books') }}
),

cleaned as (
    select
        -- Primary Key
        id as book_id,

        -- Identifiers
        trim(upc) as upc,

        -- Core attributes
        trim(title) as title,
        initcap(trim(category)) as category,
        star_rating,
        price,
        price_incl_tax,
        tax,

        -- Availability
        trim(availability) as availability_text,
        stock_count,
        case when stock_count > 0 then true else false end as is_in_stock,

        -- Content
        trim(description) as description,
        image_url,
        product_url,

        -- Price tiers
        case
            when price < 15 then 'Budget'
            when price < 35 then 'Mid-range'
            when price < 55 then 'Premium'
            else 'Luxury'
        end as price_tier,

        -- Rating category
        case
            when star_rating >= 4 then 'Excellent'
            when star_rating >= 3 then 'Good'
            when star_rating >= 2 then 'Average'
            else 'Poor'
        end as rating_category,

        -- Extraction metadata
        extracted_at,
        extraction_date,
        created_at,
        current_timestamp as _dbt_loaded_at

    from source
    where upc is not null
      and length(trim(upc)) >= 2
)

select * from cleaned
