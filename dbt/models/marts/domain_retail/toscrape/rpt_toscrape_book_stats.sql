{{
    config(
        materialized='postgres_writeback',
        postgres_schema=var('postgres_writeback_schema', 'dbt_dev_marts'),
        postgres_table='rpt_toscrape_book_stats',
        tags=['marts', 'toscrape', 'books']
    )
}}

/*
    Book stats report.
    Aggregates book catalog statistics by category.
    Writes back to Postgres schema.
*/

with books as (
    select * from {{ ref('dim_toscrape_books') }}
)

select
    category,
    count(*) as book_count,
    round(avg(price)::numeric, 2) as avg_price,
    round(avg(star_rating)::numeric, 1) as avg_rating,
    min(price) as min_price,
    max(price) as max_price,
    sum(stock_count) as total_stock,
    count(*) filter (where star_rating >= 4) as highly_rated_count,
    current_timestamp as generated_at
from books
group by category
