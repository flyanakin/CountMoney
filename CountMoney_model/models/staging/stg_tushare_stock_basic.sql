{{ config(materialized='table') }}

with import as (
    select * from {{ source('tushare', 'tushare_stock_basic') }}
    where market in ('中小板','主板','创业板','科创板') --去掉CDR和北交所
    and name not ilike '%ST%'   --去掉st股
),



formatted as (
    select
        ts_code,
        symbol,
        name,
        area,
        industry,
        market,
        {{ tushare_date_formatted('list_date') }}
    from import
),

final as (
    select
        ts_code     as stock_code,
        symbol      as stock_code_normal,
        name        as stock_name,
        area        as regisered_region,
        industry,
        market,
        list_date
    from formatted
)

select * from final
