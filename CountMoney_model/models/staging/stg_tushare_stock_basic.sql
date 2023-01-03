with import as (
    select * from {{ source('tushare', 'tushare_stock_basic') }}
),

logic as (
    select * from import
),

final as (
    select * from logic
)

select * from final
