with import as (
    select * from {{ source('tushare', 'tushare_balance_sheet') }}
),

logic as (
    select * from import
),

final as (
    select * from logic
)

select * from final
