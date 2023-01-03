with import as (
    select * from {{ source('tushare', 'tushare_daily_basic_index') }}
),

logic as (
    select * from import
),

final as (
    select * from logic
)

select * from final
