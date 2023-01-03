with import as (
    select * from {{ source('tushare', 'tushare_income_statement') }}
),

logic as (
    select * from import
),

final as (
    select * from logic
)

select * from final
