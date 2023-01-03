with
balance_sheet as (
    select * from {{ ref('stg_tushare_balance_sheet') }}
),

income as (
    select * from {{ ref('stg_tushare_income_statement') }}
),

final as (
    select
        balance_sheet.ts_code,
        balance_sheet.end_date
    from balance_sheet
    left join income
    on balance_sheet.ts_code = income.ts_code
    and balance_sheet.end_date = income.end_date
)

select * from final