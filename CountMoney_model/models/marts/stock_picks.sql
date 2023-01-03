with
basic as (
    select * from {{ ref('stg_tushare_stock_basic') }}
),

statement as (
    select * from (
        select
            *,
            row_number() over (
                partition by ts_code
                order by end_date desc
                ) as rn
        from {{ ref('int_stock_statement') }}) as import
    where import.rn = 1
),

daily_index as (
    select * from (
        select
            *,
            row_number() over (
                partition by ts_code
                order by trade_date desc
                ) as rn
        from {{ ref('stg_tushare_daily_basic_index') }}) as import
    where import.rn = 1
),

final as (
    select
        basic.ts_code
    from basic
    left join statement on statement.ts_code = basic.ts_code
    left join daily_index on daily_index.ts_code = basic.ts_code
)

select * from final