
with
import as (
    select * from {{ ref('stg_tushare_daily_basic_index') }}
),

lastest as (
    select * from (
        select
            *,
            row_number() over (
                partition by stock_code
                order by trade_date desc
                ) as rn
        from import) as t
    where t.rn = 1
),

final as (
    select
        *
    from lastest
)
select * from final
