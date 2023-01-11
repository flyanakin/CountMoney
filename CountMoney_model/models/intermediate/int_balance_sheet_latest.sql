
with
import as (
    select * from {{ ref('stg_tushare_balance_sheet') }}
),

deduplicated as (
    --根据发布日期和更新标志去重
    select * from (
        select
            *,
            row_number() over (
                partition by stock_code,end_date
                order by f_ann_date desc, update_flag desc
                ) as rn1
        from import) as t
    where t.rn1 = 1
),

latest as (
    select * from (
        select
            *,
            row_number() over (
                partition by stock_code
                order by end_date desc
                ) as rn2
        from deduplicated) as t
    where t.rn2 = 1
),

final as (
    select
        *
    from latest
)
select * from final
