with
import as (
    select * from {{ ref('stg_tushare_income_statement') }}
),

stock as (
    select * from {{ ref('stg_tushare_stock_basic') }}
),

indicator as (
    select * from {{ ref('int_finance_indicator_quarterly') }}
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

quarter_ordered as (
    select
        *,
        row_number() over (
            partition by stock_code
            order by end_date desc
            ) as order_num
    from deduplicated
),

indicator_ordered as (
    select
        *,
        row_number() over (
            partition by stock_code
            order by end_date desc
            ) as order_num
    from indicator
),

last_4_quarter as (
    --取过去4季财报
    select * from quarter_ordered
    where order_num in (1,2,3,4)
),

last_year as (
    select
        stock_code,
        total_revenue,
        net_income_exclude_minority
    from quarter_ordered
    where order_num = 5
),

last_year_indicator as (
    select
        stock_code,
        extraordinary_items
    from indicator_ordered
    where order_num = 5
),

last_quarter as (
    select
        stock_code,
        end_date,
        total_revenue,
        net_income_exclude_minority
    from quarter_ordered
    where order_num = 1
),

ttm as (
    select
        stock_code,
        round(sum(total_revenue)) as total_revenue,
        round(sum(net_income_exclude_minority)) as net_income_exclude_minority
    from last_4_quarter
    group by stock_code
),

final as (
    select
        stock.stock_code                        as stock_code,
        last_quarter.end_date                   as last_end_date,
        ttm.net_income_exclude_minority         as net_income_ttm,
        ttm.total_revenue                       as total_revenue_ttm,
        last_year.total_revenue                 as total_revenue_last_year,
        last_year.net_income_exclude_minority   as net_income_last_year,
        last_year_indicator.extraordinary_items as extraordinary_items_last_year
    from stock
    left join ttm using(stock_code)
    left join last_year using(stock_code)
    left join last_quarter using(stock_code)
    left join last_year_indicator using(stock_code)
)

select * from final