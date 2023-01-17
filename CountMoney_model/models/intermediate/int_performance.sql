with
performance_import as (
    select * from {{ ref('stg_tushare_forecast') }}
),

daily_index as (
    select * from {{ ref('int_daily_basic_index_latest') }}
),

income_import as (
    select * from {{ ref('stg_tushare_income_statement') }}
),

income_pivoted as (
    select * from {{ ref('int_income_past_year') }}
),

basic as (
    select * from {{ ref('stg_tushare_stock_basic') }}
),

performance as (
    select
        *,
        substring((end_date::text) from 1 for 4) as __year,
        extract(month from end_date) / 3 as statement_period_num
    from performance_import
),

income_deduplicated as (
    --根据发布日期和更新标志去重
    select * from (
        select
            *,
            row_number() over (
                partition by stock_code,end_date
                order by f_ann_date desc, update_flag desc
                ) as rn1
        from income_import) as t
    where t.rn1 = 1
),

income_quarter_ordered as (
    select
        *,
        row_number() over (
            partition by stock_code
            order by end_date desc
            ) as order_num
    from income_deduplicated
),

income_last_3_quarter as (
    --取过去3季的利润表
    select * from income_quarter_ordered
    where order_num in (1,2,3)
),

income_last_year as (
    --取去年同期的单季利润表
    select * from income_quarter_ordered
    where order_num in (4)
),

income_sum_3_quarter as (
    select
        stock_code,
        sum(net_income) as net_income_last_3q,
        sum(net_income_exclude_minority) as net_income_exclude_minority_last_3q
    from income_last_3_quarter
    group by stock_code
),

--计算单季度的净利润预测
performance_quarterly as (
    select
        performance.stock_code,
        performance.end_date,
        (performance.net_income_min - income_pivoted.net_income) as net_income_quarterly_min,
        (performance.net_income_max - income_pivoted.net_income) as net_income_quarterly_max
    from performance
    left join income_pivoted
    on performance.stock_code = income_pivoted.stock_code
    and performance.__year = income_pivoted.year
    and (performance.statement_period_num - 1) = income_pivoted.statement_period_num
),

final as (
    select
        performance.stock_code                          as stock_code,
        basic.stock_name                                as stock_name,
        performance.ann_date                            as ann_date,
        performance.end_date                            as end_date,
        performance.trend                               as trend,
        performance.net_income_min                      as net_income_min,
        performance.net_income_max                      as net_income_max,
        performance.net_income_stockholder_last_year    as net_income_stockholder_last_year,
        performance_quarterly.net_income_quarterly_min  as net_income_quarterly_min,
        performance_quarterly.net_income_quarterly_max  as net_income_quarterly_max,
        income_sum_3_quarter.net_income_last_3q         as net_income_last_3quarter,
        income_last_year.net_income                     as net_income_last_year_quarterly,
        daily_index.total_market_capitalization         as total_market_capitalization,
        basic.industry                                  as industry,
        performance.summary                              as summary,
        performance.change_reason                       as change_reason
    from performance
    left join income_last_year using(stock_code)
    left join income_sum_3_quarter using(stock_code)
    left join performance_quarterly using(stock_code)
    left join basic using(stock_code)
    left join daily_index using(stock_code)
)

select * from final