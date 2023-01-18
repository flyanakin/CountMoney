with
preview_import as (
    select * from {{ ref('stg_tushare_express') }}
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

preview as (
    select
        *,
        substring((end_date::text) from 1 for 4) as __year,
        extract(month from end_date) / 3 as statement_period_num
    from preview_import
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
        sum(total_revenue) as total_revenue_last_3q
    from income_last_3_quarter
    group by stock_code
),

--计算单季度的净利润营收数据
preview_quarterly as (
    select
        preview.stock_code,
        preview.end_date,
        (preview.net_income - income_pivoted.net_income) as net_income_quarterly,
        (preview.total_revenue - income_pivoted.total_revenue) as total_revenue_quarterly
    from preview
    left join income_pivoted
    on preview.stock_code = income_pivoted.stock_code
    and preview.__year = income_pivoted.year
    and (preview.statement_period_num - 1) = income_pivoted.statement_period_num
),

final as (
    select
        preview.stock_code                            as stock_code,
        basic.stock_name                              as stock_name,
        preview.ann_date                              as ann_date,
        preview.end_date                              as end_date,
        preview.net_income                            as net_income,
        preview.total_revenue                         as total_revenue,
        preview.diluted_roe                           as diluted_roe,
        preview_quarterly.net_income_quarterly        as net_income_quarterly,
        preview_quarterly.total_revenue_quarterly     as total_revenue_quarterly,
        income_sum_3_quarter.net_income_last_3q       as net_income_last_3quarter,
        income_sum_3_quarter.total_revenue_last_3q    as total_revenue_last_3quarter,
        income_last_year.net_income                   as net_income_last_year_quarterly,
        income_last_year.total_revenue                as total_revenue_last_year_quarterly,
        daily_index.total_market_capitalization       as total_market_capitalization,
        basic.industry                                as industry,
        preview.perf_summary                          as perf_summary
    from preview
    left join income_last_year using(stock_code)
    left join income_sum_3_quarter using(stock_code)
    left join preview_quarterly using(stock_code)
    inner join basic using(stock_code)
    left join daily_index using(stock_code)
)

select * from final