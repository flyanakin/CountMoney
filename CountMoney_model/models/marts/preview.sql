with
preview as (
    select * from {{ ref('int_preview') }}
),

metrics as (
    select
        stock_code,
        {{ ratio('net_income', 'net_income_last_year_quarterly', 4) }} as growth_net_income_quarterly_ratio,
        {{ ratio('total_revenue', 'total_revenue_last_year_quarterly', 4) }} as growth_total_revenue_quarterly_ratio,
        (net_income + net_income_last_3quarter) as net_income_ttm,
        (total_revenue + total_revenue_last_3quarter) as total_revenue_ttm,
        round(total_market_capitalization / (net_income + net_income_last_3quarter) ,2) as pe_ttm
    from preview
),

peg as (
    select
        stock_code,
        {{ peg('pe_ttm', 'growth_net_income_quarterly_ratio', 4) }} as peg_by_revenue,
        {{ peg('pe_ttm', 'growth_net_income_quarterly_ratio', 4) }} as peg_by_net_income
    from metrics
),

final as (
    select
        preview.stock_code                                 as stock_code                           ,
        preview.stock_name                                 as stock_name                           ,
        preview.ann_date                                   as ann_date                             ,
        preview.end_date                                   as end_date                             ,
        metrics.growth_net_income_quarterly_ratio          as growth_net_income_quarterly_ratio,
        metrics.growth_total_revenue_quarterly_ratio       as growth_total_revenue_quarterly_ratio,
        metrics.net_income_ttm                             as net_income_ttm                   ,
        metrics.total_revenue_ttm                          as total_revenue_ttm                   ,
        metrics.pe_ttm                                     as pe_ttm                           ,
        peg.peg_by_revenue                                 as peg_by_revenue                              ,
        peg.peg_by_net_income                              as peg_by_net_income                              ,
        preview.diluted_roe                                as diluted_roe,
        preview.industry                                   as industry                             ,
        preview.perf_summary                               as perf_summary
    from preview
    left join metrics using(stock_code)
    left join peg using(stock_code)
)

select * from final