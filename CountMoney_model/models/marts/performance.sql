with
performance as (
    select * from {{ ref('int_performance') }}
),

metrics as (
    select
        stock_code,
        {{ ratio('net_income_min', 'net_income_last_year_quarterly', 4) }} as growth_net_income_quarterly_ratio_min,
        {{ ratio('net_income_max', 'net_income_last_year_quarterly', 4) }} as growth_net_income_quarterly_ratio_max,
        (net_income_min + net_income_last_3quarter) as net_income_ttm_min,
        (net_income_max + net_income_last_3quarter) as net_income_ttm_max,
        round(total_market_capitalization / (net_income_min + net_income_last_3quarter) ,2) as pe_ttm_max,
        round(total_market_capitalization / (net_income_max + net_income_last_3quarter) ,2) as pe_ttm_min
    from performance
),

peg as (
    select
        stock_code,
        {{ peg('pe_ttm_max', 'growth_net_income_quarterly_ratio_min', 4) }} as peg_max,
        {{ peg('pe_ttm_min', 'growth_net_income_quarterly_ratio_max', 4) }} as peg_min
    from metrics
),

final as (
    select
        performance.stock_code                          as stock_code                           ,
        performance.stock_name                          as stock_name                           ,
        performance.ann_date                            as ann_date                             ,
        performance.end_date                            as end_date                             ,
        performance.trend                               as trend                                ,
        metrics.growth_net_income_quarterly_ratio_min   as growth_net_income_quarterly_ratio_min,
        metrics.growth_net_income_quarterly_ratio_max   as growth_net_income_quarterly_ratio_max,
        metrics.net_income_ttm_min                      as net_income_ttm_min                   ,
        metrics.net_income_ttm_max                      as net_income_ttm_max                   ,
        metrics.pe_ttm_min                              as pe_ttm_min                           ,
        metrics.pe_ttm_max                              as pe_ttm_max                           ,
        peg.peg_min                                     as peg_min                              ,
        peg.peg_max                                     as peg_max                              ,
        performance.industry                            as industry                             ,
        performance.summary                             as summary                              ,
        performance.change_reason                       as change_reason                        
    from performance
    left join metrics using(stock_code)
    left join peg using(stock_code)
)

select * from final