with
stock as (
    select * from {{ ref('stg_tushare_stock_basic') }}
),

portfolio as (
    select * from {{ ref('stg_airtable_portfolio') }}
),

daily_index as (
    select * from {{ ref('int_daily_basic_index_latest') }}
),

portfolio_full as (
    select
        portfolio.stock_code,
        portfolio.stock_name,
        portfolio.position,
        portfolio.cost,
        daily_index.close,
        portfolio.sub_portfolio
    from portfolio
    left join daily_index
    on portfolio.stock_code = daily_index.stock_code
),

metrics as (
    select
        stock_code,
        round((close - cost)*position, 2) as profit,
        round((close - cost) / cost, 4) as profit_ration,
        round((close * position),2) as market_capitalization
    from portfolio_full
),

final as (
    select
       portfolio_full.stock_code     as stock_code,
       portfolio_full.stock_name     as stock_name,
       portfolio_full.position       as position,
       portfolio_full.cost           as cost,
       portfolio_full.close          as last,
       metrics.market_capitalization as market_capitalization,
       metrics.profit                as profit,
       metrics.profit_ration         as profit_ration,
       portfolio_full.sub_portfolio  as sub_portfolio,
       stock.industry                as industry
    from portfolio_full
    left join metrics
    on portfolio_full.stock_code = metrics.stock_code
    left join stock
    on portfolio_full.stock_code = stock.stock_code
)

select * from final