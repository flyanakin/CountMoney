with
portfolio as (
    select * from {{ ref('int_portfolio') }}
),

metrics as (
    select
        stock_code,
        round((cost*position), 2) as total_cost,
        round((close - cost)*position, 2) as profit,
        round((close - cost) / cost, 4) as profit_ratio,
        round((close * position),2) as market_capitalization
    from portfolio
),

final as (
    select
       portfolio.stock_code          as stock_code,
       portfolio.stock_name          as stock_name,
       portfolio.position            as position,
       portfolio.cost                as cost,
       portfolio.close               as last,
       metrics.total_cost            as total_cost,
       metrics.market_capitalization as market_capitalization,
       metrics.profit                as profit,
       metrics.profit_ratio          as profit_ratio,
       portfolio.sub_portfolio       as sub_portfolio,
       portfolio.industry            as industry,
       portfolio.order_date          as order_date
    from portfolio
    left join metrics
    on portfolio.stock_code = metrics.stock_code
)

select * from final