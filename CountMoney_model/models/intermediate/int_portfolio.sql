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
        (case
            when portfolio.stock_name = '现金' then portfolio.cost
            else daily_index.close
        end) as close,
        portfolio.sub_portfolio,
        portfolio.order_date,
        stock.industry
    from portfolio
    left join daily_index
    on portfolio.stock_code = daily_index.stock_code
    left join stock
    on portfolio.stock_code = stock.stock_code
),

final as (
    select * from portfolio_full
)

select * from final