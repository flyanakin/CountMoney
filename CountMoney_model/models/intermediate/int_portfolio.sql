with
stock as (
    select * from {{ ref('stg_tushare_stock_basic') }}
),

bond as (
    select * from {{ ref('stg_tushare_convertible_bond_basic') }}
),

bond_daily as (
    select * from {{ ref('stg_tushare_convertible_bond_daily') }}
),

portfolio_stock as (
    select * from {{ ref('stg_airtable_portfolio') }}
    where asset_type = '股票'
),

portfolio_cash as (
    select * from {{ ref('stg_airtable_portfolio') }}
    where asset_type = '现金'
),

portfolio_bond as (
    select * from {{ ref('stg_airtable_portfolio') }}
    where asset_type = '债券'
),

daily_index as (
    select * from {{ ref('int_daily_basic_index_latest') }}
),

stock_asset as (
    select
        portfolio_stock.code,
        portfolio_stock.asset_name,
        portfolio_stock.asset_type,
        portfolio_stock.position,
        portfolio_stock.cost,
        daily_index.close,
        portfolio_stock.sub_portfolio,
        portfolio_stock.order_date,
        stock.industry
    from portfolio_stock
    left join daily_index
    on portfolio_stock.code = daily_index.stock_code
    left join stock
    on portfolio_stock.code = stock.stock_code
),

cash_asset as (
    select
        code,
        asset_name,
        asset_type,
        position,
        cost,
        cost as close,
        sub_portfolio,
        order_date,
        null as industry
    from portfolio_cash
),

bond_asset as (
    select
        portfolio_bond.code,
        portfolio_bond.asset_name,
        portfolio_bond.asset_type,
        portfolio_bond.position,
        portfolio_bond.cost,
        bond_daily.close,
        portfolio_bond.sub_portfolio,
        portfolio_bond.order_date,
        stock.industry
    from portfolio_bond
    left join bond_daily
    on portfolio_bond.code = bond_daily.bond_code
    left join bond
    on portfolio_bond.code = bond.bond_code
    left join stock
    on bond.stock_code = stock.stock_code
),

final as (
    select * from stock_asset
    union all
    select * from bond_asset
    union all
    select * from cash_asset
)

select * from final