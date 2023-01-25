with
stock as (
    select * from {{ ref('stg_tushare_stock_basic') }}
),

daily_index as (
    select * from {{ ref('int_daily_basic_index_latest') }}
),

balance as (
    select * from {{ ref('int_balance_sheet_latest') }}
),

income as (
    select * from {{ ref('int_income_statement_latest') }}
),

income_pivoted as (
    select * from {{ ref('int_income_pivoted_to_stock') }}
),

qi_a_index_defence as (
    select
        balance.stock_code,
        balance.end_date,
        {{ insolvent_index('total_cur_assets', 'total_cur_liab',
        'lt_borr', 'bond_payable') }} as insolvent_index,
        {{ impairment_goodwill_index('goodwill', 'r_and_d', 'intan_assets',
        'total_hldr_eqy_exc_min_int', 'oth_eqt_tools_p_shr', 'oth_eqt_tools') }} as impairment_goodwill_index,
        {{ current_ratio('total_cur_assets','total_cur_liab') }} as current_ratio,
        {{ receivables_index('accounts_receiv', 'notes_receiv', 'oth_receiv', 'lt_rec', 'total_revenue_ttm') }} as receivables_index,
        net_income_ttm,
        net_income_exclude_minority as net_income_latest,
        round((total_revenue - total_revenue_last_year)) as total_revenue_yoy,
        round((net_income_exclude_minority - net_income_last_year)) as net_income_yoy

    from balance
    left join income_pivoted
    on balance.stock_code = income_pivoted.stock_code and balance.end_date = income_pivoted.last_end_date
    left join income
    on balance.stock_code = income.stock_code and balance.end_date = income.end_date
),

growth_joined as (
    select
        income.stock_code,
        income.end_date,
        (net_income_exclude_minority - coalesce(extraordinary_items,0)) as net_income_pure,
        (net_income_last_year - coalesce(extraordinary_items_last_year,0)) as net_income_pure_last_year,
        total_revenue,
        total_revenue_last_year
    from income
    left join income_pivoted
        on income.stock_code = income_pivoted.stock_code
),

growth as (
    select
        stock_code,
        end_date,
        {{ ratio('net_income_pure', 'net_income_pure_last_year',4 ) }} as net_income_yoy_ratio,
        {{ ratio('total_revenue', 'total_revenue_last_year', 4) }} as total_revenue_yoy_ratio
    from growth_joined
),

peg as (
    select
        growth.stock_code,
        growth.end_date,
        {{ peg('pe_ttm', 'net_income_yoy_ratio', 3) }} as peg_by_net_income,
        {{ peg('pe_ttm', 'total_revenue_yoy_ratio', 3) }} as peg_by_revenue
    from growth
    left join daily_index
        on growth.stock_code = daily_index.stock_code
),

final as (
    select
        stock.stock_code,
        stock.stock_name,
        stock.industry,
        qi_a_index_defence.end_date,
        qi_a_index_defence.insolvent_index,
        qi_a_index_defence.impairment_goodwill_index,
        qi_a_index_defence.current_ratio,
        qi_a_index_defence.receivables_index,
        qi_a_index_defence.net_income_ttm,
        qi_a_index_defence.net_income_latest,
        qi_a_index_defence.total_revenue_yoy,
        qi_a_index_defence.net_income_yoy,
        growth.total_revenue_yoy_ratio,
        growth.net_income_yoy_ratio,
        peg.peg_by_net_income,
        peg.peg_by_revenue,
        daily_index.close,
        daily_index.pe_ttm,
        daily_index.dividend_ratio,
        daily_index.dividend_ratio_ttm,
        daily_index.total_market_capitalization
    from stock
    left join qi_a_index_defence
        on stock.stock_code = qi_a_index_defence.stock_code
    left join daily_index
        on stock.stock_code = daily_index.stock_code
    left join growth
        on stock.stock_code = growth.stock_code
    left join peg
        on stock.stock_code = peg.stock_code
)

select * from final