with
stock as (
    select * from {{ ref('stg_tushare_stock_basic') }}
),

daily_index as (
    select * from {{ ref('int_daily_basic_index_lastest') }}
),

balance as (
    select * from {{ ref('int_balance_sheet_lastest') }}
),

income as (
    select * from {{ ref('int_income_statement_lastest') }}
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
        net_income_exclude_minority as net_income_lastest,
        (total_revenue - total_revenue_last_year) as growth_total_revenue_yearly,
        (net_income_exclude_minority - net_income_last_year) as growth_net_income_yearly,

    from balance
    left join income_pivoted
    on balance.stock_code = income_pivoted.stock_code and balance.end_date = income_pivoted.last_end_date
    left join income
    on balance.stock_code = income.stock_code and balance.end_date = income.end_date
),

final as (
    select
        stock.stock_code,
        stock.stock_name,

    from stock
    left join qi_a_index_defence
        on stock.stock_code = qi_a_index_defence.stock_code
)

select * from final