{{ config(materialized='table') }}


with import as (
    select * from {{ source('tushare', 'tushare_convertible_bond_basic') }}
),

formatted as (
    select
        ts_code,
        bond_short_name,
        stk_code,
        stk_short_name
    from import
),

final as (
    select
        ts_code              as bond_code,
        bond_short_name      as bond_short_name,
        stk_code             as stock_code,
        stk_short_name       as stock_name
    from formatted
)

select * from final
