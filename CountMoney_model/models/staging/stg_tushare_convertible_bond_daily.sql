{% set number_fields =  ["close"] %}



with import as (
    select * from (
        select
            *,
            row_number() over (
                partition by ts_code,trade_date
                order by created_at desc
                ) as rn_created
        from {{ source('tushare', 'tushare_convertible_bond_daily') }}) as partitioned
    where partitioned.rn_created = 1
),

latest as (
    select * from (
        select
            *,
            row_number() over (
                partition by ts_code
                order by trade_date desc
                ) as rn_trade_date
        from {{ source('tushare', 'tushare_convertible_bond_daily') }}) as partitioned
    where partitioned.rn_trade_date = 1
),

formatted as (
    select
        ts_code,
        {{ tushare_date_formatted('trade_date') }},
        {% for number_field in number_fields %}
        round({{ number_field }}::numeric, 2) as {{ number_field }},
        {%- endfor %}
        created_at
    from latest
),

final as (
    select
        formatted.ts_code              as bond_code,
        formatted.trade_date           as trade_date,
        formatted.close                as close,
        formatted.created_at           as created_at
    from formatted
)

select * from final
