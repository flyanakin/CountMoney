{% set number_fields =  ["close"] %}



with import as (
    select * from {{ source('tushare', 'tushare_convertible_bond_daily') }}
),

formatted as (
    select
        ts_code,
        {{ tushare_date_formatted('trade_date') }},
        {% for number_field in number_fields %}
        round({{ number_field }}::numeric, 2) as {{ number_field }},
        {%- endfor %}

        created_at
    from import
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
