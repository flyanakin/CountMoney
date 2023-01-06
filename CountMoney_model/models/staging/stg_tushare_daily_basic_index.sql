{% set unit_covert_fields = ["total_share", "float_share", "free_share", "total_mv",
                             "circ_mv"] %}



with import as (
    select * from {{ source('tushare', 'tushare_daily_basic_index') }}
),

formatted as (
    select
        ts_code,
        {{ tushare_date_formatted('trade_date') }},
        close,
        turnover_rate,
        turnover_rate_f,
        volume_ratio,
        pe,
        pe_ttm,
        pb,
        ps,
        ps_ttm,
        dv_ratio,
        dv_ttm,
        total_share,
        float_share,
        free_share,
        total_mv,
        circ_mv,
        created_at
    from import
),

unit_converted as (
    select
        ts_code,
        trade_date,

        {% for unit_covert_field in unit_covert_fields %}
        round({{ unit_covert_field }}*10000) as {{ unit_covert_field }}
        {% if not loop.last %},{% endif %}
        {%- endfor %}

    from formatted
),

final as (
    select
        formatted.ts_code              as stock_code,
        formatted.trade_date           as trade_date,
        formatted.close                as close,
        formatted.turnover_rate        as turnover_rate,
        formatted.turnover_rate_f      as turnover_rate_free,
        formatted.volume_ratio         as volume_ratio,
        formatted.pe                   as pe,
        formatted.pe_ttm               as pe_ttm,
        formatted.pb                   as pb,
        formatted.ps                   as ps,
        formatted.ps_ttm               as ps_ttm,
        formatted.dv_ratio             as dividend_ratio,
        formatted.dv_ttm               as dividend_ratio_ttm,
        unit_converted.total_share     as total_share,
        unit_converted.float_share     as float_share,
        unit_converted.free_share      as free_share,
        unit_converted.total_mv        as total_market_capitalization,
        unit_converted.circ_mv         as circulating_market_capitalization,
        formatted.created_at           as created_at
    from formatted
    left join unit_converted
    on formatted.ts_code = unit_converted.ts_code
           and formatted.trade_date = unit_converted.trade_date
)

select * from final
