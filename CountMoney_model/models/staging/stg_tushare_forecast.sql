{% set number_fields = ["p_change_min","p_change_max","net_profit_min","net_profit_max","last_parent_net"] %}

with import as (
    --输入按created_time去重
    select * from (
        select
            *,
            row_number() over (
                partition by ts_code,end_date
                order by created_at desc
                ) as rn
        from {{ source('tushare', 'tushare_forecast') }}) as partitioned
    where partitioned.rn = 1
),

formatted as (
    select
        ts_code,
        {{ tushare_date_formatted('ann_date') }},
        {{ tushare_date_formatted('end_date') }},
        type,
        {% for number_field in number_fields %}
        round({{ number_field }}::numeric, 2) as {{ number_field }},
        {%- endfor %}
        summary,
        change_reason,
        created_at
    from import
),

unit_coverted as (
    select
        ts_code,
        end_date,
        {{ percent_to_num('p_change_min') }},
        {{ percent_to_num('p_change_max') }},
        {{ money_unit_convert('net_profit_min', "'ten_thousand'") }},
        {{ money_unit_convert('net_profit_max', "'ten_thousand'") }},
        {{ money_unit_convert('last_parent_net', "'ten_thousand'") }}
    from formatted
),

final as (
    select
        formatted.ts_code               as stock_code,
        formatted.ann_date              as ann_date,
        formatted.end_date              as end_date,
        formatted.type                  as trend,
        unit_coverted.p_change_min      as net_income_change_min,
        unit_coverted.p_change_max      as net_income_change_max,
        unit_coverted.net_profit_min    as net_income_min,
        unit_coverted.net_profit_max    as net_income_max,
        unit_coverted.last_parent_net   as net_income_stockholder_last_year,
        formatted.summary               as summary,
        formatted.change_reason         as change_reason
    from formatted
    left join unit_coverted
    on formatted.ts_code = unit_coverted.ts_code and formatted.end_date = unit_coverted.end_date
)

select * from final