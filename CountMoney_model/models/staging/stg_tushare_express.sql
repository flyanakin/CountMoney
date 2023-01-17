{% set number_fields = ["revenue","operate_profit","total_profit","n_income","total_assets",
                        "total_hldr_eqy_exc_min_int","diluted_roe","yoy_net_profit"] %}

with import as (
    --输入按created_time去重
    select * from (
        select
            *,
            row_number() over (
                partition by ts_code,end_date
                order by created_at desc
                ) as rn
        from {{ source('tushare', 'tushare_express') }}) as partitioned
    where partitioned.rn = 1
),

formatted as (
    select
        ts_code,
        {{ tushare_date_formatted('ann_date') }},
        {{ tushare_date_formatted('end_date') }},

        {% for number_field in number_fields %}
        round({{ number_field }}::numeric, 2) as {{ number_field }},
        {%- endfor %}
        perf_summary,
        created_at
    from import
),

unit_coverted as (
    select
        ts_code,
        end_date,
        {{ percent_to_num('diluted_roe') }}
    from formatted
),

final as (
    select
        formatted.ts_code                       as stock_code,
        formatted.ann_date                      as ann_date,
        formatted.end_date                      as end_date,
        formatted.revenue                       as revenue,
        formatted.operate_profit                as operate_profit,
        formatted.n_income                      as net_income,
        formatted.total_assets                  as total_assets,
        formatted.total_hldr_eqy_exc_min_int    as total_hldr_eqy_exc_min_int,
        unit_coverted.diluted_roe               as diluted_roe,
        formatted.yoy_net_profit                as net_income_yoy,
        formatted.perf_summary                  as perf_summary
    from formatted
    left join unit_coverted
    on formatted.ts_code = unit_coverted.ts_code and formatted.end_date = unit_coverted.end_date
)

select * from final