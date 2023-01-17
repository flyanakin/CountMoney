{% set agg_metrics = ["net_income","net_income_exclude_minority","total_revenue"] %}

with
import as (
    select * from {{ ref('stg_tushare_income_statement') }}
),

deduplicated as (
    --根据发布日期和更新标志去重
    select * from (
        select
            *,
            row_number() over (
                partition by stock_code,end_date
                order by f_ann_date desc, update_flag desc
                ) as rn1
        from import) as t
    where t.rn1 = 1
),

quarter_ordered as (
    select
        *,
        --增加中间字段处理
        substring((end_date::text) from 1 for 4) as __year,
        row_number() over (
            partition by stock_code
            order by end_date desc
            ) as order_num
    from deduplicated
),

q1_metrics as (
    select
        stock_code,
        __year,
        '一季报' as statement_type,
        {% for agg_metric in agg_metrics %}
        sum({{ agg_metric }}) as {{ agg_metric }},
        {%- endfor %}
        1     as cnt
    from quarter_ordered
    where statement_period in ('一季度')
    group by stock_code,__year
    having count(stock_code) = 1
),

h1_metrics as (
    select
        stock_code,
        __year,
        '中报' as statement_type,
        {% for agg_metric in agg_metrics %}
        sum({{ agg_metric }}) as {{ agg_metric }},
        {%- endfor %}
        count(stock_code) as cnt
    from quarter_ordered
    where statement_period in ('一季度','二季度')
    group by stock_code,__year
    having count(stock_code) = 2
),

q3_metrics as (
    select
        stock_code,
        __year,
        '三季报' as statement_type,
        {% for agg_metric in agg_metrics %}
        sum({{ agg_metric }}) as {{ agg_metric }},
        {%- endfor %}
        count(stock_code) as cnt
    from quarter_ordered
    where statement_period in ('一季度','二季度','三季度')
    group by stock_code,__year
    having count(stock_code) = 3
),

h2_metrics as (
    select
        stock_code,
        __year,
        '年报' as statement_type,
        {% for agg_metric in agg_metrics %}
        sum({{ agg_metric }}) as {{ agg_metric }},
        {%- endfor %}
        count(stock_code) as cnt
    from quarter_ordered
    where statement_period in ('一季度','二季度','三季度','四季度')
    group by stock_code,__year
    having count(stock_code) = 4 --过滤缺少财报的情况
),

metrics_unioned as (
    select * from q1_metrics
    union all
    select * from h1_metrics
    union all
    select * from q3_metrics
    union all
    select * from h2_metrics
),

last_5_quarter as (
    --只取过去5份财报
    select * from (
        select
            *,
            row_number() over (
                partition by stock_code
                order by __year desc, cnt desc
                ) as rn2
        from metrics_unioned) as t
    where t.rn2 in (1,2,3,4,5)
),

final as (
    select
        stock_code,
        __year as year,
        statement_type,
        net_income,
        net_income_exclude_minority,
        total_revenue,
        cnt as statement_period_num
    from last_5_quarter
)

select * from final