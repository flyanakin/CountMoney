{% set metrics = ["extraordinary_items","net_income_exclude_extra_item"] %}

with
import as (
    select * from {{ ref('stg_tushare_finance_indicator') }}
),

quarter_ordered as (
    select
        *,
        --增加中间字段处理
        substring((end_date::text) from 1 for 4) as __year,
        (substring((end_date::text) from 6 for 2))::numeric / 3 as __quarter
    from import
),

q1 as (
    select * from quarter_ordered
    where __quarter = 1
),

h1 as (
    select * from quarter_ordered
    where __quarter = 2
),

q3 as (
    select * from quarter_ordered
    where __quarter = 3
),

h2 as (
    select * from quarter_ordered
    where __quarter = 4
),

q1_quarterly as (
    select
        stock_code,
        end_date,
        {% for metric in metrics %}
        {{ metric }}
        {% if not loop.last %},{% endif %}
        {%- endfor %}
    from q1
),

q2_quarterly as (
    select
        h1.stock_code,
        h1.end_date,
        {% for metric in metrics %}
        (h1.{{ metric }} - q1.{{ metric }}) as {{ metric }}
        {% if not loop.last %},{% endif %}
        {%- endfor %}
    from h1
    left join q1
        on h1.stock_code = q1.stock_code
               and h1.__year = q1.__year
               and (h1.__quarter-1) = q1.__quarter
),

q3_quarterly as (
    select
        q3.stock_code,
        q3.end_date,
        {% for metric in metrics %}
        (q3.{{ metric }} - h1.{{ metric }}) as {{ metric }}
        {% if not loop.last %},{% endif %}
        {%- endfor %}
    from q3
        left join h1
            on q3.stock_code = h1.stock_code
                and q3.__year = h1.__year
                and (q3.__quarter-1) = h1.__quarter
),

q4_quarterly as (
    select
        h2.stock_code,
        h2.end_date,
        {% for metric in metrics %}
        (h2.{{ metric }} - q3.{{ metric }}) as {{ metric }}
        {% if not loop.last %},{% endif %}
        {%- endfor %}
    from h2
        left join q3
    on h2.stock_code = q3.stock_code
        and h2.__year = q3.__year
        and (h2.__quarter-1) = q3.__quarter
),

final as (
    select * from q1_quarterly
    union all
    select * from q2_quarterly
    union all
    select * from q3_quarterly
    union all
    select * from q4_quarterly
)

select * from final
