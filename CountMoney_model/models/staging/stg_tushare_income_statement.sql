{% set atom_metrics = ["total_revenue", "n_income_attr_p"] %}

with import as (
    --输入按created_time去重
    select * from (
        select
            *,
            row_number() over (
                partition by statement_id
                order by ann_date desc
                ) as rn
        from {{ source('tushare', 'tushare_income_statement') }}) as partitioned
    where partitioned.rn = 1
),

error_cleaned as (
    select * from import
    where rn = 1
),

formatted as (
    select
     statement_id,
     ts_code,
     {{ tushare_date_formatted('ann_date') }},
     {{ tushare_date_formatted('end_date') }},
     {{ company_type_trans('comp_type') }},
     {{ report_type_trans('report_type') }},
     {{ statement_period_trans('end_type') }},

     {% for atom_metric in atom_metrics %}
     {{ atom_metric }},
     {%- endfor %}

     update_flag,
     created_at
    from error_cleaned
),

final as (
    select * from formatted
)

select * from final
