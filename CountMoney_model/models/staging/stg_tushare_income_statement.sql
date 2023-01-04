{% set atom_metrics = ["total_revenue", "n_income_attr_p"] %}

with import as (
    select
        *,
        row_number() over (
            partition by ts_code
            order by ann_date desc
            )
    from {{ source('tushare', 'tushare_income_statement') }}
),

error_cleaned as (
 select * from import
),

formatted as (
 select
     report_id,
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
