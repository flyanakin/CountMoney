with import as (
    select * from {{ source('tushare', 'tushare_income_statement') }}
),

error_cleaned as (
 select * from import
),

formatted as (
 select
     ts_code,
     {{ tushare_date_formatted('ann_date') }},
     {{ tushare_date_formatted('end_date') }},
     {{ company_type_trans('comp_type') }},
     {{ report_type_trans('report_type') }},

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
