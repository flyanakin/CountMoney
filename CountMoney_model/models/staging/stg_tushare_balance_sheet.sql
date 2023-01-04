{% set atom_metrics = ["accounts_receiv", "notes_receiv", "oth_receiv","lt_rec",
                       "total_cur_assets", "total_cur_liab","goodwill","r_and_d",
                       "intan_assets","total_hldr_eqy_exc_min_int","oth_eqt_tools_p_shr","oth_eqt_tools",
                       "lt_borr","bond_payable"
                       ] %}

with import as (
    select * from {{ source('tushare', 'tushare_balance_sheet') }}
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
