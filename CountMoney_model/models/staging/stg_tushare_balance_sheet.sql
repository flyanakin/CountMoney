{% set atom_metrics = ["accounts_receiv", "notes_receiv", "oth_receiv","lt_rec",
                       "total_cur_assets", "total_cur_liab","goodwill","r_and_d",
                       "intan_assets","total_hldr_eqy_exc_min_int","oth_eqt_tools_p_shr","oth_eqt_tools",
                       "lt_borr","bond_payable"
                       ] %}

with import as (
    --输入按created_time去重
    select * from (
        select
            *,
            row_number() over (
                partition by statement_id
                order by ann_date desc
                ) as rn
        from {{ source('tushare', 'tushare_balance_sheet') }}) as partitioned
    where partitioned.rn = 1
),

error_cleaned as (
    select * from import
),

formatted as (
    select
        statement_id,
        ts_code,
        {{ tushare_date_formatted('ann_date') }},
        {{ tushare_date_formatted('f_ann_date') }},
        {{ tushare_date_formatted('end_date') }},
        {{ company_type_trans('comp_type') }},
        {{ report_type_trans('report_type') }},
        {{ statement_period_trans('end_type') }},

        {% for atom_metric in atom_metrics %}
        round({{ atom_metric }}::numeric, 2) as {{ atom_metric }},
        {%- endfor %}

        update_flag,
        created_at
    from error_cleaned
),

final as (
    select
        statement_id,
        ts_code as stock_code,
        ann_date,
        f_ann_date,
        end_date,
        company_type,
        report_type,
        statement_period,

        {% for atom_metric in atom_metrics %}
        {{ atom_metric }},
        {%- endfor %}

        update_flag,
        created_at
    from formatted
)

select * from final
