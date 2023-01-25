{% set atom_metrics = ["extra_item"] %}

with import as (
    --输入按created_time去重
    select * from (
        select
            *,
            row_number() over (
                partition by ts_code,end_date,update_flag
                order by created_at desc
                ) as rn_created,
            row_number() over (
                partition by ts_code,end_date
                order by update_flag desc
                ) as rn_update
        from {{ source('tushare', 'tushare_fina_indicator') }}) as partitioned
    where partitioned.rn_created = 1 and partitioned.rn_update = 1
),

formatted as (
    select
     ts_code,
     {{ tushare_date_formatted('ann_date') }},
     {{ tushare_date_formatted('end_date') }},

     {% for atom_metric in atom_metrics %}
     round({{ atom_metric }}::numeric, 2) as {{ atom_metric }},
     {%- endfor %}

     update_flag,
     created_at
    from import
),

final as (
    select
        ts_code as stock_code,
        ann_date,
        end_date,
        extra_item as extraordinary_items,
        update_flag,
        created_at
    from formatted
)

select * from final
