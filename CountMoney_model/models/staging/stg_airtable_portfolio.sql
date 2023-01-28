{% set number_fields = ["position", "cost"] %}

with import as (
    select * from {{ source('airtable', 'airtable_portfolio') }}
),

portfolio as (
    select * from (
        select
            *,
            row_number() over (
                partition by code
                order by created_at desc
                ) as rn
        from import) as t
    where t.rn = 1
),

formatted as (
    select
        code,
        asset_name,
        asset_type,
        {% for number_field in number_fields %}
        round({{ number_field }}::numeric, 2) as {{ number_field }},
        {%- endfor %}
        sub_portfolio,
        order_date,
        created_at
    from portfolio
),

final as (
    select * from  formatted
)

select * from final