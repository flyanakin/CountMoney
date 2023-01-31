{% set number_fields = ["position", "cost"] %}

with portfolio as (
    select * from {{ source('airtable', 'airtable_portfolio') }}
    where created_at = (select max(created_at) from {{ source('airtable', 'airtable_portfolio') }})
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