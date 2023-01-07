{% macro receivables_index(accounts_receiv, notes_receiv, oth_receiv, lt_rec, total_revenue_ttm) %}
    {# 计算资不抵债风险 #}
    (case
        when {{ total_revenue_ttm }} = 0 then null
        when {{ total_revenue_ttm }} is null then null
        else
            round(
                (COALESCE({{ accounts_receiv }}, 0) + COALESCE({{ notes_receiv }}, 0) +
                 COALESCE({{ oth_receiv }}, 0) + COALESCE({{ lt_rec }}, 0)) /
                {{ total_revenue_ttm }},
                3)
    end
    )
{% endmacro %}