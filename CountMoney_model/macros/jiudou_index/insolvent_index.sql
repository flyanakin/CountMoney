{% macro insolvent_index(total_cur_assets, total_cur_liab, lt_borr, bond_payable) %}
    {# 计算资不抵债风险 #}
    (case
        when (COALESCE({{ lt_borr }}, 0) + COALESCE({{ bond_payable }}, 0)) =0 then null
        else
            round(
                (COALESCE({{ total_cur_assets }}, 0) - COALESCE({{ total_cur_liab }}, 0)) /
                (COALESCE({{ lt_borr }}, 0) + COALESCE({{ bond_payable }}, 0)),
                2)
    end
    )
{% endmacro %}