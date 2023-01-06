{% macro current_ratio(total_cur_assets, total_cur_liab) %}
    {# 计算资不抵债风险 #}
    (case
        when {{ total_cur_liab }} is null then null
        else
            round(
                COALESCE({{ total_cur_assets }}, 0) / {{ total_cur_liab }} ,
                2)
    end
    )
{% endmacro %}