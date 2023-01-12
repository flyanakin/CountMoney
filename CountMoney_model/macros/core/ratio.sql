{% macro ratio(current_period, origin_period, retain) %}
    {# 计算比率，(期末值-期初值)/期初值 retain为保留多少位小数#}
    (case
        when {{ origin_period }} is null then null
        else
            round(
                (COALESCE({{ current_period }}, 0) - {{ origin_period }}) /
                {{ origin_period }},
                {{ retain }})
    end
    )
{% endmacro %}