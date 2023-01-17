{% macro percent_to_num(col) %}
    {# 将百分数转化成小数 #}
    round({{ col }} / 100, 4) as {{col}}
{% endmacro %}