{% macro money_unit_convert(col, origin_unit) %}
    {# 万元转元、亿元转元 #}
    case
        when {{ origin_unit }} = 'ten_thousand' then round({{ col }} * 10000)
        when {{ origin_unit }} = 'one_hundred_thousand' then round({{ col }} * 100000000)
    end as {{ col }}
{% endmacro %}