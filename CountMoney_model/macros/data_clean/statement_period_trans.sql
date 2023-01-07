{% macro statement_period_trans(col) %}
    {# 财报季度为可读的字符串 #}
    case
        when {{ col }} = '1' then '一季度'
        when {{ col }} = '2' then '二季度'
        when {{ col }} = '3' then '三季度'
        when {{ col }} = '4' then '四季度'
    end as statement_period
{% endmacro %}