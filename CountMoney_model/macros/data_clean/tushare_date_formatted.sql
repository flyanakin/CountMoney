{% macro tushare_date_formatted(col) %}
    {# 统一日期格式转换，pg函数，输入列，返回日期date #}
    to_date({{ col }}, 'YYYYMMDD') as {{ col }}
{% endmacro %}