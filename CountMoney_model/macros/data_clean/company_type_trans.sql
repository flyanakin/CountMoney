{% macro company_type_trans(col) %}
    {# 公司类型转换为可读的字符串 #}
    case
        when {{ col }} = '1' then '一般工商业'
        when {{ col }} = '2' then '银行'
        when {{ col }} = '3' then '保险'
        when {{ col }} = '4' then '证券'
    end as company_type
{% endmacro %}