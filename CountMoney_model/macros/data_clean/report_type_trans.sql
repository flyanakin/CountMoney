{% macro report_type_trans(col) %}
    {# 将报告类型转化为可读的信息 #}
    case
        when {{ col }} = '1' then '合并报表'
        when {{ col }} = '2' then '单季合并'
        when {{ col }} = '3' then '调整单季合并报表'
        when {{ col }} = '4' then '调整合并报表'
        when {{ col }} = '5' then '调整前合并报表'
        when {{ col }} = '6' then '母公司报表'
        when {{ col }} = '7' then '母公司单季报表'
        when {{ col }} = '8' then '母公司调整单季表'
        when {{ col }} = '9' then '母公司调整表'
        when {{ col }} = '10' then '母公司调整前报表'
        when {{ col }} = '11' then '母公司调整前合并报表'
        when {{ col }} = '12' then '母公司调整前报表'
    end as report_type
{% endmacro %}