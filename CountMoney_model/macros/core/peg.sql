{% macro peg(pe, growth, retain) %}
    {# 计算peg，pe/(增长率*100) retain为保留多少位小数#}
    (case
        when {{ growth }} is null then null
        when {{ growth }} = 0 then null
        when {{ pe }} is null then null
        else
            round(
                {{ pe }} / ({{ growth }} * 100),
                {{ retain }})
    end
    )
{% endmacro %}