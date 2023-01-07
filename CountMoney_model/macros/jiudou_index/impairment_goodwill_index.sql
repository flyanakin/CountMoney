{% macro impairment_goodwill_index(goodwill, r_and_d, intan_assets, total_hldr_eqy_exc_min_int,
    oth_eqt_tools_p_shr, oth_eqt_tools) %}
    {# 计算商誉减值风险 #}
    (case
        when (COALESCE({{ total_hldr_eqy_exc_min_int }}, 0) - COALESCE({{ oth_eqt_tools_p_shr }}, 0) - COALESCE({{ oth_eqt_tools }}, 0)) =0 then null
        else
            round(
                (COALESCE({{ goodwill }}, 0) + COALESCE({{ r_and_d }}, 0) + COALESCE({{ intan_assets }}, 0)) /
                (COALESCE({{ total_hldr_eqy_exc_min_int }}, 0) - COALESCE({{ oth_eqt_tools_p_shr }}, 0) - COALESCE({{ oth_eqt_tools }}, 0)) ,
                3)
    end
    )
{% endmacro %}