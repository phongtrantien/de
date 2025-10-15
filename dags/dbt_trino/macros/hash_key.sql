{% macro hash_key() -%}
    md5(
        to_utf8(
            CONCAT_WS(
                '|',
                {%- for c in varargs -%}
                    COALESCE(CAST({{ c }} AS VARCHAR), '')
                    {%- if not loop.last %}, {% endif -%}
                {%- endfor -%}
            )
        )
    )
{%- endmacro %}

