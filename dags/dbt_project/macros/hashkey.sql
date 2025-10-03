{% macro generate_hash(columns) %}
md5(cast(concat({{ columns | join(",'|',") }}) as string))
{% endmacro %}
