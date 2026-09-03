{#
    dbt's default generate_schema_name concatenates <target_schema>_<custom_schema>
    (e.g. "ANALYTICS_marts"). Override it so a model's +schema config names the
    schema exactly, giving staging/intermediate/marts clean physical separation.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
