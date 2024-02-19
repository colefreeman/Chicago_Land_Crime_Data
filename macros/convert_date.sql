{% macro to_timestamp(date_column_name) -%}

    TO_TIMESTAMP_NTZ("{{date_column_name}}" / 1000000)

{%- endmacro %}