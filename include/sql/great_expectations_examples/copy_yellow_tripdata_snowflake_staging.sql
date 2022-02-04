{% set table_schema = params.table_schema %}
MERGE INTO {{ conn.snowflake_default.schema }}.{{ params.table_name }} as dest
USING (
    SELECT *
    FROM
    {{ conn.snowflake_default.schema }}.{{ params.audit_table_name }}
) as stg
ON dest.PICKUP_DATETIME = stg.PICKUP_DATETIME
  AND dest.DROPOFF_DATETIME = stg.DROPOFF_DATETIME
WHEN NOT MATCHED THEN
INSERT (
    {%- for name, col_dict in table_schema.items() -%}
    {%- if loop.first %}
    {{ name }}
    {%- else %}
    ,{{ name }}
    {%- endif %}
    {%- endfor %}
)
VALUES
(
    {% for name, col_dict in table_schema.items() %}
    {%- if not loop.first %}
    ,{%- endif -%}
    {%- if 'default' in col_dict.keys() -%}
        COALESCE(stg.{{ name }}, '{{col_dict.get('default', 'missing_value')}}')
    {%- else -%}
        stg.{{ name }}
    {%- endif -%}
    {%- endfor %}
)
