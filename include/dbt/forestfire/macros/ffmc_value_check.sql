{% test ffmc_value_check(model, column_name) %}

SELECT {{ column_name }}
FROM `simple_bigquery_example_dag.{{ model }}`
HAVING NOT({{ column_name }} >= 90)

{% endtest %}
