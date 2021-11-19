{% test ffmc_value_check(model, column_name) %}

SELECT {{ column_name }}
FROM `astronomer-cloud-dev-236021.simple_bigquery_example_dag.{{ model }}`
HAVING NOT({{ column_name }} >= 90)

{% endtest %}
