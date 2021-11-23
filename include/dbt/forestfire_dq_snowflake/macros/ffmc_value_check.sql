{% test ffmc_value_check(model, column_name) %}

SELECT {{ column_name }}
FROM {{ model }}
HAVING NOT({{ column_name }} < 90)

{% endtest %}
