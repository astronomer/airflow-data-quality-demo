-- Template to check various columns in the yellow tripdata data set.
SELECT MIN({{ params.check_name }})
FROM(
  SELECT
    CASE WHEN {{ params.check_statement }} THEN 1 ELSE 0 END AS {{ params.check_name }}
  FROM {{ params.table }}
)
