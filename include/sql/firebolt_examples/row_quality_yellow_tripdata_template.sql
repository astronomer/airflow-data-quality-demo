-- Template to check various columns in the yellow tripdata data set.
SELECT MIN({{ params.col }})
FROM(
  SELECT
    CASE WHEN {{ params.check_statement }} THEN 1 ELSE 0 END AS {{ params.col }}
  FROM {{ params.table }}
)
