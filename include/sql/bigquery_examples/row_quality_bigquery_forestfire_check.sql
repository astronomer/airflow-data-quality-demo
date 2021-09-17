-- Query to check if row items match particular parameters passed in by Operator.
SELECT ID,
  CASE y WHEN {{ params.y }} THEN 1 ELSE 0 END AS y_check,
  CASE month WHEN '{{ params.month }}' THEN 1 ELSE 0 END AS month_check,
  CASE day WHEN '{{ params.day }}' THEN 1 ELSE 0 END AS day_check,
  CASE ffmc WHEN {{ params.ffmc }} THEN 1 ELSE 0 END AS ffmc_check,
  CASE dmc WHEN {{ params.dmc }} THEN 1 ELSE 0 END AS dmc_check,
  CASE dc WHEN {{ params.dc }} THEN 1 ELSE 0 END AS dc_check,
  CASE isi WHEN {{ params.isi }} THEN 1 ELSE 0 END AS isi_check,
  CASE temp WHEN {{ params.temp }} THEN 1 ELSE 0 END AS temp_check,
  CASE rh WHEN {{ params.rh }} THEN 1 ELSE 0 END AS rh_check,
  CASE wind WHEN {{ params.wind }} THEN 1 ELSE 0 END AS wind_check,
  CASE rain WHEN {{ params.rain }} THEN 1 ELSE 0 END AS rain_check,
  CASE area WHEN {{ params.area }} THEN 1 ELSE 0 END AS area_check
FROM {{ params.dataset }}.{{ params.table }}
WHERE ID = {{ params.id }}
