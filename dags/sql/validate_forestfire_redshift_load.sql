-- SQL query to validate the upload of forestfires.csv
SELECT
  query,
  trim(filename) as filename,
  line_number,
  colname,
  raw_line,
  raw_field_value,
  err_code,
  err_reason
FROM stl_load_errors
WHERE filename LIKE '%{{ params.filename }}%'
ORDER BY query DESC
LIMIT 1;
