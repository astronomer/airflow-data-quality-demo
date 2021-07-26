-- SQL query to validate the upload of forestfires.csv
SELECT CASE 0 WHEN COUNT(trim(filename)) THEN 1 ELSE 0 END as filename_check
FROM stl_load_errors
WHERE filename LIKE '%{{ params.filename }}%'
LIMIT 1;
