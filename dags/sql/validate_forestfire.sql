-- SQL query to validate the upload of forestfires.csv
select query, trim(filename) as filename, curtime, status
from stl_load_commits
where filename like '%{{ params.filename }}%' order by query;
