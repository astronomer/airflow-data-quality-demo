-- Load store_failures dbt data from the default, overwritten table to a permanent table
INSERT INTO {{ params.destination_table }} ({{ params.columns }})
SELECT {{ params.columns }}
FROM {{ params.source_table }};
