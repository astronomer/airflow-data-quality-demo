-- Load store_failures dbt data from the default, overwritten table to a permanent table

INSERT INTO dbt_dataset.permanent_store_failures (col)
SELECT col
FROM dbt_dataset.store_failures
