SELECT
    id,
    month,
    day,
    total_cost,
    area,
    total_cost / area as cost_per_area
FROM {{ params.table_name }}
