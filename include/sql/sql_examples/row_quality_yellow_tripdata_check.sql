-- Query to check row items
SELECT vendor_id, pickup_datetime,
  CASE WHEN dropoff_datetime > pickup_datetime THEN 1 ELSE 0 END AS date_check,
  CASE WHEN passenger_count >= 0 THEN 1 ELSE 0 END AS passenger_count_check,
  CASE WHEN trip_distance >= 0 AND trip_distance <= 100 THEN 1 ELSE 0 END AS trip_distance_check,
  CASE WHEN ROUND((fare_amount + extra + mta_tax + tip_amount + improvement_surcharge + COALESCE(congestion_surcharge, 0)), 1) = ROUND(total_amount, 1) THEN 1
       WHEN ROUND(fare_amount + extra + mta_tax + tip_amount + improvement_surcharge, 1) = ROUND(total_amount, 1) THEN 1 ELSE 0 END AS fare_check
FROM {{ var.json.aws_configs.redshift_table }}
WHERE pickup_datetime IN (SELECT pickup_datetime FROM {{ var.json.aws_configs.redshift_table }} ORDER BY RANDOM() LIMIT 1)
