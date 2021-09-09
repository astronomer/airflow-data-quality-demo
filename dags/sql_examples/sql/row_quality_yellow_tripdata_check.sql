-- Query to check row items
SELECT vendor_id,
  CASE WHEN dropoff_datetime > pickup_datetime THEN 1 ELSE 0 END AS date_check,
  CASE WHEN passenger_count > 0 THEN 1 ELSE 0 END AS passenger_count_check,
  CASE WHEN trip_distance > 0 AND trip_distance <= 100 THEN 1 ELSE 0 END AS trip_distance_check,
  CASE WHEN (fare_amount + extra, mta_tax + tip_amount + improvement_surcharge + COALESCE(congestion_surcharge, 0)) = total_amount THEN 1 ELSE 0 END AS fare_check
FROM {{ params.table }}
WHERE vendor_id = {{ params.vendor_id }}
  AND pickup_datetime = {{ params.pickup_datetime }}
