CREATE TABLE IF NOT EXISTS {{ conn.snowflake_default.schema }}.{{ params.table_name }}
(
    vendor_id int,
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    passenger_count int,
    trip_distance float,
    rate_code_id int,
    store_and_fwd_flag varchar,
    pickup_location_id int,
    dropoff_location_id int,
    payment_type int,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    improvement_surcharge float,
    total_amount float,
    congestion_surcharge float
);
