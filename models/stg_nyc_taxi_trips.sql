-- Staging model: clean view of raw NYC Taxi data

SELECT
    VENDORID                AS vendor_id,
    TPEP_PICKUP_DATETIME    AS pickup_datetime,
    TPEP_DROPOFF_DATETIME   AS dropoff_datetime,
    PASSENGER_COUNT         AS passenger_count,
    TRIP_DISTANCE           AS trip_distance,
    FARE_AMOUNT             AS fare_amount,
    TIP_AMOUNT              AS tip_amount,
    TOTAL_AMOUNT            AS total_amount,
    PAYMENT_TYPE            AS payment_type,
    TIME_OF_DAY             AS time_of_day,
    TRIP_DURATION_MINUTES   AS trip_duration_minutes,
    FARE_PER_MILE           AS fare_per_mile,
    PICKUP_HOUR             AS pickup_hour,
    PICKUP_DATE             AS pickup_date

FROM {{ source('nyc_taxi', 'NYC_TAXI_TRIPS') }}

WHERE fare_amount > 0
  AND trip_distance > 0
  AND passenger_count >= 1