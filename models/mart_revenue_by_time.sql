SELECT
    time_of_day,
    COUNT(*) AS total_trips,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(SUM(fare_amount), 2) AS total_revenue,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_mins
FROM {{ ref('stg_nyc_taxi_trips') }}
GROUP BY time_of_day
ORDER BY total_revenue DESC