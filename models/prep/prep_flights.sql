WITH flights_one_month AS (
    SELECT * 
    FROM {{ref('staging_flights_one_month')}}
),
flights_cleaned AS(
    SELECT 
        flight_date::DATE,
        TO_CHAR(dep_time, 'fm0000')::TIME AS dep_time, -- Format as TIME
        TO_CHAR(sched_dep_time, 'fm0000')::TIME AS sched_dep_time, -- Scheduled departure time
        dep_delay,
        (dep_delay * '1 minute'::INTERVAL) AS dep_delay_interval, -- Convert dep_delay to INTERVAL
        TO_CHAR(arr_time, 'fm0000')::TIME AS arr_time, -- Arrival time
        TO_CHAR(sched_arr_time, 'fm0000')::TIME AS sched_arr_time, -- Scheduled arrival time
        arr_delay,
        (arr_delay * '1 minute'::INTERVAL) AS arr_delay_interval, -- Convert arr_delay to INTERVAL
        airline,
        tail_number,
        flight_number,
        origin,
        dest,
        air_time,
        (air_time * '1 minute'::INTERVAL) AS air_time_interval, -- Convert air_time to INTERVAL
        actual_elapsed_time,
        (actual_elapsed_time * '1 minute'::INTERVAL) AS actual_elapsed_time_interval, -- Convert actual_elapsed_time to INTERVAL
        (distance * 0.621371)::NUMERIC(6,2) AS distance_km, -- Convert distance to kilometers
        cancelled,
        diverted
    FROM flights_one_month
)
SELECT * FROM flights_cleaned
