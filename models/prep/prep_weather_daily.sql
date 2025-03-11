WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
		, date_part('day', date) AS date_day
		, date_part('month', date) AS date_month
		, date_part('year', date) AS date_year
		, date_part('week', date) AS cw
		, to_char(date, 'month') AS month_name
		, to_char(date, 'day') AS weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN month_name in ('December', 'January', 'February') THEN 'winter'
			WHEN month_name in ('March', 'April', 'May') THEN 'spring'
            WHEN month_name in ('June', 'July', 'August') THEN 'summer'
            WHEN month_name in ('September', 'October', 'November') THEN 'autumn'
		END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date;