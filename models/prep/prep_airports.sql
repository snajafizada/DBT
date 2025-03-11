WITH airports_reorder AS (
    SELECT faa,
           name AS airport_name,
           city,
           country, -- Keep country here
           region,  -- Reorder: region comes after country
           lat AS latitude,
           lon AS longitude,
           alt AS elevation,
           tz AS timezone,
           dst AS destination
    FROM "lavender_notebooks"."s_saedanajafizada"."staging_airports"
)
SELECT * FROM airports_reorder
