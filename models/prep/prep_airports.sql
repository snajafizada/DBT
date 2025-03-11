WITH airports_reorder AS (
    SELECT faa AS 
           ,name AS airport_name
           ,city AS city
           ,country AS country -- Keep country here
           ,region AS region  -- Reorder: region comes after country
           ,lat AS latitude
           ,lon AS longitude
           ,alt AS elevation
           ,tz AS timezone
           ,dst AS destination
    FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder