WITH airports_reorder AS (
    SELECT faa
           ,airport_name
           ,city
           ,country
           ,region  -- Reorder: region comes after country
           ,latitude
           ,longitude
           ,elevation
           ,timezone
           ,iso_country
           ,iso_region
    FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder
