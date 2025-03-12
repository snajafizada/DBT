with flight_route_stats as (
                SELECT
                    origin
                    ,dest
                    ,count (flight_number)
                    ,count (distinct tail_number) as unique_tails
                    ,count (distinct airline) as unique_airlines
                    ,avg(actual_elapsed_time_interval) as avg_actual_elapsed_
                    ,avg(arr_delay_interval) as avg_arr_delay
                    ,avg(dep_delay_interval) as avg_dep_delay
                    ,max(arr_delay_interval) as max_arr_delay
                    ,min(arr_delay_interval) as min_arr_delay
                    ,sum(cancelled) as total_cancelled
                    ,sum(diverted) as total_diverted
                from {{ref('prep_flights')}}
                group by (origin, dest)
)
select o.city as origin_city,
        d.city as dest_city,
        o.airport_name as origin_name,
        d.airport_name as dest_name,
        o.country as origin_country,
        d.country as dest_country,
        f.*
from flight_route_stats f
left join  {{ref('prep_airports')}} o
    on f.origin=o.faa
left join  {{ref('prep_airports')}} d
    on f.dest=d.faa