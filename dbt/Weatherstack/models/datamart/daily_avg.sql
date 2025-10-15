{{config(
    materialized='table',
)}}

select 
    city,
    date(local_time) as date,
    round(avg(temperature)::numeric,2) as average_temperature,
    round(avg(wind_speed)::numeric,2) as average_wind_speed
from {{ ref('stg_weather_data') }}
group by city, date(local_time)
order by city, date(local_time)