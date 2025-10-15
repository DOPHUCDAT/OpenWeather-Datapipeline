{{config(
    materialized='table',
    unique_key='id',
)}}

with source as (
    select *
    from {{ source('weather', 'raw_weather_data') }}
),

deduplicate as (
    select
        *,
        row_number() over (partition by time order by inserted_at) as rn
    from source
)

select
    id,
    city,
    temperature,
    weather_descriptions,
    wind_speed,
    time as local_time,
    (inserted_at + (utc_offset || 'hours')::interval) as inserted_time
from deduplicate
where rn = 1