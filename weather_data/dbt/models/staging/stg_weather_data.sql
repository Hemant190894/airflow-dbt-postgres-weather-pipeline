{{ config(materialized='table') }}

with weather_data as (
select 
location_name,
location_country,
location_region,
latitude,
longitude,
timezone_id,
observation_timestamp,
current_temperature_celsius as temperature_c,
weather_description,
humidity_percentage as humidity_pct,
wind_speed_kmh as wind_speed_kmh,
pressure_mb as pressure_mb,
precipitation_mm as precip_mm,
visibility_km as visibility_km,
uv_index,
air_quality_co,
air_quality_no2,
air_quality_o3,
air_quality_so2,
air_quality_pm2_5,
air_quality_pm10,
air_quality_us_epa_index,
air_quality_gb_defra_index,
CAST(created_at as Date) as created_at,
ROW_NUMBER() OVER (PARTITION BY location_name, observation_timestamp ORDER BY observation_timestamp desc) as row_num
from {{ source('weather_raw', 'weather_api_raw_data') }}
)
select 
    location_name,
    location_country,
    location_region,
    latitude,
    longitude,
    timezone_id,
    observation_timestamp,
    temperature_c,
    weather_description,
    humidity_pct,
    wind_speed_kmh,
    pressure_mb,
    precip_mm,
    visibility_km,
    uv_index,
    air_quality_co,
    air_quality_no2,
    air_quality_o3,
    air_quality_so2,
    air_quality_pm2_5,
    air_quality_pm10,
    air_quality_us_epa_index,
    air_quality_gb_defra_index,
    created_at
from weather_data 
where row_num = 1
