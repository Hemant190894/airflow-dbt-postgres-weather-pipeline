{{ config(materialized='table') }}

with weather_data as (
    select 
        location_name,
        case when location_name = 'Delhi' and location_country <> 'India'
             then 'India' else location_country end as location_country,
        case when location_name = 'Delhi' and location_country <> 'India'
             then 'New Delhi' else location_region end as location_region,
        round(avg(temperature_c)::numeric, 2) as avg_temperature_c,
        round(avg(humidity_pct)::numeric, 2) as avg_humidity_pct,
        round(avg(wind_speed_kmh)::numeric, 2) as avg_wind_speed_kmh,
        round(avg(pressure_mb)::numeric, 2) as avg_pressure_mb,
        round(avg(precip_mm)::numeric, 2) as avg_precip_mm,
        round(avg(visibility_km)::numeric, 2) as avg_visibility_km,
        round(avg(uv_index)::numeric, 2) as avg_uv_index,
        round(avg(air_quality_co)::numeric, 2) as avg_air_quality_co,
        round(avg(air_quality_no2)::numeric, 2) as avg_air_quality_no2,
        round(avg(air_quality_o3)::numeric, 2) as avg_air_quality_o3,
        round(avg(air_quality_so2)::numeric, 2) as avg_air_quality_so2,
        round(avg(air_quality_pm2_5)::numeric, 2) as avg_air_quality_pm2_5,
        round(avg(air_quality_pm10)::numeric, 2) as avg_air_quality_pm10,
        round(avg(air_quality_us_epa_index)::numeric, 2) as avg_air_quality_us_epa_index,
        round(avg(air_quality_gb_defra_index)::numeric, 2) as avg_air_quality_gb_defra_index,
        max(observation_timestamp) as last_observed_at,
        count(*) as record_count
    from {{ ref('stg_weather_data') }}
    group by 
        location_name,
        case when location_name = 'Delhi' and location_country <> 'India'
             then 'India' else location_country end,
        case when location_name = 'Delhi' and location_country <> 'India'
             then 'New Delhi' else location_region end
)  
select * from weather_data
