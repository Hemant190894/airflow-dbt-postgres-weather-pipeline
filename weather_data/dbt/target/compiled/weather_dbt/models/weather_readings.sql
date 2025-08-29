

-- Simple model selecting key fields from the raw ingest
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
  created_at
from "weather_data"."weather_api"."weather_api_raw_data"