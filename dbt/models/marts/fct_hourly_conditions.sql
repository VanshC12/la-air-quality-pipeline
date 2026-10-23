{#-
    Hourly fact table. Grain: one row per (location_id, observed_at).

    Adds the local-time keys the dashboard and daily mart need, plus an
    *instantaneous* PM2.5 AQI from the hourly concentration. The regulatory
    AQI uses a 24-hour mean and lives in mart_daily_air_quality; the hourly
    number here is for now-casting / trend lines only.
-#}

with hourly as (

    select * from {{ ref('int_hourly_conditions') }}

),

enriched as (

    select
        location_id,
        observed_at,
        {{ to_local('observed_at') }}                         as observed_at_local,
        ({{ to_local('observed_at') }})::date                 as date_key_local,
        extract(hour from {{ to_local('observed_at') }})::int as hour_local,

        temperature_c,
        relative_humidity_pct,
        precipitation_mm,
        wind_speed_kmh,
        wind_direction_deg,
        surface_pressure_hpa,

        pm2_5,
        pm10,
        ozone,
        nitrogen_dioxide,
        sulphur_dioxide,
        carbon_monoxide,

        {{ pm25_to_aqi('pm2_5') }}                            as pm25_aqi_hourly,
        provider_us_aqi,
        provider_us_aqi_pm2_5,

        weather_response_id,
        air_quality_response_id,
        ingested_at
    from hourly

)

select
    *,
    {{ aqi_category('pm25_aqi_hourly') }} as pm25_aqi_hourly_category
from enriched
