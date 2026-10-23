{#-
    Conform the two hourly feeds onto one grain: (location_id, observed_at).

    FULL OUTER JOIN so an hour present in only one feed still appears
    (the other feed's columns are null). fct_hourly_conditions materialises
    this; here it stays a view so the join logic lives in one place.
-#}

with weather as (

    select * from {{ ref('stg_open_meteo__weather_hourly') }}

),

air_quality as (

    select * from {{ ref('stg_open_meteo__air_quality_hourly') }}

)

select
    coalesce(w.location_id, a.location_id)      as location_id,
    coalesce(w.observed_at, a.observed_at)      as observed_at,

    -- weather
    w.temperature_c,
    w.relative_humidity_pct,
    w.precipitation_mm,
    w.wind_speed_kmh,
    w.wind_direction_deg,
    w.surface_pressure_hpa,

    -- air quality
    a.pm2_5,
    a.pm10,
    a.ozone,
    a.nitrogen_dioxide,
    a.sulphur_dioxide,
    a.carbon_monoxide,
    a.provider_us_aqi,
    a.provider_us_aqi_pm2_5,

    -- lineage
    w.response_id                                as weather_response_id,
    a.response_id                                as air_quality_response_id,
    greatest(w.ingested_at, a.ingested_at)       as ingested_at

from weather as w
full outer join air_quality as a
    on  w.location_id = a.location_id
    and w.observed_at = a.observed_at
