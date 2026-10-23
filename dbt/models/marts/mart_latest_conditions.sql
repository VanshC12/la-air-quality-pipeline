{#-
    Most recent observed hour per location - the "right now" panel of the
    dashboard. Grain: one row per location.
-#}

with ranked as (

    select
        *,
        row_number() over (partition by location_id order by observed_at desc) as rn
    from {{ ref('fct_hourly_conditions') }}
    where pm2_5 is not null

)

select
    r.location_id,
    l.location_name,
    l.region,
    l.climate_zone,
    l.latitude,
    l.longitude,
    r.observed_at,
    r.observed_at_local,
    r.pm2_5,
    r.pm10,
    r.ozone,
    r.pm25_aqi_hourly,
    r.pm25_aqi_hourly_category,
    r.provider_us_aqi,
    r.temperature_c,
    r.relative_humidity_pct,
    r.wind_speed_kmh,
    r.wind_direction_deg,
    now() - r.observed_at as observation_age
from ranked as r
join {{ ref('dim_location') }} as l
    on r.location_id = l.location_id
where r.rn = 1
