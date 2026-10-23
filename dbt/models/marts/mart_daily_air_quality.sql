{#-
    Daily air-quality mart. Grain: one row per (location_id, date_key_local),
    where the day is the *local* (America/Los_Angeles) calendar day.

    The daily PM2.5 AQI follows the EPA method: AQI of the 24-hour mean
    concentration, reported only when at least 18 of 24 hourly values are
    present (EPA's 75% completeness rule). Incomplete days keep their
    statistics but carry a null AQI and a completeness flag so dashboards
    can show the gap instead of a misleading number.
-#}

with hourly as (

    select * from {{ ref('fct_hourly_conditions') }}

),

locations as (

    select * from {{ ref('dim_location') }}

),

daily as (

    select
        location_id,
        date_key_local                                        as date_key,

        count(*)                                              as hours_in_day,
        count(pm2_5)                                          as hours_with_pm25,

        round(avg(pm2_5), 1)                                  as pm25_mean,
        max(pm2_5)                                            as pm25_max,
        round(percentile_cont(0.95) within group (order by pm2_5)::numeric, 1)
                                                              as pm25_p95,
        round(avg(pm10), 1)                                   as pm10_mean,
        max(ozone)                                            as ozone_max,
        round(avg(nitrogen_dioxide), 1)                       as no2_mean,
        max(provider_us_aqi)                                  as provider_us_aqi_max,

        round(avg(temperature_c), 1)                          as temperature_mean_c,
        max(temperature_c)                                    as temperature_max_c,
        min(temperature_c)                                    as temperature_min_c,
        round(sum(precipitation_mm), 1)                       as precipitation_total_mm,
        max(wind_speed_kmh)                                   as wind_speed_max_kmh,
        round(avg(relative_humidity_pct), 0)                  as humidity_mean_pct
    from hourly
    group by location_id, date_key_local

),

with_aqi as (

    select
        *,
        hours_with_pm25 >= 18                                 as is_complete_day,
        case
            when hours_with_pm25 >= 18 then {{ pm25_to_aqi('pm25_mean') }}
            else null
        end                                                   as daily_pm25_aqi
    from daily

),

with_rolling as (

    select
        *,
        {{ aqi_category('daily_pm25_aqi') }}                  as daily_pm25_aqi_category,
        daily_pm25_aqi > 100                                  as is_unhealthy_for_sensitive_groups,
        round(avg(pm25_mean) over (
            partition by location_id
            order by date_key
            rows between 6 preceding and current row
        ), 1)                                                 as pm25_mean_7d
    from with_aqi

)

select
    r.location_id,
    r.date_key,
    l.location_name,
    l.region,
    l.climate_zone,
    r.hours_in_day,
    r.hours_with_pm25,
    r.is_complete_day,
    r.pm25_mean,
    r.pm25_max,
    r.pm25_p95,
    r.pm25_mean_7d,
    r.daily_pm25_aqi,
    r.daily_pm25_aqi_category,
    r.is_unhealthy_for_sensitive_groups,
    r.provider_us_aqi_max,
    r.pm10_mean,
    r.ozone_max,
    r.no2_mean,
    r.temperature_mean_c,
    r.temperature_max_c,
    r.temperature_min_c,
    r.precipitation_total_mm,
    r.wind_speed_max_kmh,
    r.humidity_mean_pct
from with_rolling as r
left join locations as l
    on r.location_id = l.location_id
