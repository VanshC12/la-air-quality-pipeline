-- Top 10 worst days (by daily PM2.5 AQI) across all locations, with the
-- weather that accompanied them. `dbt compile` renders this to target/ for
-- pasting into a SQL client; analyses are never materialised.
select
    date_key,
    location_name,
    region,
    daily_pm25_aqi,
    daily_pm25_aqi_category,
    pm25_mean,
    pm25_max,
    temperature_max_c,
    wind_speed_max_kmh,
    humidity_mean_pct
from {{ ref('mart_daily_air_quality') }}
where is_complete_day
order by daily_pm25_aqi desc nulls last, pm25_mean desc
limit 10
