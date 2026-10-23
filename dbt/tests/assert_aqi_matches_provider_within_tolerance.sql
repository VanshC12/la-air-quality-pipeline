{{ config(severity='warn') }}

-- Cross-check our EPA PM2.5 AQI macro against the provider's own hourly
-- PM2.5 AQI. Both apply breakpoint arithmetic to the hourly value, so they
-- should agree closely. A systematic gap is informative rather than fatal:
-- the most likely cause is a breakpoint-table difference (EPA revised the
-- PM2.5 breakpoints in May 2024; this project uses the revised table), which
-- is exactly the kind of upstream methodology drift this test exists to
-- surface. Warn-only: the provider number is informational and the marts
-- publish our own 24-hour EPA AQI, not this one.
select
    location_id,
    observed_at,
    pm2_5,
    pm25_aqi_hourly,
    provider_us_aqi_pm2_5
from {{ ref('fct_hourly_conditions') }}
where pm25_aqi_hourly is not null
  and provider_us_aqi_pm2_5 is not null
  and abs(pm25_aqi_hourly - provider_us_aqi_pm2_5) > 5
