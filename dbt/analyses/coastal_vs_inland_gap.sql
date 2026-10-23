-- Does the coast really breathe easier? Average daily PM2.5 and share of
-- days above "Good" for coastal vs inland locations, by month.
select
    date_trunc('month', date_key)::date                               as month,
    climate_zone,
    count(distinct location_id)                                       as locations,
    round(avg(pm25_mean), 1)                                          as avg_daily_pm25,
    round(avg(daily_pm25_aqi), 0)                                     as avg_daily_aqi,
    round(100.0 * avg(case when daily_pm25_aqi > 50 then 1 else 0 end), 1)
                                                                      as pct_days_above_good
from {{ ref('mart_daily_air_quality') }}
where is_complete_day
group by 1, 2
order by 1, 2
