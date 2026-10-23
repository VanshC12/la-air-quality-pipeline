-- The daily DAG only ever fetches completed UTC days, so nothing in the fact
-- table should sit in the future. A manual run with end_date = today is
-- tolerated (Open-Meteo returns the forecast for the rest of that day), hence
-- the one-day grace period.
select
    location_id,
    observed_at
from {{ ref('fct_hourly_conditions') }}
where observed_at > (current_date + interval '1 day')
