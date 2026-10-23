{{ config(severity='warn') }}

-- Every *interior* local day should have a full set of PM2.5 hours. The
-- first and last loaded days are excluded: a local (America/Los_Angeles) day
-- straddles two UTC days, so the edges of any loaded range are naturally
-- partial. Gaps in the middle mean a run was skipped - the cue to backfill.
with bounds as (

    select
        min(date_key) as first_day,
        max(date_key) as last_day
    from {{ ref('mart_daily_air_quality') }}

)

select
    m.location_id,
    m.date_key,
    m.hours_with_pm25
from {{ ref('mart_daily_air_quality') }} as m
cross join bounds as b
where m.date_key > b.first_day
  and m.date_key < b.last_day
  and m.hours_with_pm25 < 18
