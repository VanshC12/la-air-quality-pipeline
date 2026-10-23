{#-
    Calendar dimension generated in-warehouse (no external package needed).
    Grain: one row per calendar day in the configured range.
-#}

with spine as (

    select generate_series(
        '{{ var("dim_date_start") }}'::date,
        '{{ var("dim_date_end") }}'::date,
        interval '1 day'
    )::date as date_key

)

select
    date_key,
    extract(year    from date_key)::integer          as year,
    extract(quarter from date_key)::integer          as quarter,
    extract(month   from date_key)::integer          as month,
    to_char(date_key, 'Month')                       as month_name,
    extract(day     from date_key)::integer          as day_of_month,
    extract(isodow  from date_key)::integer          as day_of_week,     -- 1 = Monday
    to_char(date_key, 'Day')                         as day_name,
    extract(week    from date_key)::integer          as iso_week,
    extract(doy     from date_key)::integer          as day_of_year,
    extract(isodow  from date_key) in (6, 7)         as is_weekend,
    case
        when extract(month from date_key) in (12, 1, 2) then 'Winter'
        when extract(month from date_key) in (3, 4, 5)  then 'Spring'
        when extract(month from date_key) in (6, 7, 8)  then 'Summer'
        else 'Fall'
    end                                              as season,
    -- Southern California wildfire season, used to annotate PM2.5 spikes.
    extract(month from date_key) between 6 and 11    as is_wildfire_season
from spine
