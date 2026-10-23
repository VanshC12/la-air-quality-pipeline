{{
    config(
        unique_key=['location_id', 'observed_at']
    )
}}

{#-
    Flatten the Open-Meteo weather payloads into one typed row per
    (location, hour).

    Open-Meteo returns "parallel arrays": hourly.time[i] lines up with
    hourly.temperature_2m[i], etc. We generate an index per array position
    with generate_series and pick the i-th element of every array
    (jsonb ->> int is 0-based).

    Incremental: only responses ingested after the high-watermark are
    re-flattened; delete+insert on (location_id, observed_at) replaces any
    hour that a re-run or backfill re-fetched.
-#}

with responses as (

    select
        response_id,
        location_id,
        ingested_at,
        payload
    from {{ source('open_meteo', 'api_responses') }}
    where source = 'open_meteo_weather'
    {% if is_incremental() %}
      and ingested_at > (select coalesce(max(ingested_at), '1970-01-01'::timestamptz) from {{ this }})
    {% endif %}

),

hourly as (

    select
        r.response_id,
        r.location_id,
        r.ingested_at,
        r.payload -> 'hourly' -> 'time'                 ->> (t.idx - 1)             as observed_at_text,
        (r.payload -> 'hourly' -> 'temperature_2m'       ->> (t.idx - 1))::numeric   as temperature_c,
        (r.payload -> 'hourly' -> 'relative_humidity_2m' ->> (t.idx - 1))::numeric   as relative_humidity_pct,
        (r.payload -> 'hourly' -> 'precipitation'        ->> (t.idx - 1))::numeric   as precipitation_mm,
        (r.payload -> 'hourly' -> 'wind_speed_10m'       ->> (t.idx - 1))::numeric   as wind_speed_kmh,
        (r.payload -> 'hourly' -> 'wind_direction_10m'   ->> (t.idx - 1))::numeric   as wind_direction_deg,
        (r.payload -> 'hourly' -> 'surface_pressure'     ->> (t.idx - 1))::numeric   as surface_pressure_hpa
    from responses as r
    cross join lateral generate_series(1, jsonb_array_length(r.payload -> 'hourly' -> 'time')) as t(idx)

),

typed as (

    select
        location_id,
        -- API timestamps are requested in UTC and arrive as 'YYYY-MM-DDTHH:MM' with no offset.
        (observed_at_text || ':00+00')::timestamptz as observed_at,
        temperature_c,
        relative_humidity_pct,
        precipitation_mm,
        wind_speed_kmh,
        wind_direction_deg,
        surface_pressure_hpa,
        response_id,
        ingested_at
    from hourly

),

deduplicated as (

    -- A re-fetched day may overlap a previous one; keep the most recent fetch.
    select *
    from (
        select
            *,
            row_number() over (
                partition by location_id, observed_at
                order by ingested_at desc, response_id desc
            ) as rn
        from typed
    ) as ranked
    where rn = 1

)

select
    location_id,
    observed_at,
    temperature_c,
    relative_humidity_pct,
    precipitation_mm,
    wind_speed_kmh,
    wind_direction_deg,
    surface_pressure_hpa,
    response_id,
    ingested_at
from deduplicated
