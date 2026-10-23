{{
    config(
        unique_key=['location_id', 'observed_at']
    )
}}

{#-
    Flatten the Open-Meteo air-quality payloads into one typed row per
    (location, hour). Same parallel-array technique as the weather model.
    All pollutant concentrations are ug/m3 (Open-Meteo reports CO in ug/m3 too).
-#}

with responses as (

    select
        response_id,
        location_id,
        ingested_at,
        payload
    from {{ source('open_meteo', 'api_responses') }}
    where source = 'open_meteo_air_quality'
    {% if is_incremental() %}
      and ingested_at > (select coalesce(max(ingested_at), '1970-01-01'::timestamptz) from {{ this }})
    {% endif %}

),

hourly as (

    select
        r.response_id,
        r.location_id,
        r.ingested_at,
        r.payload -> 'hourly' -> 'time'              ->> (t.idx - 1)            as observed_at_text,
        (r.payload -> 'hourly' -> 'pm2_5'            ->> (t.idx - 1))::numeric  as pm2_5,
        (r.payload -> 'hourly' -> 'pm10'             ->> (t.idx - 1))::numeric  as pm10,
        (r.payload -> 'hourly' -> 'ozone'            ->> (t.idx - 1))::numeric  as ozone,
        (r.payload -> 'hourly' -> 'nitrogen_dioxide' ->> (t.idx - 1))::numeric  as nitrogen_dioxide,
        (r.payload -> 'hourly' -> 'sulphur_dioxide'  ->> (t.idx - 1))::numeric  as sulphur_dioxide,
        (r.payload -> 'hourly' -> 'carbon_monoxide'  ->> (t.idx - 1))::numeric  as carbon_monoxide,
        (r.payload -> 'hourly' -> 'us_aqi'           ->> (t.idx - 1))::integer  as provider_us_aqi,
        (r.payload -> 'hourly' -> 'us_aqi_pm2_5'     ->> (t.idx - 1))::integer  as provider_us_aqi_pm2_5
    from responses as r
    cross join lateral generate_series(1, jsonb_array_length(r.payload -> 'hourly' -> 'time')) as t(idx)

),

typed as (

    select
        location_id,
        (observed_at_text || ':00+00')::timestamptz as observed_at,
        pm2_5,
        pm10,
        ozone,
        nitrogen_dioxide,
        sulphur_dioxide,
        carbon_monoxide,
        provider_us_aqi,
        provider_us_aqi_pm2_5,
        response_id,
        ingested_at
    from hourly

),

deduplicated as (

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
    pm2_5,
    pm10,
    ozone,
    nitrogen_dioxide,
    sulphur_dioxide,
    carbon_monoxide,
    provider_us_aqi,
    provider_us_aqi_pm2_5,
    response_id,
    ingested_at
from deduplicated
