{#
  US EPA Air Quality Index for PM2.5 (24-hour), using the breakpoints that
  took effect 6 May 2024 (EPA lowered the annual standard and revised the
  "Good"/"Moderate" boundaries).

      PM2.5 (ug/m3)       AQI        Category
      0.0   -   9.0       0 -  50    Good
      9.1   -  35.4      51 - 100    Moderate
     35.5   -  55.4     101 - 150    Unhealthy for Sensitive Groups
     55.5   - 125.4     151 - 200    Unhealthy
    125.5   - 225.4     201 - 300    Very Unhealthy
    225.5   - 325.4     301 - 500    Hazardous
                > 325.4       500    (capped)

  Formula:  I = (I_hi - I_lo) / (C_hi - C_lo) * (C - C_lo) + I_lo
  where C is the concentration truncated (not rounded) to one decimal.

  The macro takes a SQL expression so it can be applied to a daily mean
  (the official EPA method) or to an hourly value (an "instantaneous" AQI,
  useful for now-casting but not the regulatory number).
#}
{% macro pm25_to_aqi(pm25_expr) -%}
    (
        case
            when ({{ pm25_expr }}) is null then null
            when ({{ pm25_expr }}) < 0 then null
            {%- set bp = [
                (0.0,   9.0,   0,  50),
                (9.1,   35.4,  51, 100),
                (35.5,  55.4,  101, 150),
                (55.5,  125.4, 151, 200),
                (125.5, 225.4, 201, 300),
                (225.5, 325.4, 301, 500),
            ] -%}
            {%- for c_lo, c_hi, i_lo, i_hi in bp %}
            when trunc(({{ pm25_expr }})::numeric, 1) <= {{ c_hi }}
                then round(
                    ({{ i_hi }} - {{ i_lo }})::numeric / ({{ c_hi }} - {{ c_lo }})::numeric
                    * (trunc(({{ pm25_expr }})::numeric, 1) - {{ c_lo }})
                    + {{ i_lo }}
                )::integer
            {%- endfor %}
            else 500
        end
    )
{%- endmacro %}


{% macro aqi_category(aqi_expr) -%}
    (
        case
            when ({{ aqi_expr }}) is null then null
            when ({{ aqi_expr }}) <= 50  then 'Good'
            when ({{ aqi_expr }}) <= 100 then 'Moderate'
            when ({{ aqi_expr }}) <= 150 then 'Unhealthy for Sensitive Groups'
            when ({{ aqi_expr }}) <= 200 then 'Unhealthy'
            when ({{ aqi_expr }}) <= 300 then 'Very Unhealthy'
            else 'Hazardous'
        end
    )
{%- endmacro %}


{#
  Convert a UTC timestamptz to local wall-clock time (timestamp without tz)
  in the project's configured timezone. Centralised so DST is handled once.
#}
{% macro to_local(ts_expr) -%}
    (({{ ts_expr }}) at time zone '{{ var("local_timezone") }}')
{%- endmacro %}
