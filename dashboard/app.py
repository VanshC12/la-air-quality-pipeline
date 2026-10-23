"""Streamlit dashboard over the dbt marts.

Reads only from the `analytics` schema (never from raw), so it is a pure
consumer of the warehouse contract the marts expose. Run with
`make dashboard` (Docker) or `streamlit run dashboard/app.py` (host).
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

DSN = os.environ.get("WAREHOUSE_DSN", "postgresql://warehouse:warehouse@localhost:5433/warehouse")

AQI_COLORS = {
    "Good": "#4CAF50",
    "Moderate": "#F2C230",
    "Unhealthy for Sensitive Groups": "#F28C28",
    "Unhealthy": "#E53935",
    "Very Unhealthy": "#8E44AD",
    "Hazardous": "#6D1A1A",
}

st.set_page_config(page_title="LA Air Quality", page_icon=None, layout="wide")


@st.cache_data(ttl=300)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with psycopg2.connect(DSN) as conn:
        return pd.read_sql(sql, conn, params=params)


st.title("Los Angeles Air Quality")
st.caption(
    "Open-Meteo hourly data, modelled with dbt, orchestrated by Airflow. EPA 24-hour PM2.5 AQI."
)

# ---------------------------------------------------------------- health strip
runs = query(
    """select run_id, window_start, status, responses_loaded, started_at, finished_at
         from raw.pipeline_runs order by started_at desc limit 10"""
)
dq = query(
    """select check_name, severity, passed, details, checked_at
         from raw.dq_results where run_id = (select run_id from raw.pipeline_runs
                                              where status <> 'running' order by started_at desc limit 1)
        order by severity, check_name"""
)
col1, col2, col3, col4 = st.columns(4)
if not runs.empty:
    last = runs.iloc[0]
    col1.metric("Last run", str(last["window_start"]), last["status"])
    col2.metric("Responses loaded", int(last["responses_loaded"]))
    col3.metric("Runs succeeded (last 10)", int((runs["status"] == "succeeded").sum()))
if not dq.empty:
    col4.metric("DQ checks passed", f"{int(dq['passed'].sum())}/{len(dq)}")

# ---------------------------------------------------------------- latest
st.subheader("Right now")
latest = query(
    """select location_name, region, climate_zone, observed_at_local, pm2_5, pm10, ozone,
              pm25_aqi_hourly, pm25_aqi_hourly_category, temperature_c, wind_speed_kmh,
              latitude, longitude
         from analytics.mart_latest_conditions order by pm25_aqi_hourly desc nulls last"""
)
if latest.empty:
    st.info("No data yet. Trigger the DAG or run `make backfill-synthetic`.")
    st.stop()

left, right = st.columns([3, 2])
with left:
    st.dataframe(
        latest[
            [
                "location_name",
                "region",
                "observed_at_local",
                "pm2_5",
                "pm25_aqi_hourly",
                "pm25_aqi_hourly_category",
                "temperature_c",
                "wind_speed_kmh",
            ]
        ].rename(
            columns={
                "location_name": "Location",
                "region": "Region",
                "observed_at_local": "Observed (local)",
                "pm2_5": "PM2.5 (ug/m3)",
                "pm25_aqi_hourly": "AQI (hourly)",
                "pm25_aqi_hourly_category": "Category",
                "temperature_c": "Temp (C)",
                "wind_speed_kmh": "Wind (km/h)",
            }
        ),
        hide_index=True,
        width="stretch",
    )
with right:
    fig = px.scatter_map(
        latest,
        lat="latitude",
        lon="longitude",
        color="pm25_aqi_hourly_category",
        color_discrete_map=AQI_COLORS,
        size="pm2_5",
        size_max=22,
        hover_name="location_name",
        hover_data={"pm2_5": True, "pm25_aqi_hourly": True, "latitude": False, "longitude": False},
        zoom=8,
        height=360,
    )
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend_title_text="AQI")
    st.plotly_chart(fig, width="stretch")

# ---------------------------------------------------------------- daily trend
st.subheader("Daily PM2.5 AQI")
days = st.slider("Days", min_value=7, max_value=120, value=30, step=1)
daily = query(
    """select date_key, location_name, climate_zone, daily_pm25_aqi, daily_pm25_aqi_category,
              pm25_mean, pm25_mean_7d, temperature_max_c, is_complete_day
         from analytics.mart_daily_air_quality
        where date_key >= current_date - %s::int
        order by date_key, location_name""",
    (days,),
)
tab1, tab2, tab3 = st.tabs(["By location", "Coastal vs inland", "Weather vs PM2.5"])
with tab1:
    fig = px.line(
        daily,
        x="date_key",
        y="daily_pm25_aqi",
        color="location_name",
        labels={"date_key": "", "daily_pm25_aqi": "AQI (24-h PM2.5)", "location_name": "Location"},
    )
    for y, label in [(50, "Good"), (100, "Moderate"), (150, "USG")]:
        fig.add_hline(
            y=y,
            line_dash="dot",
            line_color="#999",
            annotation_text=label,
            annotation_position="right",
        )
    st.plotly_chart(fig, width="stretch")
with tab2:
    zone = (
        daily[daily["is_complete_day"]]
        .groupby(["date_key", "climate_zone"], as_index=False)["pm25_mean"]
        .mean()
    )
    fig = px.area(
        zone,
        x="date_key",
        y="pm25_mean",
        color="climate_zone",
        labels={"date_key": "", "pm25_mean": "PM2.5 ug/m3 (daily mean)", "climate_zone": "Zone"},
    )
    st.plotly_chart(fig, width="stretch")
with tab3:
    fig = px.scatter(
        daily.dropna(subset=["daily_pm25_aqi"]),
        x="temperature_max_c",
        y="pm25_mean",
        color="climate_zone",
        hover_data=["location_name", "date_key"],
        labels={
            "temperature_max_c": "Daily max temperature (C)",
            "pm25_mean": "PM2.5 daily mean",
            "climate_zone": "Zone",
        },
    )
    st.plotly_chart(fig, width="stretch")

# ---------------------------------------------------------------- ops
with st.expander("Pipeline runs and data-quality checks"):
    st.dataframe(runs, hide_index=True, width="stretch")
    st.dataframe(dq, hide_index=True, width="stretch")
