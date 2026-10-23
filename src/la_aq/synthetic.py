"""Synthetic Open-Meteo-shaped responses for tests, CI and offline demos.

Generated payloads have exactly the same shape as real API responses (same
keys, same parallel-array ``hourly`` layout, same units), so every downstream
step - load, quality gate, dbt models, dbt tests, dashboard - runs unchanged
without network access. Values follow plausible diurnal cycles and are
deterministic for a given (location, day) so CI assertions are stable.
"""

from __future__ import annotations

import math
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from la_aq.extract import HOURLY_VARIABLES, OpenMeteoClient, RawEnvelope, Source, land
from la_aq.locations import Location

_UNITS = {
    Source.WEATHER: {
        "time": "iso8601",
        "temperature_2m": "°C",
        "relative_humidity_2m": "%",
        "precipitation": "mm",
        "wind_speed_10m": "km/h",
        "wind_direction_10m": "°",
        "surface_pressure": "hPa",
    },
    Source.AIR_QUALITY: {
        "time": "iso8601",
        "pm10": "μg/m³",
        "pm2_5": "μg/m³",
        "carbon_monoxide": "μg/m³",
        "nitrogen_dioxide": "μg/m³",
        "sulphur_dioxide": "μg/m³",
        "ozone": "μg/m³",
        "us_aqi": "USAQI",
        "us_aqi_pm2_5": "USAQI",
    },
}


def _hours(day: date) -> list[str]:
    start = datetime(day.year, day.month, day.day)
    return [(start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(24)]


def _rng(location: Location, day: date, source: Source) -> random.Random:
    return random.Random(f"{location.location_id}|{day.isoformat()}|{source.value}")


def synthetic_payload(source: Source, location: Location, day: date) -> dict:
    rng = _rng(location, day, source)
    times = _hours(day)
    # Inland locations run hotter and more polluted than the coast - a
    # recognisable pattern that makes the demo dashboard look right.
    inland = location.region.lower() in {
        "inland empire",
        "san gabriel valley",
        "san fernando valley",
    }
    hourly: dict[str, list] = {"time": times}

    if source is Source.WEATHER:
        base_t = 24.0 if inland else 19.0
        hourly["temperature_2m"] = [
            round(base_t + 7 * math.sin((h - 9) / 24 * 2 * math.pi) + rng.gauss(0, 0.6), 1)
            for h in range(24)
        ]
        hourly["relative_humidity_2m"] = [
            int(min(100, max(10, 60 - 25 * math.sin((h - 9) / 24 * 2 * math.pi) + rng.gauss(0, 3))))
            for h in range(24)
        ]
        hourly["precipitation"] = [
            0.0 if rng.random() > 0.04 else round(rng.uniform(0.1, 2.5), 1) for _ in range(24)
        ]
        hourly["wind_speed_10m"] = [
            round(max(0.0, 8 + 6 * math.sin((h - 14) / 24 * 2 * math.pi) + rng.gauss(0, 1.5)), 1)
            for h in range(24)
        ]
        hourly["wind_direction_10m"] = [int(rng.uniform(180, 300)) for _ in range(24)]
        hourly["surface_pressure"] = [round(1012 + rng.gauss(0, 1.2), 1) for _ in range(24)]
    else:
        base_pm = 22.0 if inland else 9.0
        pm25 = [
            round(
                max(0.5, base_pm + 6 * math.sin((h - 7) / 24 * 2 * math.pi) + rng.gauss(0, 2.0)), 1
            )
            for h in range(24)
        ]
        # Occasional gap, like the real feed.
        if rng.random() < 0.05:
            pm25[rng.randrange(24)] = None
        hourly["pm2_5"] = pm25
        hourly["pm10"] = [None if v is None else round(v * rng.uniform(1.4, 2.2), 1) for v in pm25]
        hourly["carbon_monoxide"] = [round(rng.uniform(150, 450), 0) for _ in range(24)]
        hourly["nitrogen_dioxide"] = [round(rng.uniform(5, 45), 1) for _ in range(24)]
        hourly["sulphur_dioxide"] = [round(rng.uniform(0.5, 4), 1) for _ in range(24)]
        hourly["ozone"] = [
            round(max(5, 60 + 40 * math.sin((h - 14) / 24 * 2 * math.pi) + rng.gauss(0, 5)), 1)
            for h in range(24)
        ]
        hourly["us_aqi_pm2_5"] = [None if v is None else _pm25_aqi_py(v) for v in pm25]
        hourly["us_aqi"] = [
            None if v is None else max(v, int(rng.uniform(20, 60))) for v in hourly["us_aqi_pm2_5"]
        ]

    expected = ["time", *HOURLY_VARIABLES[source]]
    assert sorted(hourly) == sorted(expected), "synthetic payload drifted from HOURLY_VARIABLES"

    # The real API snaps to its model grid; emulate a small, realistic drift.
    return {
        "latitude": round(location.latitude + 0.02, 4),
        "longitude": round(location.longitude - 0.03, 4),
        "generationtime_ms": 0.42,
        "utc_offset_seconds": 0,
        "timezone": "UTC",
        "timezone_abbreviation": "UTC",
        "elevation": 90.0,
        "hourly_units": _UNITS[source],
        "hourly": hourly,
    }


# Reference implementation of the EPA PM2.5 -> AQI formula (2024 breakpoints).
# The dbt macro `pm25_to_aqi` is the production copy; tests compare the two.
_PM25_BREAKPOINTS = [
    (0.0, 9.0, 0, 50),
    (9.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 125.4, 151, 200),
    (125.5, 225.4, 201, 300),
    (225.5, 325.4, 301, 500),
]


def _pm25_aqi_py(pm25: float) -> int:
    c = math.floor(pm25 * 10) / 10  # truncate to one decimal, as EPA specifies
    for c_lo, c_hi, i_lo, i_hi in _PM25_BREAKPOINTS:
        if c_lo <= c <= c_hi:
            return round((i_hi - i_lo) / (c_hi - c_lo) * (c - c_lo) + i_lo)
    return 500


def synthetic_envelope(source: Source, location: Location, day: date) -> RawEnvelope:
    return RawEnvelope(
        source=source.value,
        location_id=location.location_id,
        window_start=day,
        window_end=day,
        fetched_at=datetime(day.year, day.month, day.day, 6, 5, tzinfo=UTC),
        request_params=OpenMeteoClient.build_params(source, location, day, day),
        payload=synthetic_payload(source, location, day),
    )


def land_synthetic_day(locations: list[Location], day: date, raw_dir: Path) -> list[Path]:
    paths = []
    for source in Source:
        for loc in locations:
            paths.append(land(synthetic_envelope(source, loc, day), raw_dir))
    return paths
