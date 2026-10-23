from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from la_aq.extract import HOURLY_VARIABLES, Source
from la_aq.locations import load_locations
from la_aq.synthetic import _pm25_aqi_py, land_synthetic_day, synthetic_payload

SEED = Path(__file__).resolve().parents[2] / "dbt" / "seeds" / "locations.csv"


def test_seed_loads_and_ids_are_unique():
    locs = load_locations(SEED)
    assert len(locs) == 10
    assert len({loc.location_id for loc in locs}) == 10
    assert all(33 < loc.latitude < 35 and -119 < loc.longitude < -116 for loc in locs)


def test_inactive_locations_are_filtered(tmp_path: Path):
    csv = tmp_path / "locations.csv"
    csv.write_text(
        "location_id,location_name,region,county,latitude,longitude,is_active\n"
        "a,A,R,C,34.0,-118.0,true\n"
        "b,B,R,C,34.1,-118.1,false\n"
    )
    assert [loc.location_id for loc in load_locations(csv)] == ["a"]
    assert [loc.location_id for loc in load_locations(csv, active_only=False)] == ["a", "b"]


def test_duplicate_ids_rejected(tmp_path: Path):
    csv = tmp_path / "locations.csv"
    csv.write_text(
        "location_id,location_name,region,county,latitude,longitude,is_active\n"
        "a,A,R,C,34.0,-118.0,true\n"
        "a,A2,R,C,34.1,-118.1,true\n"
    )
    with pytest.raises(ValueError, match="Duplicate"):
        load_locations(csv)


@pytest.mark.parametrize("source", list(Source))
def test_synthetic_payload_matches_api_shape(source, locations, day):
    payload = synthetic_payload(source, locations[0], day)
    hourly = payload["hourly"]
    assert set(hourly) == {"time", *HOURLY_VARIABLES[source]}
    assert len(hourly["time"]) == 24
    assert hourly["time"][0] == "2026-10-20T00:00" and hourly["time"][-1] == "2026-10-20T23:00"
    assert all(len(v) == 24 for v in hourly.values())
    assert payload["timezone"] == "UTC"


def test_synthetic_payload_is_deterministic(locations, day):
    a = synthetic_payload(Source.AIR_QUALITY, locations[0], day)
    b = synthetic_payload(Source.AIR_QUALITY, locations[0], day)
    assert a == b
    assert a != synthetic_payload(Source.AIR_QUALITY, locations[1], day)


def test_land_synthetic_day_writes_both_sources(tmp_path: Path, locations, day):
    paths = land_synthetic_day(locations, day, tmp_path)
    assert len(paths) == 4
    rel = sorted(p.relative_to(tmp_path).as_posix() for p in paths)
    assert rel == [
        "open_meteo_air_quality/dt=2026-10-20/dtla.json",
        "open_meteo_air_quality/dt=2026-10-20/riverside.json",
        "open_meteo_weather/dt=2026-10-20/dtla.json",
        "open_meteo_weather/dt=2026-10-20/riverside.json",
    ]


@pytest.mark.parametrize(
    ("pm25", "aqi"),
    [
        (0.0, 0),
        (5.0, 28),
        (9.0, 50),
        (9.1, 51),
        (12.0, 56),
        (35.4, 100),
        (35.5, 101),
        (40.0, 112),
        (55.4, 150),
        (100.0, 182),
        (125.4, 200),
        (225.4, 300),
        (325.4, 500),
        (400.0, 500),
    ],
)
def test_reference_aqi_formula_matches_epa_table(pm25, aqi):
    """Pins the Python reference used to cross-check the dbt macro (2024 EPA breakpoints)."""
    assert _pm25_aqi_py(pm25) == aqi


def test_day_fixture_is_a_date(day):
    assert isinstance(day, date)
