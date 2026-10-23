from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from la_aq.config import Settings
from la_aq.locations import Location

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def locations() -> list[Location]:
    return [
        Location("dtla", "Downtown Los Angeles", "Central LA", "Los Angeles", 34.0522, -118.2437),
        Location("riverside", "Riverside", "Inland Empire", "Riverside", 33.9533, -117.3962),
    ]


@pytest.fixture
def day() -> date:
    return date(2026, 10, 20)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        raw_data_dir=tmp_path / "raw",
        locations_csv=REPO / "dbt" / "seeds" / "locations.csv",
        init_sql=REPO / "sql" / "init" / "01_warehouse.sql",
        request_pause_s=0.0,
    )
