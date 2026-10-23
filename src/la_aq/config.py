"""Runtime configuration.

All settings come from environment variables so the same code runs unchanged
on a laptop, inside the Airflow container, and in CI. Defaults target the
local docker-compose stack (warehouse exposed on host port 5433).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    warehouse_dsn: str = "postgresql://warehouse:warehouse@localhost:5433/warehouse"
    raw_data_dir: Path = Path("data/raw")
    locations_csv: Path = Path("dbt/seeds/locations.csv")
    init_sql: Path = Path("sql/init/01_warehouse.sql")
    weather_url: str = "https://api.open-meteo.com/v1/forecast"
    air_quality_url: str = "https://air-quality-api.open-meteo.com/v1/air-quality"
    http_timeout_s: float = 30.0
    # Open-Meteo is free without a key for non-commercial use (10k req/day).
    # Be a good citizen: a short pause between calls keeps us far below limits.
    request_pause_s: float = 0.2

    @classmethod
    def from_env(cls) -> Settings:
        d = cls()
        return cls(
            warehouse_dsn=os.getenv("WAREHOUSE_DSN", d.warehouse_dsn),
            raw_data_dir=Path(os.getenv("RAW_DATA_DIR", str(d.raw_data_dir))),
            locations_csv=Path(os.getenv("LOCATIONS_CSV", str(d.locations_csv))),
            init_sql=Path(os.getenv("WAREHOUSE_INIT_SQL", str(d.init_sql))),
            weather_url=os.getenv("OPEN_METEO_WEATHER_URL", d.weather_url),
            air_quality_url=os.getenv("OPEN_METEO_AIR_QUALITY_URL", d.air_quality_url),
            http_timeout_s=float(os.getenv("HTTP_TIMEOUT_S", d.http_timeout_s)),
            request_pause_s=float(os.getenv("REQUEST_PAUSE_S", d.request_pause_s)),
        )
