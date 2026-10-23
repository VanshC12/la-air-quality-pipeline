"""Monitoring locations.

The dbt seed ``dbt/seeds/locations.csv`` is the single source of truth: dbt
loads it into ``dim_location`` and the extractor reads the same file to know
which coordinates to query. Add a row there and both sides pick it up.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Location:
    location_id: str
    location_name: str
    region: str
    county: str
    latitude: float
    longitude: float
    is_active: bool = True


def _parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def load_locations(csv_path: Path, active_only: bool = True) -> list[Location]:
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = [
            Location(
                location_id=r["location_id"].strip(),
                location_name=r["location_name"].strip(),
                region=r["region"].strip(),
                county=r["county"].strip(),
                latitude=float(r["latitude"]),
                longitude=float(r["longitude"]),
                is_active=_parse_bool(r.get("is_active", "true")),
            )
            for r in reader
        ]
    if active_only:
        rows = [loc for loc in rows if loc.is_active]
    if not rows:
        raise ValueError(f"No locations loaded from {csv_path}")
    ids = [loc.location_id for loc in rows]
    if len(ids) != len(set(ids)):
        raise ValueError(f"Duplicate location_id values in {csv_path}")
    return rows
