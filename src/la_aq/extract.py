"""Extract: Open-Meteo API client + raw landing zone.

Design notes
------------
* Each (source, location, UTC day) is one API call and one landed file. A day
  is the unit of work everywhere (Airflow data interval, CLI loop, warehouse
  natural key), which keeps re-runs and backfills trivially idempotent.
* Responses are stored verbatim inside an *envelope* that records exactly what
  was requested and when. The warehouse can be rebuilt from these files.
* Landing path is Hive-partitioned (``<source>/dt=YYYY-MM-DD/<location>.json``)
  so the same layout can be dropped onto S3/GCS and crawled by Spark/Athena/BigQuery.
* Retries use exponential backoff for transient failures only (5xx, 429,
  network errors). 4xx errors are permanent and surface immediately.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from la_aq.config import Settings
from la_aq.locations import Location

log = logging.getLogger(__name__)


class Source(StrEnum):
    WEATHER = "open_meteo_weather"
    AIR_QUALITY = "open_meteo_air_quality"

    @classmethod
    def parse(cls, value: str) -> Source:
        aliases = {
            "weather": cls.WEATHER,
            "air_quality": cls.AIR_QUALITY,
            "aq": cls.AIR_QUALITY,
        }
        if value in aliases:
            return aliases[value]
        return cls(value)


HOURLY_VARIABLES: dict[Source, list[str]] = {
    Source.WEATHER: [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "wind_direction_10m",
        "surface_pressure",
    ],
    Source.AIR_QUALITY: [
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "us_aqi",
        "us_aqi_pm2_5",
    ],
}


class APIError(RuntimeError):
    """Permanent failure (bad request, unexpected payload). Do not retry."""


class RetryableAPIError(APIError):
    """Transient failure (5xx, 429, network). Safe to retry."""


@dataclass(frozen=True)
class RawEnvelope:
    source: str
    location_id: str
    window_start: date
    window_end: date
    fetched_at: datetime
    request_params: dict[str, Any]
    payload: dict[str, Any]

    def to_json(self) -> str:
        d = asdict(self)
        d["window_start"] = self.window_start.isoformat()
        d["window_end"] = self.window_end.isoformat()
        d["fetched_at"] = self.fetched_at.isoformat()
        return json.dumps(d, separators=(",", ":"))

    @classmethod
    def from_json(cls, text: str) -> RawEnvelope:
        d = json.loads(text)
        return cls(
            source=d["source"],
            location_id=d["location_id"],
            window_start=date.fromisoformat(d["window_start"]),
            window_end=date.fromisoformat(d["window_end"]),
            fetched_at=datetime.fromisoformat(d["fetched_at"]),
            request_params=d["request_params"],
            payload=d["payload"],
        )


class OpenMeteoClient:
    def __init__(self, settings: Settings, session: requests.Session | None = None):
        self.settings = settings
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "la-air-quality-pipeline/0.1"})

    def _url(self, source: Source) -> str:
        return {
            Source.WEATHER: self.settings.weather_url,
            Source.AIR_QUALITY: self.settings.air_quality_url,
        }[source]

    @staticmethod
    def build_params(source: Source, location: Location, start: date, end: date) -> dict[str, Any]:
        return {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "hourly": ",".join(HOURLY_VARIABLES[source]),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            # Always request UTC. Local-time conversion happens in dbt, which
            # keeps DST handling in one place and makes timestamps unambiguous.
            "timezone": "UTC",
        }

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(RetryableAPIError),
        before_sleep=lambda rs: log.warning(
            "Retrying Open-Meteo call (attempt %s): %s", rs.attempt_number, rs.outcome.exception()
        ),
    )
    def fetch(self, source: Source, location: Location, start: date, end: date) -> RawEnvelope:
        params = self.build_params(source, location, start, end)
        url = self._url(source)
        try:
            resp = self.session.get(url, params=params, timeout=self.settings.http_timeout_s)
        except (requests.ConnectionError, requests.Timeout) as exc:
            raise RetryableAPIError(f"network error calling {url}: {exc}") from exc

        if resp.status_code == 429 or resp.status_code >= 500:
            raise RetryableAPIError(f"HTTP {resp.status_code} from {url}")
        if resp.status_code >= 400:
            reason = _error_reason(resp)
            raise APIError(f"HTTP {resp.status_code} from {url}: {reason}")

        try:
            payload = resp.json()
        except ValueError as exc:
            raise APIError(f"non-JSON response from {url}") from exc

        if not isinstance(payload, dict) or "hourly" not in payload:
            raise APIError(f"unexpected payload shape from {url}: keys={list(payload)[:10]}")

        return RawEnvelope(
            source=source.value,
            location_id=location.location_id,
            window_start=start,
            window_end=end,
            fetched_at=datetime.now(tz=UTC),
            request_params=params,
            payload=payload,
        )


def _error_reason(resp: requests.Response) -> str:
    try:
        body = resp.json()
        return str(body.get("reason") or body)
    except ValueError:
        return resp.text[:200]


def landing_path(raw_dir: Path, envelope: RawEnvelope) -> Path:
    return (
        raw_dir
        / envelope.source
        / f"dt={envelope.window_start.isoformat()}"
        / f"{envelope.location_id}.json"
    )


def land(envelope: RawEnvelope, raw_dir: Path) -> Path:
    """Write the envelope atomically (tmp file + rename) so a crash never leaves a half file."""
    path = landing_path(raw_dir, envelope)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(envelope.to_json(), encoding="utf-8")
    os.replace(tmp, path)
    return path


def extract_day(
    source: Source,
    locations: list[Location],
    day: date,
    settings: Settings,
    client: OpenMeteoClient | None = None,
) -> list[Path]:
    """Fetch one UTC day for every location and land the responses. Returns landed paths."""
    client = client or OpenMeteoClient(settings)
    paths: list[Path] = []
    for loc in locations:
        env = client.fetch(source, loc, day, day)
        path = land(env, settings.raw_data_dir)
        n_hours = len(env.payload.get("hourly", {}).get("time", []))
        log.info(
            "landed %s %s %s (%d hourly rows) -> %s",
            source.value,
            loc.location_id,
            day,
            n_hours,
            path,
        )
        paths.append(path)
        if settings.request_pause_s:
            time.sleep(settings.request_pause_s)
    return paths
