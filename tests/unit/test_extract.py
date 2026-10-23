from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
import requests

from la_aq.extract import (
    HOURLY_VARIABLES,
    APIError,
    OpenMeteoClient,
    RawEnvelope,
    RetryableAPIError,
    Source,
    extract_day,
    land,
    landing_path,
)
from la_aq.synthetic import synthetic_payload


def _response(status: int, body: dict | None = None, text: str = "") -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.text = text
    if body is None:
        resp.json.side_effect = ValueError("no json")
    else:
        resp.json.return_value = body
    return resp


def _client(settings, *responses):
    session = MagicMock(spec=requests.Session)
    session.headers = {}
    session.get.side_effect = list(responses)
    client = OpenMeteoClient(settings, session=session)
    # No sleeping between retries in tests.
    client.fetch.retry.wait = lambda *_: 0  # type: ignore[attr-defined]
    return client, session


def test_build_params_requests_utc_and_all_variables(locations, day):
    params = OpenMeteoClient.build_params(Source.AIR_QUALITY, locations[0], day, day)
    assert params["timezone"] == "UTC"
    assert params["start_date"] == params["end_date"] == "2026-10-20"
    assert params["hourly"].split(",") == HOURLY_VARIABLES[Source.AIR_QUALITY]
    assert params["latitude"] == pytest.approx(34.0522)


def test_fetch_returns_envelope_with_verbatim_payload(settings, locations, day):
    payload = synthetic_payload(Source.WEATHER, locations[0], day)
    client, session = _client(settings, _response(200, payload))

    env = client.fetch(Source.WEATHER, locations[0], day, day)

    assert env.source == "open_meteo_weather"
    assert env.location_id == "dtla"
    assert env.payload == payload
    assert env.request_params["hourly"].startswith("temperature_2m")
    session.get.assert_called_once()
    assert session.get.call_args.args[0] == settings.weather_url


def test_fetch_retries_transient_errors_then_succeeds(settings, locations, day):
    payload = synthetic_payload(Source.AIR_QUALITY, locations[0], day)
    client, session = _client(
        settings,
        _response(503),
        requests.ConnectionError("boom"),
        _response(429),
        _response(200, payload),
    )

    env = client.fetch(Source.AIR_QUALITY, locations[0], day, day)

    assert env.payload == payload
    assert session.get.call_count == 4


def test_fetch_gives_up_after_five_transient_failures(settings, locations, day):
    client, session = _client(settings, *[_response(502)] * 6)
    with pytest.raises(RetryableAPIError):
        client.fetch(Source.AIR_QUALITY, locations[0], day, day)
    assert session.get.call_count == 5


def test_fetch_does_not_retry_client_errors(settings, locations, day):
    client, session = _client(settings, _response(400, {"error": True, "reason": "Invalid date"}))
    with pytest.raises(APIError, match="Invalid date"):
        client.fetch(Source.WEATHER, locations[0], day, day)
    assert session.get.call_count == 1


def test_fetch_rejects_payload_without_hourly_block(settings, locations, day):
    client, _ = _client(settings, _response(200, {"latitude": 1, "longitude": 2}))
    with pytest.raises(APIError, match="unexpected payload"):
        client.fetch(Source.WEATHER, locations[0], day, day)


def test_land_writes_hive_partitioned_path_and_round_trips(settings, locations, day):
    env = RawEnvelope(
        source=Source.WEATHER.value,
        location_id="dtla",
        window_start=day,
        window_end=day,
        fetched_at=datetime(2026, 10, 21, 6, 0, tzinfo=UTC),
        request_params={"a": 1},
        payload=synthetic_payload(Source.WEATHER, locations[0], day),
    )

    path = land(env, settings.raw_data_dir)

    assert path == landing_path(settings.raw_data_dir, env)
    assert (
        path.relative_to(settings.raw_data_dir).as_posix()
        == "open_meteo_weather/dt=2026-10-20/dtla.json"
    )
    assert not path.with_suffix(".json.tmp").exists()
    assert RawEnvelope.from_json(path.read_text()) == env
    assert json.loads(path.read_text())["payload"]["hourly"]["time"][0] == "2026-10-20T00:00"


def test_extract_day_lands_one_file_per_location(settings, locations, day):
    client, session = _client(
        settings,
        *[_response(200, synthetic_payload(Source.AIR_QUALITY, loc, day)) for loc in locations],
    )

    paths = extract_day(Source.AIR_QUALITY, locations, day, settings, client=client)

    assert [p.name for p in paths] == ["dtla.json", "riverside.json"]
    assert session.get.call_count == 2
    assert all(p.exists() for p in paths)
