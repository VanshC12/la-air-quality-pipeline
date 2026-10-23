"""Loader + quality gate against a real Postgres.

Needs WAREHOUSE_DSN pointing at a database the test may write to (the CI job
starts a throwaway postgres service; locally, `make up` exposes one on 5433).
Skipped when the variable is not set so `pytest` stays green on a laptop
without Docker.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, timedelta

import pytest

from la_aq.config import Settings
from la_aq.load import connect, finish_run, init_warehouse, load_day, start_run
from la_aq.quality import DataQualityError, quality_gate, run_checks
from la_aq.synthetic import land_synthetic_day

pytestmark = pytest.mark.integration

DSN = os.environ.get("WAREHOUSE_DSN")
if not DSN:
    pytest.skip("WAREHOUSE_DSN not set", allow_module_level=True)


@pytest.fixture
def conn(settings: Settings):
    c = connect(DSN)
    init_warehouse(c, settings.init_sql)
    yield c
    # Leave no trace: everything these tests write is tagged with a test__ run_id.
    c.rollback()
    with c.cursor() as cur:
        for table in ("raw.dq_results", "raw.api_responses", "raw.pipeline_runs"):
            cur.execute(f"delete from {table} where run_id like 'test__%%'")  # noqa: S608
    c.commit()
    c.close()


@pytest.fixture
def test_day() -> date:
    # A day far in the past so the test never collides with real pipeline data.
    return date(2001, 1, 1) + timedelta(days=uuid.uuid4().int % 3000)


def _count(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def test_load_is_idempotent_and_gate_passes(conn, settings, locations, test_day):
    run_id = f"test__{uuid.uuid4().hex[:8]}"
    land_synthetic_day(locations, test_day, settings.raw_data_dir)
    ids = [loc.location_id for loc in locations]

    start_run(conn, run_id, test_day, test_day)
    n1 = load_day(conn, settings.raw_data_dir, test_day, run_id)
    assert n1 == 4  # 2 locations x 2 sources

    first_ingested = _count(
        conn, "select max(ingested_at) from raw.api_responses where run_id = %s", (run_id,)
    )
    n2 = load_day(conn, settings.raw_data_dir, test_day, run_id)
    assert n2 == 4
    rows = _count(
        conn,
        "select count(*) from raw.api_responses where window_start = %s and location_id = any(%s)",
        (test_day, ids),
    )
    assert rows == 4, "re-loading the same day must upsert, not duplicate"
    assert (
        _count(conn, "select max(ingested_at) from raw.api_responses where run_id = %s", (run_id,))
        > first_ingested
    ), "re-load must advance the ingested_at watermark"

    results = quality_gate(conn, run_id, test_day, test_day, ids)
    critical = [r for r in results if r.severity == "critical"]
    assert critical and all(r.passed for r in critical)
    assert _count(conn, "select count(*) from raw.dq_results where run_id = %s", (run_id,)) == len(
        results
    )

    finish_run(conn, run_id, "succeeded", responses_loaded=n2)
    assert (
        _count(conn, "select status from raw.pipeline_runs where run_id = %s", (run_id,))
        == "succeeded"
    )


def test_gate_fails_closed_on_bad_data(conn, settings, locations, test_day):
    run_id = f"test__{uuid.uuid4().hex[:8]}"
    land_synthetic_day(locations, test_day, settings.raw_data_dir)
    ids = [loc.location_id for loc in locations]
    load_day(conn, settings.raw_data_dir, test_day, run_id)

    # Corrupt one hourly PM2.5 value in the warehouse copy.
    with conn.cursor() as cur:
        cur.execute(
            """update raw.api_responses
                  set payload = jsonb_set(payload, '{hourly,pm2_5,3}', '-5'::jsonb)
                where run_id = %s and source = 'open_meteo_air_quality' and location_id = 'dtla'""",
            (run_id,),
        )
    conn.commit()

    with pytest.raises(DataQualityError, match="pm25_range"):
        quality_gate(conn, run_id, test_day, test_day, ids)

    failed = {r.check_name for r in run_checks(conn, test_day, test_day, ids) if not r.passed}
    assert "pm25_range" in failed


def test_gate_detects_missing_location(conn, settings, locations, test_day):
    run_id = f"test__{uuid.uuid4().hex[:8]}"
    land_synthetic_day(locations[:1], test_day, settings.raw_data_dir)
    load_day(conn, settings.raw_data_dir, test_day, run_id)

    results = run_checks(conn, test_day, test_day, [loc.location_id for loc in locations])
    coverage = next(r for r in results if r.check_name == "location_coverage")
    assert not coverage.passed
    assert coverage.observed_value == 2 and coverage.threshold == 4
