"""DAG integrity: the DAG file parses under the real Airflow version, has the
expected tasks and wiring, and processes the *previous* UTC day.

Runs in CI with Airflow installed; skipped elsewhere.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")
pendulum = pytest.importorskip("pendulum")

pytestmark = pytest.mark.dag

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_shape(dagbag):
    dag = dagbag.dags["la_air_quality_daily"]
    assert {t.task_id for t in dag.tasks} == {
        "start_run",
        "extract_weather",
        "extract_air_quality",
        "load_raw",
        "quality_gate",
        "dbt_build",
        "dbt_test",
        "mark_succeeded",
        "mark_failed",
    }
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert dag.default_args["retries"] >= 1

    wiring = {t.task_id: set(t.downstream_task_ids) for t in dag.tasks}
    assert {"extract_weather", "extract_air_quality"} <= wiring["start_run"]
    assert "load_raw" in wiring["extract_weather"] and "load_raw" in wiring["extract_air_quality"]
    assert "quality_gate" in wiring["load_raw"]
    assert "dbt_build" in wiring["quality_gate"]
    assert "dbt_test" in wiring["dbt_build"]
    assert "mark_succeeded" in wiring["dbt_test"]
    # dbt must never run unless the gate passed.
    assert "dbt_build" not in wiring["load_raw"]


def test_mark_failed_fires_on_any_failure(dagbag):
    dag = dagbag.dags["la_air_quality_daily"]
    failed = dag.get_task("mark_failed")
    assert failed.trigger_rule == "one_failed"  # TriggerRule is a str enum
    assert {
        "extract_weather",
        "extract_air_quality",
        "load_raw",
        "quality_gate",
        "dbt_build",
        "dbt_test",
    } <= set(failed.upstream_task_ids)


def test_run_processes_previous_completed_utc_day(dagbag):
    dag = dagbag.dags["la_air_quality_daily"]
    scheduled = dag.timetable.infer_manual_data_interval(
        run_after=pendulum.datetime(2026, 10, 22, 6, tz="UTC")
    )
    manual = dag.timetable.infer_manual_data_interval(
        run_after=pendulum.datetime(2026, 10, 22, 14, 37, tz="UTC")
    )
    assert scheduled.start.date().isoformat() == "2026-10-21"
    assert manual.start.date().isoformat() == "2026-10-21"
    assert scheduled.end - scheduled.start == timedelta(days=1)
