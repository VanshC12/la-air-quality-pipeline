"""
### LA Air Quality - daily ELT

Fetches the previous UTC day of hourly **air quality** and **weather** for every
location in `dbt/seeds/locations.csv`, lands the raw JSON, loads it into
`raw.api_responses`, runs the pre-transform quality gate, then builds and
tests the dbt models.

```
start_run ─┬─ extract_weather ─────┐
           └─ extract_air_quality ─┴─ load_raw ─ quality_gate ─ dbt_build ─ dbt_test ─ mark_succeeded
                                                                            (any failure) ─ mark_failed
```

* **Idempotent**: every task is keyed on the data interval's date; re-running a
  day overwrites that day in the landing zone and the warehouse.
* **Backfill**: `airflow backfill create --dag-id la_air_quality_daily --from-date ... --to-date ...`
  (or `python -m la_aq run --start ... --end ...` outside Airflow).
* **Fail closed**: a critical data-quality failure stops the run before dbt touches the marts.
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

log = logging.getLogger(__name__)

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT = f"cd {DBT_PROJECT_DIR} && dbt --no-use-colors"
DBT_FLAGS = f"--profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"

# 06:00 UTC daily (22:00/23:00 Pacific). In Airflow 3 a bare cron string gives a
# zero-length data interval at the trigger time, so the one-day `interval` is
# set explicitly: a run that fires at D 06:00 covers [D-1 06:00, D 06:00) and
# processes UTC day D-1, the most recent *completed* day. Manual triggers get
# the same one-day interval ending at the trigger time.
SCHEDULE = CronTriggerTimetable("0 6 * * *", timezone="UTC", interval=timedelta(days=1))


def _window() -> tuple[str, date, date]:
    """(run_id, window_start, window_end): the UTC day this run processes."""
    ctx = get_current_context()
    start = ctx["data_interval_start"]
    day = date(start.year, start.month, start.day)
    return str(ctx["run_id"]), day, day


@dag(
    dag_id="la_air_quality_daily",
    description="Open-Meteo air quality + weather for LA -> Postgres raw -> dbt marts",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "retry_exponential_backoff": True,
    },
    tags=["air-quality", "elt", "dbt", "open-meteo"],
    doc_md=__doc__,
)
def la_air_quality_daily():
    @task
    def start_run() -> dict:
        """Open a row in raw.pipeline_runs and publish the window to downstream tasks."""
        from la_aq.config import Settings
        from la_aq.load import connect, init_warehouse
        from la_aq.load import start_run as _start

        run_id, ws, we = _window()
        settings = Settings.from_env()
        with connect(settings.warehouse_dsn) as conn:
            init_warehouse(conn, settings.init_sql)
            _start(conn, run_id, ws, we, dag_run_id=run_id)
        log.info("run %s window %s..%s", run_id, ws, we)
        return {"run_id": run_id, "window_start": ws.isoformat(), "window_end": we.isoformat()}

    @task
    def extract(source_name: str, run: dict) -> list[str]:
        """Call Open-Meteo for every location and land raw JSON. Returns landed paths."""
        from la_aq.config import Settings
        from la_aq.extract import Source, extract_day
        from la_aq.locations import load_locations

        settings = Settings.from_env()
        locations = load_locations(settings.locations_csv)
        day = date.fromisoformat(run["window_start"])
        paths = extract_day(Source.parse(source_name), locations, day, settings)
        return [str(p) for p in paths]

    @task
    def load_raw(run: dict, landed: list[list[str]]) -> int:
        """Upsert the landed files into raw.api_responses."""
        from la_aq.config import Settings
        from la_aq.load import connect, load_envelopes, read_envelopes

        settings = Settings.from_env()
        paths = [p for group in landed for p in group]
        with connect(settings.warehouse_dsn) as conn:
            n = load_envelopes(conn, read_envelopes(paths), run["run_id"])
        log.info("loaded %d responses", n)
        return n

    @task
    def quality_gate(run: dict, responses_loaded: int) -> list[dict]:
        """Fail the run if any critical raw-layer check fails (write-audit-publish)."""
        from la_aq.config import Settings
        from la_aq.load import connect
        from la_aq.locations import load_locations
        from la_aq.quality import quality_gate as _gate

        settings = Settings.from_env()
        ids = [loc.location_id for loc in load_locations(settings.locations_csv)]
        with connect(settings.warehouse_dsn) as conn:
            results = _gate(
                conn,
                run["run_id"],
                date.fromisoformat(run["window_start"]),
                date.fromisoformat(run["window_end"]),
                ids,
            )
        return [r.__dict__ for r in results]

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} seed {DBT_FLAGS} && {DBT} run {DBT_FLAGS}",
        doc_md="Seeds `locations`, then runs staging (incremental) and marts.",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_FLAGS}",
        doc_md="Schema tests, singular tests and unit tests. Failing rows are stored in the `dq` schema.",
    )

    @task
    def mark_succeeded(run: dict, responses_loaded: int) -> None:
        from la_aq.config import Settings
        from la_aq.load import connect, finish_run

        settings = Settings.from_env()
        with connect(settings.warehouse_dsn) as conn:
            finish_run(conn, run["run_id"], "succeeded", responses_loaded=responses_loaded)

    @task(trigger_rule="one_failed")
    def mark_failed(run: dict | None) -> None:
        from la_aq.config import Settings
        from la_aq.load import connect, finish_run

        if not run:  # start_run itself failed: nothing was recorded, nothing to close
            log.warning("no run row to mark as failed")
            return
        settings = Settings.from_env()
        with connect(settings.warehouse_dsn) as conn:
            finish_run(conn, run["run_id"], "failed", notes="see Airflow task logs")

    run = start_run()
    weather = extract.override(task_id="extract_weather")("weather", run)
    air = extract.override(task_id="extract_air_quality")("air_quality", run)
    loaded = load_raw(run, [weather, air])
    gate = quality_gate(run, loaded)

    succeeded = mark_succeeded(run, loaded)
    failed = mark_failed(run)

    gate >> dbt_build >> dbt_test >> succeeded
    [weather, air, loaded, gate, dbt_build, dbt_test] >> failed


la_air_quality_daily()
