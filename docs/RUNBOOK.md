# Runbook: `la_air_quality_daily`

Operational notes for the daily pipeline. Written the way an on-call engineer would want it:
symptom → where to look → what to do.

## Service map

| Component | Where | Health |
|---|---|---|
| Airflow UI / API | http://localhost:8080 | `curl localhost:8080/api/v2/monitor/health` |
| Scheduler | `airflow-scheduler` container | `make ps`, `make logs` |
| DAG processor | `airflow-dag-processor` | import errors show in the UI under *Dags → Import errors* |
| Warehouse | `warehouse` container, host port 5433 | `make psql` |
| Airflow metadata DB | `airflow-db` container (not exposed) | - |
| Dashboard | http://localhost:8501 (profile `dashboard`) | `/_stcore/health` |

## Where state lives

| What | Where | Notes |
|---|---|---|
| Raw API responses | `raw.api_responses` + `data/raw/<source>/dt=YYYY-MM-DD/*.json` | both are rebuildable from the API |
| Run ledger | `raw.pipeline_runs` | one row per Airflow run (run_id = Airflow run_id) or CLI run |
| Quality results | `raw.dq_results` | one row per check per run |
| Failed dbt test rows | `dq.<test_name>` | created only when a test fails |
| dbt artifacts | `dbt/target/` | `run_results.json`, `manifest.json`, `sources.json` (freshness) |

Useful queries:

```sql
-- Last 10 runs
select run_id, window_start, status, responses_loaded, started_at, finished_at
from raw.pipeline_runs order by started_at desc limit 10;

-- Checks that failed recently
select r.window_start, d.check_name, d.severity, d.observed_value, d.threshold, d.details
from raw.dq_results d join raw.pipeline_runs r using (run_id)
where not d.passed order by d.checked_at desc limit 20;

-- Which days are missing or partial?
select location_id, date_key, hours_with_pm25
from analytics.mart_daily_air_quality
where not is_complete_day order by date_key desc;
```

## Failure modes

### `extract_*` fails after retries

*Symptom:* task log ends with `RetryableAPIError: HTTP 5xx` or a network error after 5 attempts.

*Cause:* Open-Meteo outage or no egress from the container.

*Action:* nothing to fix in the data; clear the task (or the whole run) once the API is back.
Retries are exponential (1 s → 30 s) and the task has 2 Airflow-level retries on top, so this
only surfaces for sustained outages. Check egress with
`docker compose exec airflow-scheduler curl -sI https://api.open-meteo.com`.

*Symptom:* `APIError: HTTP 400 ... Invalid date` (no retries).

*Cause:* the requested day is outside the API's window (the forecast endpoint serves roughly the
past 92 days). Happens on backfills that reach too far back.

*Action:* limit the backfill range. For older history switch the weather source to the Open-Meteo
*archive* endpoint (`OPEN_METEO_WEATHER_URL=https://archive-api.open-meteo.com/v1/archive`); the
response shape is identical.

### `quality_gate` fails

*Symptom:* `DataQualityError: critical data-quality checks failed: <names>`. `dbt_build` is skipped,
`mark_failed` runs, the dashboard health strip shows the failed run.

| Check | Likely cause | Action |
|---|---|---|
| `location_coverage` | an extract task was cleared/partially retried; a location was added to the seed mid-run | re-run the day (`make backfill START=<run date> END=<run date>`) |
| `hourly_row_count` / `window_boundaries` | API returned a partial day (rare) or a timezone param changed | inspect `raw.api_responses.payload -> 'hourly' -> 'time'` for the run; re-run the day |
| `pm25_range` / `temperature_range` | upstream model glitch or a units change at the provider | confirm in the raw payload; if the provider changed units, update the staging model and the thresholds together |
| `pm25_null_rate` (warn) | CAMS gaps | no action unless persistent; the daily mart already withholds AQI below 18 h |
| `grid_snap_drift` (warn) | a seed coordinate sits far from the model grid | move the coordinate or accept the drift |

Warn-level failures never block the run; they are visible in `raw.dq_results`.

### `dbt_build` fails

*Symptom:* compile or database error in the task log.

*Action:* reproduce from the host with the same env vars:

```bash
cd dbt && WAREHOUSE_HOST=localhost WAREHOUSE_PORT=5433 dbt run --profiles-dir . --select <model>
```

If a staging model is in a bad state (e.g. interrupted mid-`delete+insert`), rebuild it:
`dbt run --full-refresh --select staging`. The raw layer is untouched, so this is always safe.

### `dbt_test` fails

*Symptom:* one or more tests `FAIL`; failing rows are in `dq.<test_name>`.

```sql
select * from dq.unique_combination_of_columns_fct_hourly_conditions_location_id__observed_at;
```

Common cases:

- `relationships_*_date_key__dim_date`: data outside `dim_date`'s range → widen `dim_date_start/end` in `dbt_project.yml`.
- `assert_past_days_are_complete` (warn): a day in the middle of the loaded range is partial → backfill that day.
- `assert_aqi_matches_provider_within_tolerance` (warn): provider changed its AQI method → compare a few rows; our EPA table is the one the marts publish.

### Scheduler shows the DAG but it never runs

- The DAG is paused: toggle it in the UI. (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` is `false` in Compose.)
- `max_active_runs=1`: a stuck run blocks the queue. Mark it failed and re-trigger.
- Clock: the schedule is 06:00 **UTC**. A manual trigger processes the previous UTC day regardless of the time of day.

### Containers unhealthy after `make up`

- `airflow-init` exits non-zero → usually a permissions issue on bind mounts. Set `AIRFLOW_UID=$(id -u)` in `.env` (Linux) and run `make up` again.
- `airflow-apiserver` unhealthy for > 2 minutes → check `docker compose logs airflow-apiserver`; the JWT secret must be identical across services (it is, via the shared `.env`).

## Backfills

```bash
# Airflow (dates are RUN dates; a run on D processes UTC day D-1)
make backfill START=2026-10-02 END=2026-10-08

# From the host, bypassing Airflow (dates are DATA dates)
make backfill-local START=2026-10-01 END=2026-10-07
```

Both paths end in the same place: upserted raw rows with a fresh `ingested_at`, which the
incremental staging models pick up on the next `dbt run`. Backfilling an already-loaded day
is safe and replaces that day.

## Adding / changing locations

1. Edit `dbt/seeds/locations.csv`.
2. Next run: the extractor reads the CSV, `dbt seed` reloads `locations`, `dim_location` rebuilds.
3. Backfill the new location's history if needed (the whole range re-extracts; existing locations are simply overwritten with identical data).

The `location_coverage` check expects every *active* location to have data, so adding a row
and not running an extract will fail the next gate - by design.

## Rotating secrets

`AIRFLOW__API_AUTH__JWT_SECRET`, `FERNET_KEY` and the warehouse password live in `.env` only.
Change them there and `make down && make up`. Changing `FERNET_KEY` after connections have been
stored requires `airflow rotate-fernet-key`.
