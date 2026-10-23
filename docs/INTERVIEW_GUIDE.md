# Interview guide: LA Air Quality Data Platform

How to present this project on a resume and talk about it in an interview. Keep this file
out of the public repo if you prefer (add it to `.gitignore`).

---

## Resume bullets (pick 2–3)

**Project title line:** *LA Air Quality Data Platform* | Python, Airflow 3, dbt, PostgreSQL, Docker, GitHub Actions

- Built an end-to-end ELT pipeline in **Airflow 3** ingesting hourly air-quality and weather data for 10 LA-basin locations from REST APIs into a **PostgreSQL** warehouse, with idempotent day-level upserts that make re-runs and backfills safe.
- Modelled raw JSONB into a **star schema** (2 dimensions, hourly fact, daily marts) with **dbt** incremental models, implementing the US EPA 2024 PM2.5 AQI as a tested SQL macro with 75% daily-completeness rules and timezone/DST-aware aggregation.
- Designed a two-layer **data-quality** framework: a fail-closed gate of 7 SQL checks on the raw layer (write-audit-publish) plus 80 dbt data tests, 2 dbt unit tests and source-freshness SLAs, with every result persisted for trend analysis.
- Containerised the platform with **Docker Compose** and added **GitHub Actions** CI running ruff, 30 unit tests, Postgres integration tests, a full `dbt build`, and an Airflow DAG integrity check on every push.

Adjust numbers if you change the project (they are accurate for the code as delivered).

---

## The 30-second pitch

> "I built a daily ELT pipeline for air quality across Los Angeles. Airflow pulls hourly
> pollutant and weather data from the Open-Meteo APIs, lands the raw JSON, and loads it into
> Postgres. Before anything is transformed, a quality gate checks coverage and plausibility,
> and if a critical check fails the run stops so bad data never reaches the dashboards. Then
> dbt flattens the JSON into a star schema and computes the EPA Air Quality Index per day.
> Everything's idempotent, so a backfill is just re-running days, and CI runs the whole thing
> against a real Postgres on every push."

## The 2-minute walkthrough (draw this on a whiteboard)

1. **Source** → two APIs, 10 locations, one call per (source, location, day).
2. **Landing** → verbatim JSON, Hive-partitioned `dt=YYYY-MM-DD`. *Why:* replayable, cloud-portable.
3. **Raw** → `raw.api_responses` as JSONB, upsert on `(source, location, window)`. *Why:* idempotent.
4. **Gate** → 7 checks, results in `raw.dq_results`, critical failure stops the DAG.
5. **dbt** → staging (incremental on `ingested_at`) → `fct_hourly_conditions` → `mart_daily_air_quality`.
6. **Consumers** → Streamlit dashboard reads only `analytics.*`.
7. **Ops** → `pipeline_runs` ledger, freshness SLAs, runbook, CI.

---

## Questions you will likely get, with strong answers

**"Why ELT and not transform in Python before loading?"**
Keeping raw JSON means I can rebuild every downstream table without calling the API again, and
upstream schema changes don't break the loader. Transformations live in dbt, so they're
version-controlled, tested and documented with lineage. The cost is heavier SQL on JSONB, which
is trivial at ~480 rows/day and bounded by the incremental watermark.

**"How is it idempotent? What happens if I run the same day twice?"**
The unit of work is one UTC day everywhere. The raw table has a unique constraint on
`(source, location_id, window_start, window_end)` and loads use `INSERT ... ON CONFLICT DO UPDATE`,
which also bumps `ingested_at`. The staging models are incremental with `delete+insert` on
`(location_id, observed_at)`, so the re-fetched hours replace the old ones. There's an
integration test that loads a day twice and asserts the row count doesn't change, and CI does
the same through dbt.

**"How do you do a backfill?"**
`airflow backfill create` over a date range with `max_active_runs=1`, or the CLI
`python -m la_aq run --start ... --end ...` outside Airflow. Both converge on the same upserts.
One gotcha I hit: in Airflow 3 a cron schedule gives a zero-length data interval by default, so
I used `CronTriggerTimetable` with an explicit one-day interval. A run on day D processes D-1, and
I have a test that asserts that for both scheduled and manual runs.

**"How do you handle late or bad data?"**
Late data: re-running a day upserts it and the watermark propagates it. Bad data: the gate
checks the raw layer before dbt runs; critical failures (missing locations, wrong row counts,
out-of-range values) fail the task, and a `one_failed` trigger-rule task closes the run as failed.
Warn-level issues are recorded but don't block. After dbt, 80 tests run and failing rows are
written to a `dq` schema so I can query them.

**"Why both a Python gate and dbt tests?"**
They answer different questions. The gate asks "did we get what we requested?" and protects the
marts. dbt tests ask "do the models hold their invariants?" - uniqueness of the grain, referential
integrity, value ranges. If I only had dbt tests, a half-empty day would already be in the marts
by the time I found out.

**"Tell me about a tricky bug or design problem."**
Timezones. The API returns UTC; a Los Angeles day straddles two UTC days, and DST days have 23
or 25 hours. I store everything in UTC and convert exactly once in the fact table with a macro.
The daily mart groups on the local date, and the tests allow 1–25 hours per day. The edge days
of any loaded range are naturally partial, so the completeness test only checks interior days.
Second one: the EPA changed the PM2.5 AQI breakpoints in May 2024. I implemented the new table,
pinned the arithmetic with dbt unit tests, and added a warn-level test that compares my number to
the provider's so I'd notice if either side drifts.

**"How would this scale / what would change in production?"**
Landing zone → S3/GCS with the same partitions; Postgres → Snowflake/BigQuery (dbt adapter swap,
JSONB → VARIANT/JSON); Airflow → MWAA/Composer/Astronomer; dbt via Cosmos so each model is its own
task; secrets in a secrets manager; alerting on `mark_failed`. At much higher volume I'd partition
the fact table by date and move extraction to parallel mapped tasks per location.

**"Why Postgres for a warehouse?"**
It runs anywhere for free, supports JSONB and window functions, and dbt treats it like any other
warehouse. The design maps directly onto a columnar warehouse; I listed the mapping in the README.

**"What would you add next?"**
Slack/email alerts on failure, SCD Type 2 on `dim_location`, dynamic task mapping per location,
a Terraform/cloud deployment, and Cosmos for model-level retries.

---

## Before you show it to anyone

- [ ] Replace `YOUR_GITHUB_USERNAME` in `README.md` (2 places).
- [ ] Push to a public GitHub repo; confirm the CI badge goes green.
- [ ] Run it once with real data (`make up`, trigger, `make backfill`), then take two screenshots:
      the Airflow graph view and the dashboard. Save them as `docs/img/airflow-graph.png` and
      `docs/img/dashboard.png` and uncomment the image line in the README.
- [ ] Pin the repo on your GitHub profile; link it from your resume and LinkedIn.
- [ ] Be able to explain every file. Read `quality.py`, the staging SQL, and `aqi.sql` until you
      could rewrite them from memory - interviewers often pick one and ask you to walk through it.
