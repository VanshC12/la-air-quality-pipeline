-- ---------------------------------------------------------------------------
-- Warehouse bootstrap: raw ("bronze") layer owned by the ingestion code.
--
-- Everything downstream of this file (staging, marts) is owned by dbt.
-- This file is idempotent: it is run by Postgres on first container start
-- (docker-entrypoint-initdb.d) AND by `python -m la_aq init-db`, so it must
-- be safe to execute repeatedly.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS raw;

-- One row per API call. The full JSON response is kept verbatim so the
-- warehouse can always be rebuilt from raw without re-hitting the API
-- (ELT: load first, transform in-warehouse with dbt).
CREATE TABLE IF NOT EXISTS raw.api_responses (
    response_id     BIGSERIAL    PRIMARY KEY,
    source          TEXT         NOT NULL,  -- open_meteo_weather | open_meteo_air_quality
    location_id     TEXT         NOT NULL,
    window_start    DATE         NOT NULL,  -- first UTC day requested (inclusive)
    window_end      DATE         NOT NULL,  -- last UTC day requested (inclusive)
    fetched_at      TIMESTAMPTZ  NOT NULL,  -- when the HTTP call completed
    run_id          TEXT         NOT NULL,  -- Airflow run_id or cli__<date>__<uuid>
    request_params  JSONB        NOT NULL,  -- exact query string sent (reproducibility)
    payload         JSONB        NOT NULL,  -- verbatim API response
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- Natural key: re-running the same window for the same location/source
    -- overwrites the previous response instead of duplicating it.
    CONSTRAINT api_responses_natural_key
        UNIQUE (source, location_id, window_start, window_end)
);

-- dbt's incremental staging models filter on ingested_at (high-watermark).
CREATE INDEX IF NOT EXISTS api_responses_ingested_at_idx
    ON raw.api_responses (ingested_at);
CREATE INDEX IF NOT EXISTS api_responses_source_window_idx
    ON raw.api_responses (source, window_start);

-- Pipeline run ledger: one row per orchestrated run (observability / audit).
CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    run_id            TEXT         PRIMARY KEY,
    dag_run_id        TEXT,
    window_start      DATE         NOT NULL,
    window_end        DATE         NOT NULL,
    started_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    status            TEXT         NOT NULL DEFAULT 'running',  -- running | succeeded | failed
    responses_loaded  INTEGER      NOT NULL DEFAULT 0,
    notes             TEXT
);

-- Results of the pre-transform data-quality gate (write-audit-publish).
CREATE TABLE IF NOT EXISTS raw.dq_results (
    dq_result_id    BIGSERIAL    PRIMARY KEY,
    run_id          TEXT         NOT NULL,
    check_name      TEXT         NOT NULL,
    severity        TEXT         NOT NULL,  -- critical | warn
    passed          BOOLEAN      NOT NULL,
    observed_value  NUMERIC,
    threshold       NUMERIC,
    details         TEXT,
    checked_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dq_results_run_id_idx ON raw.dq_results (run_id);
