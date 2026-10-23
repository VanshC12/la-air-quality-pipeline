"""Load: landed envelopes -> raw.api_responses (Postgres, JSONB).

The upsert on the natural key (source, location_id, window_start, window_end)
makes every load idempotent: re-running a day replaces that day's responses
and bumps ``ingested_at``, which is the high-watermark dbt's incremental
models use to pick the change up.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date
from pathlib import Path

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from la_aq.extract import RawEnvelope

log = logging.getLogger(__name__)

Connection = psycopg2.extensions.connection


def connect(dsn: str) -> Connection:
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def init_warehouse(conn: Connection, init_sql: Path) -> None:
    """Create the raw schema/tables. Safe to run repeatedly (all IF NOT EXISTS)."""
    sql = init_sql.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log.info("warehouse raw layer ready (%s)", init_sql)


# --------------------------------------------------------------------------- runs


def start_run(
    conn: Connection,
    run_id: str,
    window_start: date,
    window_end: date,
    dag_run_id: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.pipeline_runs (run_id, dag_run_id, window_start, window_end, status)
            VALUES (%s, %s, %s, %s, 'running')
            ON CONFLICT (run_id) DO UPDATE
               SET started_at = now(), finished_at = NULL, status = 'running',
                   responses_loaded = 0, notes = NULL, dag_run_id = EXCLUDED.dag_run_id
            """,
            (run_id, dag_run_id, window_start, window_end),
        )
    conn.commit()


def finish_run(
    conn: Connection,
    run_id: str,
    status: str,
    responses_loaded: int | None = None,
    notes: str | None = None,
) -> None:
    if status not in {"succeeded", "failed"}:
        raise ValueError(f"invalid status {status!r}")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE raw.pipeline_runs
               SET finished_at = now(),
                   status = %s,
                   responses_loaded = COALESCE(%s, responses_loaded),
                   notes = COALESCE(%s, notes)
             WHERE run_id = %s
            """,
            (status, responses_loaded, notes, run_id),
        )
    conn.commit()


# --------------------------------------------------------------------------- responses

_UPSERT_SQL = """
INSERT INTO raw.api_responses
    (source, location_id, window_start, window_end, fetched_at, run_id, request_params, payload)
VALUES %s
ON CONFLICT (source, location_id, window_start, window_end) DO UPDATE
   SET fetched_at     = EXCLUDED.fetched_at,
       run_id         = EXCLUDED.run_id,
       request_params = EXCLUDED.request_params,
       payload        = EXCLUDED.payload,
       ingested_at    = now()
"""


def load_envelopes(conn: Connection, envelopes: Iterable[RawEnvelope], run_id: str) -> int:
    rows = [
        (
            e.source,
            e.location_id,
            e.window_start,
            e.window_end,
            e.fetched_at,
            run_id,
            psycopg2.extras.Json(e.request_params),
            psycopg2.extras.Json(e.payload),
        )
        for e in envelopes
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, _UPSERT_SQL, rows, page_size=200)
    conn.commit()
    log.info("upserted %d responses into raw.api_responses (run_id=%s)", len(rows), run_id)
    return len(rows)


def read_envelopes(paths: Iterable[Path]) -> list[RawEnvelope]:
    return [RawEnvelope.from_json(Path(p).read_text(encoding="utf-8")) for p in paths]


def landed_files_for_day(
    raw_dir: Path, day: date, sources: Iterable[str] | None = None
) -> list[Path]:
    """All landed files for a UTC day, across sources (or the given ones)."""
    files: list[Path] = []
    for source_dir in sorted(raw_dir.glob("*")):
        if not source_dir.is_dir():
            continue
        if sources is not None and source_dir.name not in set(sources):
            continue
        part = source_dir / f"dt={day.isoformat()}"
        if part.is_dir():
            files.extend(sorted(part.glob("*.json")))
    return files


def load_day(conn: Connection, raw_dir: Path, day: date, run_id: str) -> int:
    files = landed_files_for_day(raw_dir, day)
    if not files:
        raise FileNotFoundError(f"no landed files for {day} under {raw_dir}")
    return load_envelopes(conn, read_envelopes(files), run_id)
