"""Quality gate: validate the raw layer *before* dbt transforms it.

This is the "audit" step of write-audit-publish. Checks run as SQL directly
against ``raw.api_responses`` for one window, results are persisted to
``raw.dq_results`` for trend analysis, and the run fails on any failed
``critical`` check so bad data never reaches the marts. ``warn`` checks are
recorded and logged but do not block.

Two layers of quality exist on purpose:
  * here  - "did we get what we asked for?" (coverage, completeness, plausibility)
  * dbt   - "does the modelled data hold its invariants?" (uniqueness, ranges, relationships)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from la_aq.extract import Source
from la_aq.load import Connection

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckResult:
    check_name: str
    severity: str  # critical | warn
    passed: bool
    observed_value: float | None
    threshold: float | None
    details: str


# Hourly values flattened straight out of the JSONB payloads for one window.
# jsonb ->> int returns the array element at that (0-based) index as text.
_HOURLY_CTE = """
WITH responses AS (
    SELECT response_id, source, location_id, payload
      FROM raw.api_responses
     WHERE window_start = %(ws)s AND window_end = %(we)s
),
hourly AS (
    SELECT r.response_id,
           r.source,
           r.location_id,
           (r.payload -> 'hourly' -> 'time'           ->> (t.ord - 1))             AS observed_at_text,
           (r.payload -> 'hourly' -> 'temperature_2m' ->> (t.ord - 1))::numeric    AS temperature_2m,
           (r.payload -> 'hourly' -> 'pm2_5'          ->> (t.ord - 1))::numeric    AS pm2_5
      FROM responses r
      CROSS JOIN LATERAL generate_series(
               1, jsonb_array_length(r.payload -> 'hourly' -> 'time')) AS t(ord)
)
"""


def _scalar(conn: Connection, sql: str, params: dict) -> float | None:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return None if row is None or row[0] is None else float(row[0])


def run_checks(
    conn: Connection,
    window_start: date,
    window_end: date,
    expected_location_ids: list[str],
) -> list[CheckResult]:
    p = {"ws": window_start, "we": window_end}
    n_days = (window_end - window_start).days + 1
    expected_hours = 24 * n_days
    expected_responses = len(expected_location_ids) * len(Source)
    results: list[CheckResult] = []

    # 1. Coverage: one response per (source, location) in the window.
    observed = (
        _scalar(
            conn,
            """SELECT COUNT(DISTINCT (source, location_id)) FROM raw.api_responses
            WHERE window_start = %(ws)s AND window_end = %(we)s""",
            p,
        )
        or 0.0
    )
    results.append(
        CheckResult(
            "location_coverage",
            "critical",
            observed >= expected_responses,
            observed,
            expected_responses,
            f"{int(observed)}/{expected_responses} (source, location) pairs present",
        )
    )

    # 2. Every response carries exactly the expected number of hourly rows.
    bad_len = (
        _scalar(
            conn,
            """SELECT COUNT(*) FROM raw.api_responses
            WHERE window_start = %(ws)s AND window_end = %(we)s
              AND jsonb_array_length(payload -> 'hourly' -> 'time') <> %(expected)s""",
            {**p, "expected": expected_hours},
        )
        or 0.0
    )
    results.append(
        CheckResult(
            "hourly_row_count",
            "critical",
            bad_len == 0,
            bad_len,
            0,
            f"{int(bad_len)} responses without exactly {expected_hours} hourly rows",
        )
    )

    # 3. The API returned the window we asked for (first/last timestamps).
    bad_window = (
        _scalar(
            conn,
            """SELECT COUNT(*) FROM raw.api_responses
            WHERE window_start = %(ws)s AND window_end = %(we)s
              AND (
                   (payload -> 'hourly' -> 'time' ->> 0) <> to_char(%(ws)s::date, 'YYYY-MM-DD') || 'T00:00'
                OR (payload -> 'hourly' -> 'time' ->> -1) <> to_char(%(we)s::date, 'YYYY-MM-DD') || 'T23:00'
              )""",
            p,
        )
        or 0.0
    )
    results.append(
        CheckResult(
            "window_boundaries",
            "critical",
            bad_window == 0,
            bad_window,
            0,
            f"{int(bad_window)} responses whose first/last hour do not match the requested window",
        )
    )

    # 4. PM2.5 completeness (warn: the model occasionally leaves gaps).
    pm25_null_rate = _scalar(
        conn,
        _HOURLY_CTE
        + """SELECT AVG(CASE WHEN pm2_5 IS NULL THEN 1.0 ELSE 0.0 END)
               FROM hourly WHERE source = %(aq)s""",
        {**p, "aq": Source.AIR_QUALITY.value},
    )
    results.append(
        CheckResult(
            "pm25_null_rate",
            "warn",
            pm25_null_rate is not None and pm25_null_rate <= 0.10,
            pm25_null_rate,
            0.10,
            f"share of null pm2_5 hourly values = {pm25_null_rate}",
        )
    )

    # 5. Plausibility: PM2.5 in [0, 1000] ug/m3.
    bad_pm25 = (
        _scalar(
            conn,
            _HOURLY_CTE
            + """SELECT COUNT(*) FROM hourly
              WHERE source = %(aq)s AND (pm2_5 < 0 OR pm2_5 > 1000)""",
            {**p, "aq": Source.AIR_QUALITY.value},
        )
        or 0.0
    )
    results.append(
        CheckResult(
            "pm25_range",
            "critical",
            bad_pm25 == 0,
            bad_pm25,
            0,
            f"{int(bad_pm25)} pm2_5 values outside [0, 1000]",
        )
    )

    # 6. Plausibility: temperature in [-30, 60] C.
    bad_temp = (
        _scalar(
            conn,
            _HOURLY_CTE
            + """SELECT COUNT(*) FROM hourly
              WHERE source = %(wx)s AND (temperature_2m < -30 OR temperature_2m > 60)""",
            {**p, "wx": Source.WEATHER.value},
        )
        or 0.0
    )
    results.append(
        CheckResult(
            "temperature_range",
            "critical",
            bad_temp == 0,
            bad_temp,
            0,
            f"{int(bad_temp)} temperature_2m values outside [-30, 60]",
        )
    )

    # 7. Open-Meteo snaps coordinates to its model grid; flag drift > 0.15 deg (~15 km).
    drift = (
        _scalar(
            conn,
            """SELECT COUNT(*) FROM raw.api_responses
            WHERE window_start = %(ws)s AND window_end = %(we)s
              AND (
                   ABS((payload ->> 'latitude')::numeric  - (request_params ->> 'latitude')::numeric)  > 0.15
                OR ABS((payload ->> 'longitude')::numeric - (request_params ->> 'longitude')::numeric) > 0.15
              )""",
            p,
        )
        or 0.0
    )
    results.append(
        CheckResult(
            "grid_snap_drift",
            "warn",
            drift == 0,
            drift,
            0,
            f"{int(drift)} responses where the API grid point is >0.15 deg from the requested coordinates",
        )
    )

    return results


def persist_results(conn: Connection, run_id: str, results: list[CheckResult]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO raw.dq_results
                   (run_id, check_name, severity, passed, observed_value, threshold, details)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            [
                (
                    run_id,
                    r.check_name,
                    r.severity,
                    r.passed,
                    r.observed_value,
                    r.threshold,
                    r.details,
                )
                for r in results
            ],
        )
    conn.commit()


def enforce(results: list[CheckResult]) -> None:
    """Log every result; raise if any critical check failed."""
    failed_critical = []
    for r in results:
        level = (
            logging.INFO
            if r.passed
            else (logging.ERROR if r.severity == "critical" else logging.WARNING)
        )
        log.log(
            level,
            "[%s] %-20s %s  %s",
            "PASS" if r.passed else "FAIL",
            r.check_name,
            r.severity,
            r.details,
        )
        if not r.passed and r.severity == "critical":
            failed_critical.append(r.check_name)
    if failed_critical:
        raise DataQualityError(f"critical data-quality checks failed: {', '.join(failed_critical)}")


def quality_gate(
    conn: Connection,
    run_id: str,
    window_start: date,
    window_end: date,
    expected_location_ids: list[str],
) -> list[CheckResult]:
    results = run_checks(conn, window_start, window_end, expected_location_ids)
    persist_results(conn, run_id, results)
    enforce(results)
    return results
