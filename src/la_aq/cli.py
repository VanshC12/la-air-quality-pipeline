"""Command-line entrypoint: ``python -m la_aq <command>`` or ``la-aq <command>``.

The CLI wraps the same functions Airflow calls, so a day can be reproduced on
a laptop exactly as the scheduler ran it. ``run`` strings the whole
extract -> load -> quality gate sequence together per day, which is how
backfills are done outside Airflow.
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import date, timedelta
from pathlib import Path

from la_aq.config import Settings
from la_aq.extract import Source, extract_day
from la_aq.load import connect, finish_run, init_warehouse, load_day, start_run
from la_aq.locations import load_locations
from la_aq.quality import DataQualityError, quality_gate
from la_aq.synthetic import land_synthetic_day

log = logging.getLogger("la_aq")


def _date(s: str) -> date:
    return date.fromisoformat(s)


def _days(start: date, end: date) -> list[date]:
    if end < start:
        raise SystemExit("--end must be on or after --start")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def _run_id(day: date) -> str:
    return f"cli__{day.isoformat()}__{uuid.uuid4().hex[:8]}"


def cmd_init_db(settings: Settings, _: argparse.Namespace) -> int:
    with connect(settings.warehouse_dsn) as conn:
        init_warehouse(conn, settings.init_sql)
    return 0


def cmd_extract(settings: Settings, args: argparse.Namespace) -> int:
    locations = load_locations(settings.locations_csv)
    sources = list(Source) if args.source == "all" else [Source.parse(args.source)]
    for day in _days(args.start, args.end or args.start):
        for source in sources:
            paths = extract_day(source, locations, day, settings)
            log.info("%s %s: landed %d files", source.value, day, len(paths))
    return 0


def cmd_synth(settings: Settings, args: argparse.Namespace) -> int:
    locations = load_locations(settings.locations_csv)
    for day in _days(args.start, args.end or args.start):
        paths = land_synthetic_day(locations, day, settings.raw_data_dir)
        log.info("%s: landed %d synthetic files", day, len(paths))
    return 0


def cmd_load(settings: Settings, args: argparse.Namespace) -> int:
    with connect(settings.warehouse_dsn) as conn:
        for day in _days(args.start, args.end or args.start):
            run_id = args.run_id or _run_id(day)
            start_run(conn, run_id, day, day)
            try:
                n = load_day(conn, settings.raw_data_dir, day, run_id)
            except Exception as exc:
                finish_run(conn, run_id, "failed", notes=str(exc))
                raise
            finish_run(conn, run_id, "succeeded", responses_loaded=n)
            log.info("%s: loaded %d responses (run_id=%s)", day, n, run_id)
    return 0


def cmd_quality(settings: Settings, args: argparse.Namespace) -> int:
    locations = load_locations(settings.locations_csv)
    ids = [loc.location_id for loc in locations]
    rc = 0
    with connect(settings.warehouse_dsn) as conn:
        for day in _days(args.start, args.end or args.start):
            run_id = args.run_id or _run_id(day)
            try:
                quality_gate(conn, run_id, day, day, ids)
            except DataQualityError as exc:
                log.error("%s: %s", day, exc)
                rc = 1
    return rc


def cmd_run(settings: Settings, args: argparse.Namespace) -> int:
    """extract -> load -> quality for each day; the local equivalent of one DAG run."""
    locations = load_locations(settings.locations_csv)
    ids = [loc.location_id for loc in locations]
    with connect(settings.warehouse_dsn) as conn:
        init_warehouse(conn, settings.init_sql)
        for day in _days(args.start, args.end or args.start):
            run_id = _run_id(day)
            start_run(conn, run_id, day, day)
            try:
                if args.synthetic:
                    land_synthetic_day(locations, day, settings.raw_data_dir)
                else:
                    for source in Source:
                        extract_day(source, locations, day, settings)
                n = load_day(conn, settings.raw_data_dir, day, run_id)
                quality_gate(conn, run_id, day, day, ids)
            except Exception as exc:
                finish_run(conn, run_id, "failed", notes=str(exc)[:500])
                log.error("%s failed: %s", day, exc)
                return 1
            finish_run(conn, run_id, "succeeded", responses_loaded=n)
            log.info("%s succeeded: %d responses loaded (run_id=%s)", day, n, run_id)
    log.info("raw layer up to date. Next: `dbt build` (see Makefile: make dbt-build)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="la-aq", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="create the raw schema/tables (idempotent)").set_defaults(
        func=cmd_init_db
    )

    def window(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--start", type=_date, required=True, help="first UTC day (YYYY-MM-DD)")
        sp.add_argument("--end", type=_date, help="last UTC day (default: --start)")

    s = sub.add_parser("extract", help="call Open-Meteo and land raw JSON")
    window(s)
    s.add_argument("--source", default="all", choices=["all", "weather", "air_quality"])
    s.set_defaults(func=cmd_extract)

    s = sub.add_parser(
        "synth", help="land synthetic (offline) responses instead of calling the API"
    )
    window(s)
    s.set_defaults(func=cmd_synth)

    s = sub.add_parser("load", help="load landed files into raw.api_responses")
    window(s)
    s.add_argument("--run-id")
    s.set_defaults(func=cmd_load)

    s = sub.add_parser("quality", help="run the raw-layer quality gate")
    window(s)
    s.add_argument("--run-id")
    s.set_defaults(func=cmd_quality)

    s = sub.add_parser(
        "run", help="extract + load + quality gate for a date range (local backfill)"
    )
    window(s)
    s.add_argument("--synthetic", action="store_true", help="use synthetic data (no network)")
    s.set_defaults(func=cmd_run)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    settings = Settings.from_env()
    Path(settings.raw_data_dir).mkdir(parents=True, exist_ok=True)
    return args.func(settings, args)


if __name__ == "__main__":
    sys.exit(main())
