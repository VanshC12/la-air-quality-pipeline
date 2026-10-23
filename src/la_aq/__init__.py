"""la_aq: ingestion layer for the LA Air Quality data platform.

Responsibilities (in order of execution):

1. ``extract``  - call the Open-Meteo APIs and land verbatim JSON in a
                  Hive-partitioned local landing zone (``data/raw/<source>/dt=YYYY-MM-DD/``).
2. ``load``     - load landed files into ``raw.api_responses`` (Postgres, JSONB)
                  with an idempotent upsert on the natural key.
3. ``quality``  - run pre-transform data-quality checks against ``raw`` and
                  fail the run if a critical check fails (write-audit-publish).

Everything after that (staging, marts, tests) belongs to dbt.
"""

__version__ = "0.1.0"
