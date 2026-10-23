# Airflow image extended with dbt and the ingestion package's dependencies.
# One image runs every Airflow component (api-server, scheduler, dag-processor,
# triggerer), so the same Python environment executes the DAG and the dbt CLI.
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Dependencies only (the source itself is bind-mounted in docker-compose so
# edits are picked up without a rebuild; in a real deployment you'd COPY src/).
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONPATH="/opt/airflow/src" \
    DBT_PROJECT_DIR="/opt/airflow/dbt" \
    DBT_PROFILES_DIR="/opt/airflow/dbt"
