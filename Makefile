# Developer entrypoints. Run `make help` for the list.
SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
DBT     := cd dbt && dbt
# Default date range for local backfills: the last 7 completed UTC days.
START ?= $(shell date -u -d '7 days ago' +%F 2>/dev/null || date -u -v-7d +%F)
END   ?= $(shell date -u -d '1 day ago'  +%F 2>/dev/null || date -u -v-1d +%F)

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ------------------------------------------------------------------ docker
.PHONY: env up down nuke logs ps build
env: ## Create .env from the template if missing
	@test -f .env || (cp .env.example .env && echo "created .env - edit if needed")

build: env ## Build the Airflow image
	$(COMPOSE) build

up: env ## Start warehouse + Airflow (UI: http://localhost:8080, airflow/airflow)
	$(COMPOSE) up -d --build
	@echo "Airflow UI: http://localhost:8080  (user/pass from .env, default airflow/airflow)"

up-codespaces: env ## Start in GitHub Codespaces (host networking; use if Airflow can't reach its DB)
	$(COMPOSE) -f docker-compose.yml -f docker-compose.codespaces.yml up -d --build
	@echo "Airflow UI: forward port 8080 (Ports tab)"

dashboard: env ## Start the Streamlit dashboard (http://localhost:8501)
	$(COMPOSE) --profile dashboard up -d --build dashboard
	@echo "Dashboard: http://localhost:8501"

down: ## Stop containers (data volumes are kept)
	$(COMPOSE) --profile dashboard down

nuke: ## Stop containers AND delete all data volumes
	$(COMPOSE) --profile dashboard down -v --remove-orphans

logs: ## Tail Airflow logs
	$(COMPOSE) logs -f --tail=100 airflow-scheduler airflow-dag-processor

ps: ## Show container status
	$(COMPOSE) ps

# ------------------------------------------------------------------ pipeline
.PHONY: trigger backfill backfill-synthetic psql
trigger: ## Trigger one DAG run now (previous UTC day)
	$(COMPOSE) exec airflow-scheduler airflow dags trigger la_air_quality_daily

backfill: ## Backfill via Airflow (dates are *run* dates; each run loads the previous UTC day)
	$(COMPOSE) exec airflow-scheduler airflow backfill create \
		--dag-id la_air_quality_daily --from-date $(START) --to-date $(END) --max-active-runs 1

backfill-local: ## Backfill from the host without Airflow (real API): make backfill-local START=... END=...
	python -m la_aq run --start $(START) --end $(END)
	$(MAKE) dbt-build

backfill-synthetic: ## Same, but with generated data (no network needed)
	python -m la_aq run --synthetic --start $(START) --end $(END)
	$(MAKE) dbt-build

psql: ## Open a psql shell on the warehouse
	$(COMPOSE) exec warehouse psql -U $${WAREHOUSE_USER:-warehouse} -d $${WAREHOUSE_DB:-warehouse}

# ------------------------------------------------------------------ dbt (from host)
.PHONY: dbt-build dbt-run dbt-test dbt-docs dbt-freshness
dbt-build: ## dbt seed + run + test
	$(DBT) build --profiles-dir .

dbt-run: ## dbt run
	$(DBT) run --profiles-dir .

dbt-test: ## dbt test (schema, singular and unit tests)
	$(DBT) test --profiles-dir .

dbt-freshness: ## dbt source freshness
	$(DBT) source freshness --profiles-dir .

dbt-docs: ## Generate and serve dbt docs (lineage graph) on http://localhost:8081
	$(DBT) docs generate --profiles-dir . && $(DBT) docs serve --profiles-dir . --port 8081

# ------------------------------------------------------------------ quality
.PHONY: install lint fmt test test-unit test-integration
install: ## Install the package + dev tools into the current Python env
	pip install -e ".[dev]"

lint: ## ruff check + format check
	ruff check . && ruff format --check .

fmt: ## Auto-format
	ruff format . && ruff check --fix .

test-unit: ## Fast tests, no database needed
	pytest tests/unit

test-integration: ## Loader + quality gate against a live warehouse (needs WAREHOUSE_DSN)
	pytest tests/integration -m integration

test: test-unit ## Alias for test-unit
