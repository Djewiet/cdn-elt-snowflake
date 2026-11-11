.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose
DBT_LOCAL := cd dbt && DBT_TARGET=local dbt
RUN_DBT := $(COMPOSE) exec airflow-scheduler bash -c

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ----------------------------------------------------------------- dev setup
install: ## Create the local dev environment (Python venv, dbt packages, pre-commit)
	python -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]" pre-commit
	. .venv/bin/activate && cd dbt && dbt deps
	. .venv/bin/activate && pre-commit install

keys: ## Generate RSA key pairs for the two Snowflake service users in ./secrets
	@mkdir -p secrets && chmod 700 secrets
	@for u in loader transformer; do \
	  openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -inform PEM -out secrets/$${u}_rsa_key.p8 -nocrypt; \
	  openssl rsa -in secrets/$${u}_rsa_key.p8 -pubout -out secrets/$${u}_rsa_key.pub 2>/dev/null; \
	  echo "== $$u public key (paste into snowflake/01_account_setup.sql) =="; \
	  grep -v -- '-----' secrets/$${u}_rsa_key.pub | tr -d '\n'; echo; \
	done
	@chmod 600 secrets/*

# ------------------------------------------------------------- quality gates
lint: ## Ruff (Python) + SQLFluff (dbt SQL)
	ruff check . && ruff format --check .
	cd dbt && sqlfluff lint models tests macros

test: ## Unit tests (generator + DAG integrity)
	pytest -q

local-data: ## Load 72 hours of synthetic logs into a local DuckDB file
	python scripts/load_local_duckdb.py --hours 72

local-build: local-data ## Run the whole dbt project offline on DuckDB
	$(DBT_LOCAL) build

dashboard-local: ## Run the Streamlit dashboard on the offline DuckDB build (after local-build)
	CDN_DASHBOARD_DUCKDB=data/cdn_local.duckdb streamlit run streamlit/app.py

# ------------------------------------------------------------------- airflow
up: ## Build the image, initialise Airflow and start the stack
	@test -f .env || (cp .env.example .env && echo "Created .env — fill it in" && exit 1)
	@grep -q '^AIRFLOW_UID=' .env && sed -i.bak "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u)/" .env && rm -f .env.bak
	$(COMPOSE) up -d --build
	@echo "Airflow UI: http://localhost:8080"

seed: ## Load dbt seeds and dimensions in Snowflake (run once after `up`)
	$(RUN_DBT) "cd /opt/airflow/dbt && /opt/dbt-venv/bin/dbt build --target prod --select resource_type:seed dim_date dim_edge_pop dim_http_status --indirect-selection cautious"

backfill: ## Backfill a range, e.g. make backfill FROM=2026-11-01T00:00:00 TO=2026-11-02T00:00:00
	$(COMPOSE) exec airflow-scheduler airflow backfill create --dag-id cdn_elt_hourly \
	  --from-date $(FROM) --to-date $(TO) --max-active-runs 4

docs: ## Generate dbt docs (static single file) from Snowflake
	$(RUN_DBT) "cd /opt/airflow/dbt && /opt/dbt-venv/bin/dbt docs generate --target prod --static --target-path /opt/airflow/dbt/target"
	@echo "Open dbt/target/static_index.html"

down: ## Stop the stack
	$(COMPOSE) down

clean: ## Stop the stack and delete volumes + local artefacts
	$(COMPOSE) down -v
	rm -rf dbt/target dbt/dbt_packages dbt/logs data/*.duckdb

.PHONY: help install keys lint test local-data local-build dashboard-local up seed backfill docs down clean
