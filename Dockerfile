ARG AIRFLOW_VERSION=3.4.0
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG DBT_SNOWFLAKE_VERSION=1.12.5

# The base image runs as the unprivileged `airflow` user: prepare the folders
# we install into as root, then hand them over.
USER root
RUN mkdir -p /opt/dbt-venv /opt/cdn_logs \
 && chown -R airflow:root /opt/dbt-venv /opt/cdn_logs
USER airflow

# Airflow providers, pinned against the official constraints file.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt lives in its own virtualenv so its dependencies never conflict with Airflow's.
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-snowflake==${DBT_SNOWFLAKE_VERSION}"

# Source-system package (log generator), importable from DAG tasks.
COPY --chown=airflow:root pyproject.toml /opt/cdn_logs/pyproject.toml
COPY --chown=airflow:root src /opt/cdn_logs/src
RUN pip install --no-cache-dir --no-deps /opt/cdn_logs
