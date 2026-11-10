"""Hourly ELT of CDN access logs: generate → S3 (Parquet) → Snowflake RAW → dbt.

Design choices
--------------
* One DAG run == one hour of data (the run's data interval). The schedule is
  an explicit ``CronDataIntervalTimetable``: Airflow 3's default for cron
  strings (``CronTriggerTimetable``) gives a zero-length interval, which
  would make every interval-scoped dbt run a silent no-op. The source is
  deterministic per hour, the S3 key is derived from the interval and the
  Snowflake load replaces that file's rows inside a transaction, so every
  task is idempotent: retries and backfills never duplicate data.
* ``catchup=True`` + ``max_active_runs`` lets Airflow backfill history in
  parallel; dbt runs are serialized through a 1-slot pool and receive the
  interval as vars, so each run rebuilds exactly its own hour.
* The load task reconciles the row count reported by COPY INTO against the
  number of rows extracted and fails loudly on any mismatch.

Backfill example (Airflow 3 CLI):
    airflow backfill create --dag-id cdn_elt_hourly \
        --from-date 2026-11-01T00:00:00 --to-date 2026-11-03T00:00:00
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

log = logging.getLogger(__name__)

# ----------------------------------------------------------------- settings
AWS_CONN_ID = os.getenv("CDN_AWS_CONN_ID", "aws_cdn")
SNOWFLAKE_CONN_ID = os.getenv("CDN_SNOWFLAKE_CONN_ID", "snowflake_cdn")
LANDING_BUCKET = os.getenv("CDN_LANDING_BUCKET", "my-cdn-landing-bucket")
LANDING_PREFIX = os.getenv("CDN_LANDING_PREFIX", "cdn_logs")
RAW_TABLE = os.getenv("CDN_RAW_TABLE", "RAW.CDN.HTTP_LOGS")
RAW_STAGE = os.getenv("CDN_RAW_STAGE", "RAW.CDN.CDN_LOGS_STAGE")
RAW_FILE_FORMAT = os.getenv("CDN_RAW_FILE_FORMAT", "RAW.CDN.PARQUET_FF")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_BIN = os.getenv("DBT_BIN", "/opt/dbt-venv/bin/dbt")
START_DATE = datetime.fromisoformat(os.getenv("CDN_START_DATE", "2026-11-01T00:00:00+00:00"))

# Assets (Airflow 3 data-aware scheduling / lineage). Snowflake URIs need the account.
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT") or "local-account"
RAW_HTTP_LOGS = Asset(f"snowflake://{SNOWFLAKE_ACCOUNT}/raw/cdn/http_logs")
CDN_MARTS = Asset(f"snowflake://{SNOWFLAKE_ACCOUNT}/analytics/marts/fct_cdn_hourly_metrics")

DBT_VARS = (
    "{"
    '"interval_start": "{{ data_interval_start.strftime(\'%Y-%m-%d %H:%M:%S\') }}", '
    '"interval_end": "{{ data_interval_end.strftime(\'%Y-%m-%d %H:%M:%S\') }}"'
    "}"
)
DBT_ENV = {
    "DBT_TARGET": "prod",
    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    "DBT_LOG_PATH": "/tmp/dbt-logs",
    "DBT_TARGET_PATH": "/tmp/dbt-target",
}


def copy_into_sql(object_key: str) -> list[str]:
    """Statements that atomically (re)load one hourly file into RAW."""
    stage_path = object_key.removeprefix(f"{LANDING_PREFIX}/")
    return [
        "begin",
        f"delete from {RAW_TABLE} where _source_file = '{stage_path}'",
        f"""
        copy into {RAW_TABLE}
        from @{RAW_STAGE}
        files = ('{stage_path}')
        file_format = (format_name = '{RAW_FILE_FORMAT}')
        match_by_column_name = case_insensitive
        include_metadata = (_source_file = metadata$filename, _loaded_at = metadata$start_scan_time)
        force = true
        on_error = abort_statement
        """,
    ]


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="cdn_elt_hourly",
    description="Hourly CDN logs ELT: S3 landing → Snowflake RAW → dbt marts",
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    start_date=START_DATE,
    catchup=True,
    max_active_runs=4,
    default_args=default_args,
    tags=["cdn", "elt", "snowflake", "dbt"],
    doc_md=__doc__,
)
def cdn_elt_hourly():

    @task
    def extract_to_s3(
        data_interval_start: datetime | None = None, data_interval_end: datetime | None = None
    ) -> dict:
        """Generate the hour's logs and upload them as Parquet to S3."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        from cdn_logs.generator import generate_hour, partition_key
        from cdn_logs.parquet import to_parquet_bytes

        if data_interval_end - data_interval_start != timedelta(hours=1):
            raise ValueError(
                f"Expected a 1-hour data interval, got {data_interval_start} → {data_interval_end}"
            )
        records = generate_hour(data_interval_start)
        key = f"{LANDING_PREFIX}/{partition_key(data_interval_start)}"
        S3Hook(aws_conn_id=AWS_CONN_ID).load_bytes(
            to_parquet_bytes(records), key=key, bucket_name=LANDING_BUCKET, replace=True
        )
        log.info("Uploaded %s rows to s3://%s/%s", len(records), LANDING_BUCKET, key)
        return {"key": key, "rows": len(records)}

    @task(outlets=[RAW_HTTP_LOGS])
    def load_to_snowflake(extract: dict) -> int:
        """DELETE + COPY INTO in one transaction, then reconcile row counts."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()
        try:
            with conn.cursor() as cur:
                result = None
                for statement in copy_into_sql(extract["key"]):
                    cur.execute(statement)
                    if statement.lstrip().startswith("copy"):
                        columns = [c[0].lower() for c in cur.description]
                        result = [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
                loaded = sum(int(r.get("rows_loaded") or 0) for r in result or [])
                if loaded != extract["rows"]:
                    raise ValueError(
                        f"Row count mismatch for {extract['key']}: "
                        f"extracted={extract['rows']} loaded={loaded} copy_result={result}"
                    )
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        log.info("Loaded %s rows into %s", loaded, RAW_TABLE)
        return loaded

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} source freshness --select source:cdn",
        env=DBT_ENV,
        append_env=True,
        pool="dbt",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        # Staging → intermediate → dims/facts → reporting, with all their tests.
        # Incremental facts only rebuild this run's hour (incremental_window
        # macro). Seeds are loaded once at bootstrap (`make seed`).
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && {DBT_BIN} build "
            "--select +rpt_pop_daily_health +fct_cdn_hourly_metrics "
            "--exclude resource_type:seed "
            f"--vars '{DBT_VARS}'"
        ),
        env=DBT_ENV,
        append_env=True,
        pool="dbt",
        outlets=[CDN_MARTS],
    )

    load_to_snowflake(extract_to_s3()) >> dbt_source_freshness >> dbt_build


cdn_elt_hourly()
