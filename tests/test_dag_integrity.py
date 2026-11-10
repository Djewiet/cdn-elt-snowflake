"""DAG integrity tests: the DAG parses, is wired as designed, and its load SQL
is idempotent. Skipped automatically when Airflow is not installed."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_structure(dagbag):
    dag = dagbag.dags.get("cdn_elt_hourly")
    assert dag is not None
    assert dag.catchup is True
    assert dag.max_active_runs == 4
    upstream = {t.task_id: {u.task_id for u in t.upstream_list} for t in dag.tasks}
    assert upstream == {
        "extract_to_s3": set(),
        "load_to_snowflake": {"extract_to_s3"},
        "dbt_source_freshness": {"load_to_snowflake"},
        "dbt_build": {"dbt_source_freshness"},
    }


def test_runs_cover_exactly_one_hour(dagbag):
    """Guards against Airflow 3's CronTriggerTimetable default (zero-length
    intervals), which would turn every interval-scoped dbt run into a no-op."""
    from datetime import UTC, datetime, timedelta

    from airflow.timetables.base import TimeRestriction

    dag = dagbag.dags.get("cdn_elt_hourly")
    start = datetime(2026, 11, 8, tzinfo=UTC)
    first = dag.timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(earliest=start, latest=None, catchup=True),
    )
    assert first.data_interval.start == start
    assert first.data_interval.end - first.data_interval.start == timedelta(hours=1)
    second = dag.timetable.next_dagrun_info(
        last_automated_data_interval=first.data_interval,
        restriction=TimeRestriction(earliest=start, latest=None, catchup=True),
    )
    assert second.data_interval.start == first.data_interval.end


def test_dbt_tasks_are_serialized(dagbag):
    dag = dagbag.dags.get("cdn_elt_hourly")
    assert {t.pool for t in dag.tasks if t.task_id.startswith("dbt_")} == {"dbt"}


def test_retries_configured(dagbag):
    dag = dagbag.dags.get("cdn_elt_hourly")
    assert all(t.retries >= 3 for t in dag.tasks)


def test_load_sql_is_idempotent():
    from cdn_elt_hourly import copy_into_sql

    sql = copy_into_sql("cdn_logs/dt=2026-11-07/hour=14/cdn_logs_2026110714.parquet")
    stage_path = "dt=2026-11-07/hour=14/cdn_logs_2026110714.parquet"
    assert sql[0] == "begin"
    assert f"_source_file = '{stage_path}'" in sql[1] and sql[1].startswith("delete")
    assert f"files = ('{stage_path}')" in sql[2]
    assert "force = true" in sql[2]
