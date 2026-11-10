from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest

from cdn_logs.generator import COLUMNS, EDGE_POPS, generate_hour, iter_hours, partition_key
from cdn_logs.parquet import SCHEMA, to_parquet_bytes

HOUR = datetime(2026, 11, 7, 14)


@pytest.fixture(scope="module")
def records():
    return generate_hour(HOUR)


def test_is_deterministic(records):
    assert generate_hour(HOUR) == records


def test_tz_aware_input_is_normalized(records):
    assert generate_hour(datetime(2026, 11, 7, 14, 37, tzinfo=UTC)) == records


def test_different_hours_differ(records):
    assert generate_hour(datetime(2026, 11, 7, 15))[0]["request_id"] != records[0]["request_id"]


def test_records_stay_inside_the_hour(records):
    assert all(HOUR <= r["event_ts"] < datetime(2026, 11, 7, 15) for r in records)


def test_schema_and_values(records):
    assert set(records[0]) == set(COLUMNS)
    assert len({r["request_id"] for r in records}) == len(records)
    assert {r["edge_pop"] for r in records} == {p.code for p in EDGE_POPS}
    assert {r["cache_status"] for r in records} <= {"HIT", "MISS", "EXPIRED", "BYPASS"}
    assert all(r["bytes_sent"] > 0 and r["response_time_ms"] > 0 for r in records)


def test_realistic_cache_hit_ratio(records):
    cacheable = [r for r in records if r["cache_status"] != "BYPASS"]
    ratio = sum(r["cache_status"] == "HIT" for r in cacheable) / len(cacheable)
    assert 0.70 < ratio < 0.95


def test_parquet_roundtrip(records):
    table = pq.read_table(__import__("io").BytesIO(to_parquet_bytes(records)))
    assert table.schema.equals(SCHEMA)
    assert table.num_rows == len(records)


def test_partitioning_helpers():
    assert partition_key(HOUR) == "dt=2026-11-07/hour=14/cdn_logs_2026110714.parquet"
    hours = list(iter_hours(datetime(2026, 11, 7, 22, 30), datetime(2026, 11, 8, 1)))
    assert [h.hour for h in hours] == [22, 23, 0]
