"""Serialize generated records to Parquet with an explicit, stable schema."""

from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq

from cdn_logs.generator import COLUMNS

# Naive microsecond timestamps = UTC. Snowflake loads them as TIMESTAMP_NTZ
# when the file format uses USE_LOGICAL_TYPE = TRUE.
SCHEMA = pa.schema(
    [
        ("request_id", pa.string()),
        ("event_ts", pa.timestamp("us")),
        ("edge_pop", pa.string()),
        ("client_ip_hash", pa.string()),
        ("client_country", pa.string()),
        ("http_method", pa.string()),
        ("host", pa.string()),
        ("path", pa.string()),
        ("http_status", pa.int16()),
        ("bytes_sent", pa.int64()),
        ("response_time_ms", pa.float64()),
        ("cache_status", pa.string()),
        ("content_type", pa.string()),
        ("user_agent", pa.string()),
    ]
)
assert tuple(SCHEMA.names) == COLUMNS, "Parquet schema out of sync with generator"


def to_table(records: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(records, schema=SCHEMA)


def to_parquet_bytes(records: list[dict]) -> bytes:
    buf = io.BytesIO()
    pq.write_table(to_table(records), buf, compression="snappy")
    return buf.getvalue()
