"""Load N hours of generated logs into a local DuckDB file, mimicking the
Snowflake raw layer (RAW.CDN.HTTP_LOGS) so the dbt project runs offline.

    python scripts/load_local_duckdb.py --hours 48
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from cdn_logs.generator import generate_hour, iter_hours, partition_key
from cdn_logs.parquet import to_table

DDL = """
create schema if not exists raw_cdn;
create table if not exists raw_cdn.http_logs (
    request_id varchar, event_ts timestamp, edge_pop varchar, client_ip_hash varchar,
    client_country varchar, http_method varchar, host varchar, path varchar,
    http_status smallint, bytes_sent bigint, response_time_ms double,
    cache_status varchar, content_type varchar, user_agent varchar,
    _source_file varchar, _loaded_at timestamp
);
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=48)
    parser.add_argument(
        "--end",
        type=datetime.fromisoformat,
        default=datetime.now(UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0),
    )
    parser.add_argument("--db", type=Path, default=Path("data/cdn_local.duckdb"))
    args = parser.parse_args()

    args.db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db))
    con.execute(DDL)

    start = args.end - timedelta(hours=args.hours)
    for hour in iter_hours(start, args.end):
        key = partition_key(hour)
        batch = to_table(generate_hour(hour))  # noqa: F841 - referenced by DuckDB below
        # Same contract as the Airflow load task: replace the hour atomically.
        con.execute("begin")
        con.execute("delete from raw_cdn.http_logs where _source_file = ?", [key])
        con.execute(
            "insert into raw_cdn.http_logs select *, ? as _source_file, now()::timestamp from batch",
            [key],
        )
        con.execute("commit")
    total = con.execute("select count(*) from raw_cdn.http_logs").fetchone()[0]
    print(f"loaded {args.hours} hours into {args.db} (raw_cdn.http_logs = {total} rows)")


if __name__ == "__main__":
    main()
