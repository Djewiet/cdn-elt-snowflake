"""Generate hourly Parquet files to a local folder (local dev / demo).

python -m cdn_logs.cli --start 2026-11-01T00 --end 2026-11-02T00 --out data/landing
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from cdn_logs.generator import generate_hour, iter_hours, partition_key
from cdn_logs.parquet import to_parquet_bytes


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, type=datetime.fromisoformat)
    parser.add_argument("--end", required=True, type=datetime.fromisoformat)
    parser.add_argument("--out", default="data/landing", type=Path)
    args = parser.parse_args(argv)

    total = 0
    for hour in iter_hours(args.start, args.end):
        records = generate_hour(hour)
        target = args.out / partition_key(hour)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(to_parquet_bytes(records))
        total += len(records)
        print(f"{hour:%Y-%m-%d %H:00}  {len(records):>6} rows  -> {target}")
    print(f"done: {total} rows")


if __name__ == "__main__":
    main()
