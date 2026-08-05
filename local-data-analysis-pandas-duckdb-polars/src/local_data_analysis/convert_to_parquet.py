"""
Convert events.csv to compressed Parquet format.

Reads data/events.csv in a streaming fashion and writes
data/events.parquet using zstd compression.
"""

import time
from pathlib import Path

import polars as pl

DATA_DIR = Path("data")
CSV_PATH = DATA_DIR / "events.csv"
PARQUET_PATH = DATA_DIR / "events.parquet"

t0 = time.perf_counter()

# scan_csv is lazy — rows are streamed through without full materialisation
(
    pl.scan_csv(CSV_PATH, try_parse_dates=True)
    .sink_parquet(
        PARQUET_PATH,
        compression="zstd",
        compression_level=3,
    )
)

elapsed = time.perf_counter() - t0

csv_mb = CSV_PATH.stat().st_size / (1024 * 1024)
pq_mb = PARQUET_PATH.stat().st_size / (1024 * 1024)

print(f"CSV      : {csv_mb:.2f} MB  →  {CSV_PATH.resolve()}")
print(f"Parquet  : {pq_mb:.2f} MB  →  {PARQUET_PATH.resolve()}")
print(f"Ratio    : {csv_mb / pq_mb:.1f}x smaller")
print(f"Time     : {elapsed:.3f} s")
