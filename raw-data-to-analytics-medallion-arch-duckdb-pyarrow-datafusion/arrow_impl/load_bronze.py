import shutil
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from datafusion import SessionContext

# Resolve project paths dynamically
PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parent

# Check data CSV location (central repo data or local arrow_impl data)
CSV_PATH = REPO_ROOT / "data" / "events_arr.csv"
if not CSV_PATH.exists():
    CSV_PATH = PROJECT_ROOT / "data" / "events_arr.csv"

BRONZE_DIR = PROJECT_ROOT / "storage" / "bronze"
BRONZE_PARQUET_PATH = BRONZE_DIR / "events.parquet"

# Ensure storage directory exists
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

if not CSV_PATH.exists():
    raise FileNotFoundError(
        f"Raw dataset not found at {CSV_PATH}. "
        "Please run 'python generate_dirty_data.py' first."
    )

print(f"Initializing Bronze ingestion from: {CSV_PATH}")
start_time = time.time()

# 1. Initialize Apache DataFusion SessionContext
ctx = SessionContext()

# 2. Define schema with all columns as Utf8 (String) for resilient ingestion
# This prevents premature type casting errors on dirty/corrupt data
bronze_schema = pa.schema(
    [
        ("event_date", pa.string()),
        ("country", pa.string()),
        ("channel", pa.string()),
        ("user_id", pa.string()),
        ("order_id", pa.string()),
        ("revenue", pa.string()),
    ]
)

# 3. Register CSV with all_varchar equivalent schema
ctx.register_csv("raw_events_csv", str(CSV_PATH), schema=bronze_schema)

# 4. Transform to bronze table with audit lineage metadata
bronze_query = f"""
    SELECT
        event_date,
        country,
        channel,
        user_id,
        order_id,
        revenue,
        CURRENT_TIMESTAMP() AS _ingested_at,
        '{CSV_PATH.name}' AS _source_file
    FROM raw_events_csv
"""

print(f"Loading raw records into Bronze Parquet: {BRONZE_PARQUET_PATH}...")
df_bronze = ctx.sql(bronze_query)

# Remove existing target if present to allow clean overwrite
if BRONZE_PARQUET_PATH.exists():
    if BRONZE_PARQUET_PATH.is_dir():
        shutil.rmtree(BRONZE_PARQUET_PATH)
    else:
        BRONZE_PARQUET_PATH.unlink()

# Write columnar Parquet (compressed with snappy)
df_bronze.write_parquet(str(BRONZE_PARQUET_PATH), compression="snappy")

elapsed = time.time() - start_time

# Retrieve row count instantaneously from Parquet metadata footer
metadata = pq.read_metadata(BRONZE_PARQUET_PATH)
row_count = metadata.num_rows
file_size_mb = BRONZE_PARQUET_PATH.stat().st_size / (1024 * 1024)

print("Bronze layer loaded successfully!")
print(f"  Rows Ingested:    {row_count:,}")
print(f"  Output Parquet:   {BRONZE_PARQUET_PATH} ({file_size_mb:.2f} MB)")
print(f"  Execution Time:   {elapsed:.2f} seconds")
