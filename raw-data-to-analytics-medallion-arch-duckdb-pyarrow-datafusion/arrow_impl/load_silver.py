import shutil
import time
from pathlib import Path

import pyarrow.parquet as pq
from datafusion import SessionContext

# Resolve project paths dynamically
PROJECT_ROOT = Path(__file__).resolve().parent

BRONZE_PARQUET_PATH = PROJECT_ROOT / "storage" / "bronze" / "events.parquet"
SILVER_DIR = PROJECT_ROOT / "storage" / "silver"
SILVER_PARQUET_PATH = SILVER_DIR / "events.parquet"

# Ensure storage directory exists
SILVER_DIR.mkdir(parents=True, exist_ok=True)

if not BRONZE_PARQUET_PATH.exists():
    raise FileNotFoundError(
        f"Bronze dataset not found at {BRONZE_PARQUET_PATH}. "
        "Please execute 'load_bronze.py' first."
    )

print(
    "Initializing Silver layer processing (cleaning, type casting & quality gates)..."
)
start_time = time.time()

# 1. Initialize Apache DataFusion SessionContext
ctx = SessionContext()

# 2. Register Bronze Parquet table
ctx.register_parquet("bronze_events", str(BRONZE_PARQUET_PATH))

# 3. Clean, Cast, and Filter via DataFusion SQL
silver_query = """
WITH cleaned AS (
    SELECT
        -- Safe date parsing
        TRY_CAST(event_date AS DATE) AS event_date,

        -- Standardize country casing
        UPPER(TRIM(country)) AS country,

        -- Standardize marketing channel casing
        LOWER(TRIM(channel)) AS channel,

        -- Type cast identifier fields
        TRY_CAST(user_id AS BIGINT) AS user_id,
        TRY_CAST(order_id AS BIGINT) AS order_id,

        -- Strip non-numeric artifacts (e.g. '$', text tokens) before decimal conversion
        TRY_CAST(
            REGEXP_REPLACE(revenue, '[^0-9.-]', '', 'g') AS DECIMAL(12, 2)
        ) AS revenue,

        -- Retain ingestion lineage & assign processing timestamp
        _ingested_at,
        _source_file,
        CURRENT_TIMESTAMP() AS _processed_at
    FROM bronze_events
)
SELECT 
    event_date,
    country,
    channel,
    user_id,
    order_id,
    revenue,
    _ingested_at,
    _source_file,
    _processed_at
FROM cleaned
-- Data Quality Enforcement & Business Logic Range Gates
WHERE event_date IS NOT NULL
  -- Exclude out-of-bounds / sentinel dates
  AND event_date BETWEEN DATE '2024-01-01' AND DATE '2026-12-31'

  -- Categorical domain validation
  AND country IN ('US', 'UK', 'DE', 'FR', 'IN', 'JP')
  AND channel IN ('search', 'social', 'email', 'direct')

  -- Key identifier integrity checks
  AND user_id IS NOT NULL AND user_id > 0
  AND order_id IS NOT NULL AND order_id > 0

  -- Revenue range gates (filters negative charges, invalid text, and excessive outliers)
  AND revenue IS NOT NULL AND revenue >= 0.0 AND revenue <= 10000.0
"""

print(
    f"Applying transformation rules and writing to Silver Parquet: {SILVER_PARQUET_PATH}..."
)
df_silver = ctx.sql(silver_query)

# Clean target path if it already exists
if SILVER_PARQUET_PATH.exists():
    if SILVER_PARQUET_PATH.is_dir():
        shutil.rmtree(SILVER_PARQUET_PATH)
    else:
        SILVER_PARQUET_PATH.unlink()

# Write cleansed dataset to Parquet
df_silver.write_parquet(str(SILVER_PARQUET_PATH), compression="snappy")

elapsed = time.time() - start_time

# Metadata analysis
bronze_rows = pq.read_metadata(BRONZE_PARQUET_PATH).num_rows
silver_rows = pq.read_metadata(SILVER_PARQUET_PATH).num_rows
filtered_rows = bronze_rows - silver_rows
file_size_mb = SILVER_PARQUET_PATH.stat().st_size / (1024 * 1024)

print("Silver layer completed successfully!")
print(f"  Bronze Source Rows:  {bronze_rows:,}")
print(f"  Silver Clean Rows:   {silver_rows:,}")
print(
    f"  Filtered Anomalies:  {filtered_rows:,} ({(filtered_rows / bronze_rows) * 100:.2f}%)"
)
print(f"  Output Parquet:      {SILVER_PARQUET_PATH} ({file_size_mb:.2f} MB)")
print(f"  Execution Time:      {elapsed:.2f} seconds")
