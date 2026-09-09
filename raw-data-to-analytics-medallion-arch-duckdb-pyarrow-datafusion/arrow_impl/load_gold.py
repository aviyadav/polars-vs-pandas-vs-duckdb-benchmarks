import shutil
import time
from pathlib import Path

import pyarrow.parquet as pq
from datafusion import SessionContext

# Resolve project paths dynamically
PROJECT_ROOT = Path(__file__).resolve().parent

SILVER_PARQUET_PATH = PROJECT_ROOT / "storage" / "silver" / "events.parquet"
GOLD_DIR = PROJECT_ROOT / "storage" / "gold"

MART_PATH = GOLD_DIR / "daily_channel_performance.parquet"
FACT_PATH = GOLD_DIR / "fct_orders.parquet"
DIM_PATH = GOLD_DIR / "dim_users.parquet"

# Ensure gold storage directory exists
GOLD_DIR.mkdir(parents=True, exist_ok=True)

if not SILVER_PARQUET_PATH.exists():
    raise FileNotFoundError(
        f"Silver dataset not found at {SILVER_PARQUET_PATH}. "
        "Please execute 'load_silver.py' first."
    )

print("Initializing Gold layer modeling (Data Marts & Dimensional Star Schema)...")
total_start = time.time()

# 1. Initialize Apache DataFusion SessionContext
ctx = SessionContext()

# 2. Register Cleaned Silver Table
ctx.register_parquet("silver_events", str(SILVER_PARQUET_PATH))


def safe_write_parquet(df, path: Path):
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    df.write_parquet(str(path), compression="snappy")


# ----------------------------------------------------------------------
# Model A: Aggregate Data Mart (Daily Performance)
# ----------------------------------------------------------------------
print(f"Building Model A: Aggregate Data Mart -> {MART_PATH.name}...")
t0 = time.time()
mart_query = """
    SELECT 
        event_date,
        country,
        channel,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT user_id) AS unique_customers,
        ROUND(SUM(revenue), 2) AS total_revenue,
        ROUND(AVG(revenue), 2) AS avg_order_value,
        CURRENT_TIMESTAMP() AS _updated_at
    FROM silver_events
    GROUP BY 
        event_date,
        country,
        channel
    ORDER BY 
        event_date DESC, 
        total_revenue DESC
"""
df_mart = ctx.sql(mart_query)
safe_write_parquet(df_mart, MART_PATH)
print(f"  Completed in {time.time() - t0:.2f}s")

# ----------------------------------------------------------------------
# Model B: Dimensional Star Schema (Fact Table: Orders)
# ----------------------------------------------------------------------
print(f"Building Model B: Fact Table -> {FACT_PATH.name}...")
t0 = time.time()
fact_query = """
    SELECT 
        order_id,
        user_id,
        event_date,
        country,
        channel,
        revenue,
        _processed_at AS silver_processed_at,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM silver_events
"""
df_fact = ctx.sql(fact_query)
safe_write_parquet(df_fact, FACT_PATH)
print(f"  Completed in {time.time() - t0:.2f}s")

# ----------------------------------------------------------------------
# Model C: Dimensional Star Schema (Dimension Table: Users)
# ----------------------------------------------------------------------
print(f"Building Model C: Dimension Table -> {DIM_PATH.name}...")
t0 = time.time()
dim_query = """
    SELECT 
        user_id,
        MIN(event_date) AS first_active_date,
        MAX(event_date) AS last_active_date,
        COUNT(DISTINCT order_id) AS lifetime_orders,
        ROUND(SUM(revenue), 2) AS lifetime_revenue,
        CURRENT_TIMESTAMP() AS _updated_at
    FROM silver_events
    GROUP BY user_id
"""
df_dim = ctx.sql(dim_query)
safe_write_parquet(df_dim, DIM_PATH)
print(f"  Completed in {time.time() - t0:.2f}s")

total_elapsed = time.time() - total_start

# Print Summary Metrics
mart_rows = pq.read_metadata(MART_PATH).num_rows
fact_rows = pq.read_metadata(FACT_PATH).num_rows
dim_rows = pq.read_metadata(DIM_PATH).num_rows

print("\nGold layer completed successfully!")
print(
    f"  Daily Channel Mart Rows: {mart_rows:,} ({MART_PATH.stat().st_size / (1024 * 1024):.2f} MB)"
)
print(
    f"  Fact Orders Rows:        {fact_rows:,} ({FACT_PATH.stat().st_size / (1024 * 1024):.2f} MB)"
)
print(
    f"  Dim Users Rows:          {dim_rows:,} ({DIM_PATH.stat().st_size / (1024 * 1024):.2f} MB)"
)
print(f"  Total Gold Runtime:      {total_elapsed:.2f} seconds")
