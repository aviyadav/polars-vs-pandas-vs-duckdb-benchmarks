"""Gold modeling stage - analytical marts over the Silver dataset.

Mirrors duck_impl_v2/load_gold.py. DuckDB uses PRIMARY KEY + incremental
UPSERT/ON CONFLICT statements; over an append-only Silver dataset (each
``order_id`` appears at most once) the resulting state is identical to a
deterministic full recompute, so this stage runs three DataFusion SQL models
over the Silver Parquet dataset and writes them as copy-on-write files under
``storage/gold/``.
"""

import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from datafusion import SessionContext

PROJECT_ROOT = Path(__file__).resolve().parent

SILVER_DIR = PROJECT_ROOT / "storage" / "silver"
GOLD_DIR = PROJECT_ROOT / "storage" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)

FACT_PATH = GOLD_DIR / "fct_orders.parquet"
DIM_PATH = GOLD_DIR / "dim_users.parquet"
MART_PATH = GOLD_DIR / "daily_channel_performance.parquet"

FACT_SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.int64()),
        pa.field("user_id", pa.int64()),
        pa.field("event_date", pa.date32()),
        pa.field("country", pa.string()),
        pa.field("channel", pa.string()),
        pa.field("revenue", pa.decimal128(12, 2)),
        pa.field("silver_processed_at", pa.timestamp("ns")),
        pa.field("_loaded_at", pa.timestamp("ns")),
    ]
)
DIM_SCHEMA = pa.schema(
    [
        pa.field("user_id", pa.int64()),
        pa.field("first_active_date", pa.date32()),
        pa.field("last_active_date", pa.date32()),
        pa.field("lifetime_orders", pa.int64()),
        pa.field("lifetime_revenue", pa.decimal128(12, 2)),
        pa.field("_updated_at", pa.timestamp("ns")),
    ]
)
MART_SCHEMA = pa.schema(
    [
        pa.field("event_date", pa.date32()),
        pa.field("country", pa.string()),
        pa.field("channel", pa.string()),
        pa.field("total_orders", pa.int64()),
        pa.field("unique_customers", pa.int64()),
        pa.field("total_revenue", pa.decimal128(12, 2)),
        pa.field("avg_order_value", pa.decimal128(12, 2)),
        pa.field("_updated_at", pa.timestamp("ns")),
    ]
)


def collect_to_table(ctx: SessionContext, sql: str) -> pa.Table | None:
    """Execute a DataFusion SQL query and return a PyArrow table."""
    batches = ctx.sql(sql).collect()
    if not batches:
        return None
    return pa.Table.from_batches(batches)


def write_gold_file(table: pa.Table, schema: pa.Schema, audit_name: str, path: Path):
    """Attach the audit timestamp and persist the model to Parquet."""
    updated_at = datetime.now(UTC)
    audit_col = pa.array([updated_at] * table.num_rows, pa.timestamp("ns"))
    ordered = table.select(schema.names[:-1])
    final = pa.Table.from_arrays(list(ordered.columns) + [audit_col], schema=schema)
    pq.write_table(final, path, compression="snappy")
    print(f"  └─ {path.name}: {final.num_rows:,} rows")


print("Starting Gold Layer Pipeline...")

silver_files = sorted(SILVER_DIR.glob("*.parquet"))
if not silver_files:
    print("No silver files found in storage/silver. Run load_silver.py first.")
    sys.exit(1)

ctx = SessionContext()
ctx.register_parquet("silver_events", str(SILVER_DIR))

# ----------------------------------------------------------------------
# Model A: Fact Table - one row per order (QUALIFY picks newest version)
# ----------------------------------------------------------------------
print("Building Model A: fct_orders...")
fact_table = collect_to_table(
    ctx,
    """
    SELECT
        order_id,
        user_id,
        event_date,
        country,
        channel,
        revenue,
        _processed_at AS silver_processed_at
    FROM silver_events
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY _processed_at DESC, event_date DESC
    ) = 1
    """,
)
if fact_table is None or fact_table.num_rows == 0:
    pq.write_table(FACT_SCHEMA.empty_table(), FACT_PATH, compression="snappy")
    print(f"  └─ {FACT_PATH.name}: 0 rows")
else:
    write_gold_file(fact_table, FACT_SCHEMA, "_loaded_at", FACT_PATH)

# ----------------------------------------------------------------------
# Model B: User Dimension - lifetime aggregates per user
# ----------------------------------------------------------------------
print("Building Model B: dim_users...")
dim_table = collect_to_table(
    ctx,
    """
    SELECT
        user_id,
        MIN(event_date) AS first_active_date,
        MAX(event_date) AS last_active_date,
        COUNT(DISTINCT order_id) AS lifetime_orders,
        CAST(ROUND(SUM(revenue), 2) AS DECIMAL(12, 2)) AS lifetime_revenue
    FROM silver_events
    GROUP BY user_id
    """,
)
if dim_table is None or dim_table.num_rows == 0:
    pq.write_table(DIM_SCHEMA.empty_table(), DIM_PATH, compression="snappy")
    print(f"  └─ {DIM_PATH.name}: 0 rows")
else:
    write_gold_file(dim_table, DIM_SCHEMA, "_updated_at", DIM_PATH)

# ----------------------------------------------------------------------
# Model C: Aggregate Mart - daily performance by country and channel
# ----------------------------------------------------------------------
print("Building Model C: daily_channel_performance...")
mart_table = collect_to_table(
    ctx,
    """
    SELECT
        event_date,
        country,
        channel,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT user_id) AS unique_customers,
        CAST(ROUND(SUM(revenue), 2) AS DECIMAL(12, 2)) AS total_revenue,
        CAST(ROUND(AVG(revenue), 2) AS DECIMAL(12, 2)) AS avg_order_value
    FROM silver_events
    GROUP BY event_date, country, channel
    """,
)
if mart_table is None or mart_table.num_rows == 0:
    pq.write_table(MART_SCHEMA.empty_table(), MART_PATH, compression="snappy")
    print(f"  └─ {MART_PATH.name}: 0 rows")
else:
    write_gold_file(mart_table, MART_SCHEMA, "_updated_at", MART_PATH)


# ----------------------------------------------------------------------
# Summary metrics from Parquet footers
# ----------------------------------------------------------------------
def parquet_rows(path: Path) -> int:
    return pq.read_metadata(path).num_rows if path.exists() else 0


orders_count = parquet_rows(FACT_PATH)
users_count = parquet_rows(DIM_PATH)
marts_count = parquet_rows(MART_PATH)

print("Gold layer pipeline complete!")
print(f"  └─ Fact Orders Rows:            {orders_count:,}")
print(f"  └─ Dim Users Rows:              {users_count:,}")
print(f"  └─ Daily Channel Mart Rows:     {marts_count:,}")
