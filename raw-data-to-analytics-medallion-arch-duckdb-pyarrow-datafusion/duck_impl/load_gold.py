from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "storage" / "warehouse.duckdb"

con = duckdb.connect(str(DB_PATH))

# 1. Create Gold Schema
con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

# ----------------------------------------------------------------------
# Model A: Aggregate Data Mart (Daily Performance)
# ----------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE gold.daily_channel_performance AS
    SELECT 
        event_date,
        country,
        channel,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT user_id) AS unique_customers,
        ROUND(SUM(revenue), 2) AS total_revenue,
        ROUND(AVG(revenue), 2) AS avg_order_value,
        CURRENT_TIMESTAMP AS _updated_at
    FROM silver.events
    GROUP BY 
        event_date,
        country,
        channel
    ORDER BY 
        event_date DESC, 
        total_revenue DESC;
""")

# ----------------------------------------------------------------------
# Model B: Dimensional Star Schema (Fact Table)
# ----------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE gold.fct_orders AS
    SELECT 
        order_id,
        user_id,
        event_date,
        country,
        channel,
        revenue,
        _processed_at AS silver_processed_at,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM silver.events;
""")

# ----------------------------------------------------------------------
# Model C: Dimensional Star Schema (User Dimension)
# ----------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE gold.dim_users AS
    SELECT 
        user_id,
        MIN(event_date) AS first_active_date,
        MAX(event_date) AS last_active_date,
        COUNT(DISTINCT order_id) AS lifetime_orders,
        ROUND(SUM(revenue), 2) AS lifetime_revenue,
        CURRENT_TIMESTAMP AS _updated_at
    FROM silver.events
    GROUP BY user_id;
""")

# Print Summary Metrics
marts_count = con.execute(
    "SELECT COUNT(*) FROM gold.daily_channel_performance"
).fetchone()[0]
users_count = con.execute("SELECT COUNT(*) FROM gold.dim_users").fetchone()[0]
orders_count = con.execute("SELECT COUNT(*) FROM gold.fct_orders").fetchone()[0]

print("Gold layer complete!")
print(f"Daily Channel Mart Rows: {marts_count:,}")
print(f"Fact Orders Rows:        {orders_count:,}")
print(f"Dim Users Rows:         {users_count:,}")
