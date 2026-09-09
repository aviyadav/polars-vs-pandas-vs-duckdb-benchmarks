from pathlib import Path

import duckdb

# Resolve project paths dynamically
PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "storage" / "warehouse.duckdb"

# Connect to persistent DuckDB database
con = duckdb.connect(str(DB_PATH))

# Ensure Gold Schema exists
con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

print("Starting Gold Layer Pipeline...")


def ensure_table_with_pk(table_name: str, create_sql: str):
    """
    Checks if a table has a PRIMARY KEY constraint. If missing (e.g. from prior runs),
    drops and recreates the schema to avoid DuckDB ON CONFLICT/PK errors.
    """
    schema_name, tbl_name = table_name.split(".")
    has_pk = (
        con.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.table_constraints
        WHERE table_schema = '{schema_name}'
          AND table_name = '{tbl_name}'
          AND constraint_type = 'PRIMARY KEY';
    """).fetchone()[0]
        > 0
    )

    if not has_pk:
        con.execute(f"DROP TABLE IF EXISTS {table_name};")

    con.execute(create_sql)


# ----------------------------------------------------------------------
# Model A: Fact Table (Incremental Append with QUALIFY Deduplication)
# ----------------------------------------------------------------------
ensure_table_with_pk(
    "gold.fct_orders",
    """
    CREATE TABLE IF NOT EXISTS gold.fct_orders (
        order_id BIGINT PRIMARY KEY,
        user_id BIGINT,
        event_date DATE,
        country VARCHAR,
        channel VARCHAR,
        revenue DECIMAL(12, 2),
        silver_processed_at TIMESTAMP,
        _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""",
)

# Get last loaded watermark from Silver
last_loaded_silver = con.execute(
    "SELECT MAX(silver_processed_at) FROM gold.fct_orders"
).fetchone()[0]

con.execute(
    """
    INSERT INTO gold.fct_orders
    WITH deduplicated_silver AS (
        SELECT
            order_id,
            user_id,
            event_date,
            country,
            channel,
            revenue,
            _processed_at
        FROM silver.events
        WHERE (? IS NULL OR _processed_at > ?)
        -- QUALIFY ensures batch-level idempotency by picking 1 record per order_id
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _processed_at DESC, event_date DESC
        ) = 1
    )
    SELECT
        s.order_id,
        s.user_id,
        s.event_date,
        s.country,
        s.channel,
        s.revenue,
        s._processed_at AS silver_processed_at,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM deduplicated_silver s
    LEFT JOIN gold.fct_orders f ON s.order_id = f.order_id
    WHERE f.order_id IS NULL;""",
    [last_loaded_silver, last_loaded_silver],
)


# ----------------------------------------------------------------------
# Model B: User Dimension (UPSERT via ON CONFLICT)
# ----------------------------------------------------------------------
ensure_table_with_pk(
    "gold.dim_users",
    """
    CREATE TABLE IF NOT EXISTS gold.dim_users (
        user_id BIGINT PRIMARY KEY,
        first_active_date DATE,
        last_active_date DATE,
        lifetime_orders BIGINT,
        lifetime_revenue DECIMAL(12, 2),
        _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""",
)

con.execute("""
    INSERT INTO gold.dim_users
    SELECT
        user_id,
        MIN(event_date) AS first_active_date,
        MAX(event_date) AS last_active_date,
        COUNT(DISTINCT order_id) AS lifetime_orders,
        ROUND(SUM(revenue), 2) AS lifetime_revenue,
        CURRENT_TIMESTAMP AS _updated_at
    FROM silver.events
    GROUP BY user_id
    ON CONFLICT (user_id) DO UPDATE SET
        first_active_date = LEAST(gold.dim_users.first_active_date, EXCLUDED.first_active_date),
        last_active_date  = GREATEST(gold.dim_users.last_active_date, EXCLUDED.last_active_date),
        lifetime_orders   = EXCLUDED.lifetime_orders,
        lifetime_revenue  = EXCLUDED.lifetime_revenue,
        _updated_at       = EXCLUDED._updated_at;
""")
# ----------------------------------------------------------------------
# Model C: Aggregate Data Mart (Daily Channel Performance UPSERT)
# ----------------------------------------------------------------------
ensure_table_with_pk(
    "gold.daily_channel_performance",
    """
    CREATE TABLE IF NOT EXISTS gold.daily_channel_performance (
        event_date DATE,
        country VARCHAR,
        channel VARCHAR,
        total_orders BIGINT,
        unique_customers BIGINT,
        total_revenue DECIMAL(12, 2),
        avg_order_value DECIMAL(12, 2),
        _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (event_date, country, channel)
    );
""",
)

con.execute("""
    INSERT INTO gold.daily_channel_performance
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
    ON CONFLICT (event_date, country, channel) DO UPDATE SET
        total_orders     = EXCLUDED.total_orders,
        unique_customers = EXCLUDED.unique_customers,
        total_revenue    = EXCLUDED.total_revenue,
        avg_order_value  = EXCLUDED.avg_order_value,
        _updated_at      = EXCLUDED._updated_at;
""")


# Print Output Metrics
orders_count = con.execute("SELECT COUNT(*) FROM gold.fct_orders").fetchone()[0]
users_count = con.execute("SELECT COUNT(*) FROM gold.dim_users").fetchone()[0]
marts_count = con.execute(
    "SELECT COUNT(*) FROM gold.daily_channel_performance"
).fetchone()[0]

print("Gold layer pipeline complete!")
print(f"  └─ Fact Orders Rows:            {orders_count:,}")
print(f"  └─ Dim Users Rows:              {users_count:,}")
print(f"  └─ Daily Channel Mart Rows:     {marts_count:,}")
