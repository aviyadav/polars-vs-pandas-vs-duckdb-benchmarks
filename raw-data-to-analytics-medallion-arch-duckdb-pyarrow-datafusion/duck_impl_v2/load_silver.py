from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent

DB_PATH = PROJECT_ROOT / "storage" / "warehouse.duckdb"

con = duckdb.connect(str(DB_PATH))
con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

# 1. Initialize Silver table structure if it doesn't exist
con.execute("""
    CREATE TABLE IF NOT EXISTS silver.events (
        event_date DATE,
        country VARCHAR,
        channel VARCHAR,
        user_id BIGINT,
        order_id BIGINT,
        revenue DECIMAL(12, 2),
        _ingested_at TIMESTAMP,
        _source_file VARCHAR,
        _processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")

# 2. Get high-water mark for incremental execution
last_ingested = con.execute("SELECT MAX(_ingested_at) FROM silver.events").fetchone()[0]
watermark_filter = "WHERE _ingested_at > ?" if last_ingested else "WHERE ? IS NULL"

# 3. Clean and incrementally append missing records
con.execute(
    f"""
    INSERT INTO silver.events
    WITH cleaned AS (
        SELECT
            TRY_CAST(event_date AS DATE) AS event_date,
            UPPER(TRIM(country)) AS country,
            LOWER(TRIM(channel)) AS channel,
            TRY_CAST(user_id AS BIGINT) AS user_id,
            TRY_CAST(order_id AS BIGINT) AS order_id,
            TRY_CAST(REGEXP_REPLACE(revenue, '[^0-9.-]', '', 'g') AS DECIMAL(12, 2)) AS revenue,
            _ingested_at,
            _source_file
        FROM bronze.events
        {watermark_filter}
    ),
    validated AS (
        SELECT * FROM cleaned
        WHERE event_date BETWEEN DATE '2024-01-01' AND DATE '2026-12-31'
          AND country IN ('US', 'UK', 'DE', 'FR', 'IN', 'JP')
          AND channel IN ('search', 'social', 'email', 'direct')
          AND user_id > 0
          AND order_id > 0          AND revenue BETWEEN 0.0 AND 10000.0
    )
    SELECT v.*, CURRENT_TIMESTAMP AS _processed_at
    FROM validated v
    -- Idempotency check: Don't insert orders already in Silver
    LEFT JOIN silver.events s ON v.order_id = s.order_id
    WHERE s.order_id IS NULL;
""",
    [last_ingested],
)

print(
    f"Incremental Silver promotion complete. Total Rows: {con.execute('SELECT COUNT(*) FROM silver.events').fetchone()[0]:,}"
)
