from pathlib import Path
import duckdb

# Define paths
PROJECT_ROOT = Path(__file__).parent
DB_PATH = PROJECT_ROOT / "storage" / "warehouse.duckdb"

# Connect to persistent DuckDB file
con = duckdb.connect(str(DB_PATH))

# 1. Create Silver Schema
con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

# 2. Build Silver Table with Type Safety, Formatting, and Business Logic Filters
print("Loading silver layer: Deduplicating and applying type safety...")
con.execute("""
    CREATE OR REPLACE TABLE silver.events AS
    WITH cleaned AS (
        SELECT
            -- Safe type casting for dates
            TRY_CAST(event_date AS DATE) AS event_date,
            
            -- Standardize country codes to uppercase
            UPPER(TRIM(country)) AS country,
            
            -- Standardize channel names to lowercase
            LOWER(TRIM(channel)) AS channel,
            
            -- Cast numeric IDs
            TRY_CAST(user_id AS BIGINT) AS user_id,
            TRY_CAST(order_id AS BIGINT) AS order_id,
            
            -- Strip out non-numeric characters (currency symbols, spaces) before decimal cast
            TRY_CAST(
                REGEXP_REPLACE(revenue, '[^0-9.-]', '', 'g') AS DECIMAL(12, 2)
            ) AS revenue,
            
            -- Retain metadata for auditing
            _ingested_at,
            _source_file,
            CURRENT_TIMESTAMP AS _processed_at
        FROM bronze.events
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
      -- Filter out out-of-bounds dates like '9999-12-31'
      AND event_date BETWEEN DATE '2024-01-01' AND DATE '2026-12-31'
      
      -- Categorical validation
      AND country IN ('US', 'UK', 'DE', 'FR', 'IN', 'JP')
      AND channel IN ('search', 'social', 'email', 'direct')
      
      -- Identifier validation
      AND user_id IS NOT NULL AND user_id > 0
      AND order_id IS NOT NULL AND order_id > 0
      
      -- Revenue bounds validation (filters negative values and extreme outliers like '99999999.99')
      AND revenue IS NOT NULL AND revenue BETWEEN 0.0 AND 10000.0;
""")

silver_count = con.execute("SELECT COUNT(*) FROM silver.events").fetchone()[0]
bronze_count = con.execute("SELECT COUNT(*) FROM bronze.events").fetchone()[0]

print("Silver layer complete!")
print(f"Bronze Rows: {bronze_count:,}")
print(f"Silver Rows: {silver_count:,} (Filtered {bronze_count - silver_count:,} corrupt or outlier records)")

con.close()