from pathlib import Path
import duckdb

# Resolve project paths dynamically

PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parent


CSV_PATH = REPO_ROOT / "data" / "events_duck.csv"
DB_PATH = PROJECT_ROOT / "storage" / "warehouse.duckdb"

# Ensure storage directory exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

if not CSV_PATH.exists():
    raise FileNotFoundError(
        f"Raw dataset not found at {CSV_PATH}. "
        "Please run 'python generate_dirty_data.py' first."
    )

# Connect to persistent DuckDB file
con = duckdb.connect(str(DB_PATH))

# Create bronze schema if it doesn't exist
con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

# Ingest landing CSV into Bronze table
print(f"Loading {CSV_PATH} into bronze.events...")
con.execute(f"""
    CREATE OR REPLACE TABLE bronze.events AS
    SELECT
        event_date,
        country,
        channel,
        user_id,
        order_id,
        revenue,
        current_timestamp AS _ingested_at,
        '{CSV_PATH.name}' AS _source_file
    FROM read_csv('{CSV_PATH}', all_varchar=True);
""")

print(f"Bronze layer loaded successfully! Row count: {con.execute('SELECT COUNT(*) FROM bronze.events').fetchone()[0]}")
con.close()