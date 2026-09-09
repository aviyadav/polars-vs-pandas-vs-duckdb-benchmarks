import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parent

LANDING_DIR = REPO_ROOT / "data"
STORAGE_PATH = PROJECT_ROOT / "storage"
STORAGE_PATH.mkdir(exist_ok=True)
DB_PATH = STORAGE_PATH / "warehouse.duckdb"

con = duckdb.connect(str(DB_PATH))
con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

# Ensure persistent target table exists with proper schema
con.execute("""
    CREATE TABLE IF NOT EXISTS bronze.events (
        event_date VARCHAR,
        country VARCHAR,
        channel VARCHAR,
        user_id VARCHAR,
        order_id VARCHAR,
        revenue VARCHAR,
        _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        _source_file VARCHAR
    );
""")

# Scan landing folder for CSVs
csv_files = list(LANDING_DIR.glob("events_duck_*.csv"))

if not csv_files:
    print("No CSV files found in landing directory.")
    sys.exit(0)

# Fetch previously processed files to prevent duplicate reads
processed_files = {
    r[0]
    for r in con.execute("SELECT DISTINCT _source_file FROM bronze.events").fetchall()
}

new_files = [f for f in csv_files if f.name not in processed_files]

if not new_files:
    print("No new CSV files to ingest.")
else:
    for file_path in new_files:
        con.execute(f"""
            INSERT INTO bronze.events (
                event_date, country, channel, user_id, order_id, revenue, _source_file
            )
            SELECT
                event_date, country, channel, user_id, order_id, revenue, '{file_path.name}' AS _source_file
            FROM read_csv('{file_path}', all_varchar=TRUE);
        """)
        print(f"Ingested {file_path.name} into bronze.events")

print(
    f"Total Bronze Rows: {con.execute('SELECT COUNT(*) FROM bronze.events').fetchone()[0]:,}"
)
