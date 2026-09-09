"""Silver cleansing stage - incremental quality-gated promotion.

Mirrors duck_impl_v2/load_silver.py. Bronze files that have not yet been
promoted are cleaned/validated through DataFusion SQL and written to
``storage/silver/`` as one Parquet file per promoted bronze source file.
Rows whose ``order_id`` already exists in Silver are skipped so re-runs stay
idempotent.
"""

import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from datafusion import SessionContext

PROJECT_ROOT = Path(__file__).resolve().parent

BRONZE_DIR = PROJECT_ROOT / "storage" / "bronze"
SILVER_DIR = PROJECT_ROOT / "storage" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)

SILVER_SCHEMA = pa.schema(
    [
        pa.field("event_date", pa.date32()),
        pa.field("country", pa.string()),
        pa.field("channel", pa.string()),
        pa.field("user_id", pa.int64()),
        pa.field("order_id", pa.int64()),
        pa.field("revenue", pa.decimal128(12, 2)),
        pa.field("_ingested_at", pa.timestamp("ns")),
        pa.field("_source_file", pa.string()),
        pa.field("_processed_at", pa.timestamp("ns")),
    ]
)

bronze_files = sorted(BRONZE_DIR.glob("*.parquet"))
if not bronze_files:
    print("No bronze files found in storage/bronze. Run load_bronze.py first.")
    sys.exit(1)

# A silver Parquet file whose stem matches a bronze file means it was promoted.
promoted_stems = {p.stem for p in SILVER_DIR.glob("*.parquet")}
new_bronze_files = [f for f in bronze_files if f.stem not in promoted_stems]

if not new_bronze_files:
    print("No new bronze files to promote.")
else:
    for bronze_file in new_bronze_files:
        ctx = SessionContext()
        ctx.register_parquet("bronze_src", str(bronze_file))

        # Existing silver rows (only needed for order_id dedupe / idempotency)
        existing_silver = sorted(SILVER_DIR.glob("*.parquet"))
        if existing_silver:
            ctx.register_parquet("silver_existing", str(SILVER_DIR))

        anti_join = """
        SELECT v.*
        FROM validated v
        LEFT JOIN silver_existing s ON v.order_id = s.order_id
        WHERE s.order_id IS NULL
        """
        plain_select = "SELECT v.* FROM validated v"

        silver_query = f"""
        WITH cleaned AS (
            SELECT
                TRY_CAST(event_date AS DATE) AS event_date,
                UPPER(TRIM(country)) AS country,
                LOWER(TRIM(channel)) AS channel,
                TRY_CAST(user_id AS BIGINT) AS user_id,
                TRY_CAST(order_id AS BIGINT) AS order_id,
                TRY_CAST(
                    REGEXP_REPLACE(revenue, '[^0-9.-]', '', 'g') AS DECIMAL(12, 2)
                ) AS revenue,
                _ingested_at,
                _source_file
            FROM bronze_src
        ),
        validated AS (
            SELECT *
            FROM cleaned
            WHERE event_date BETWEEN DATE '2024-01-01' AND DATE '2026-12-31'
              AND country IN ('US', 'UK', 'DE', 'FR', 'IN', 'JP')
              AND channel IN ('search', 'social', 'email', 'direct')
              AND user_id IS NOT NULL AND user_id > 0
              AND order_id IS NOT NULL AND order_id > 0
              AND revenue IS NOT NULL AND revenue BETWEEN 0.0 AND 10000.0
        )
        {anti_join if existing_silver else plain_select}
        """

        batches = ctx.sql(silver_query).collect()
        cleaned = pa.Table.from_batches(batches) if batches else None

        processed_at = datetime.now(UTC)
        if cleaned is None or cleaned.num_rows == 0:
            empty = SILVER_SCHEMA.empty_table()
            dest_path = SILVER_DIR / f"{bronze_file.stem}.parquet"
            pq.write_table(empty, dest_path, compression="snappy")
            print(f"Promoted {bronze_file.name} (0 clean rows)")
            continue

        # Normalize to the canonical Silver schema + processing timestamp
        cleaned = cleaned.select(SILVER_SCHEMA.names[:-1])
        processed = pa.Table.from_arrays(
            list(cleaned.columns)
            + [pa.array([processed_at] * cleaned.num_rows, pa.timestamp("ns"))],
            schema=SILVER_SCHEMA,
        )

        dest_path = SILVER_DIR / f"{bronze_file.stem}.parquet"
        pq.write_table(processed, dest_path, compression="snappy")
        print(f"Promoted {bronze_file.name} ({processed.num_rows:,} clean rows)")

total_silver = sum(pq.read_metadata(p).num_rows for p in SILVER_DIR.glob("*.parquet"))
print(f"Total Silver Rows: {total_silver:,}")
