from datetime import UTC, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parent

DATA_DIR = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

row_count = 1_000_000

# Generate a timestamp for the filename
timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

# Create timestamped filename
csv_path = DATA_DIR / f"events_duck_{timestamp}.csv"

con = duckdb.connect()

con.execute(
    f"""
    COPY (
        SELECT
            -- 1. Date (5% dirty: malformed date strings, empty strings, or distant future dates)
            CASE
                WHEN random() < 0.05 THEN
                    (['INVALID_DATE', '9999-12-31', '', '2024/13/45', 'N/A'])[CAST(floor(random() * 5) + 1 AS BIGINT)]
                ELSE
                    CAST('2024-01-01'::DATE + CAST(floor(random() * (DATE '2026-07-31' - DATE '2024-01-01' + 1)) AS INTEGER) AS VARCHAR)
            END AS event_date,

            -- 2. Country
            CASE
                WHEN random() < 0.05 THEN
                    (['us', 'UNKNOWN', '123', 'XX', 'null'])[CAST(floor(random() * 5) + 1 AS BIGINT)]
                ELSE
                    (['US', 'UK', 'DE', 'FR', 'IN', 'JP'])[CAST(floor(random() * 6) + 1 AS BIGINT)]
            END AS country,

            -- 3. Channel
            CASE
                WHEN random() < 0.05 THEN
                    (['socail', 'searhh', 'EMail', '---', NULL])[CAST(floor(random() * 5) + 1 AS BIGINT)]
                ELSE
                    (['search', 'social', 'email', 'direct'])[CAST(floor(random() * 4) + 1 AS BIGINT)]
            END AS channel,

            -- 4. User ID
            CASE
                WHEN random() < 0.05 THEN
                    (['-1', '0', 'GUEST_USER', '9999999999'])[CAST(floor(random() * 4) + 1 AS BIGINT)]
                ELSE
                    CAST(CAST(floor(random() * 200000) + 1 AS BIGINT) AS VARCHAR)
            END AS user_id,

            -- 5. Order ID
            CASE
                WHEN random() < 0.05 THEN
                    (['ORD-ERR-999', 'NULL', 'None', '-999'])[CAST(floor(random() * 4) + 1 AS BIGINT)]
                ELSE
                    CAST(CAST(floor(random() * 900000) + 1 AS BIGINT) AS VARCHAR)
            END AS order_id,

            -- 6. Revenue
            CASE
                WHEN random() < 0.05 THEN
                    (['-$50.00', 'FREE', '99999999.99', 'ERROR', '$-10.50'])[CAST(floor(random() * 5) + 1 AS BIGINT)]
                WHEN random() < 0.15 THEN '0.0'
                ELSE CAST(round((-log(random()) - log(random())) * 30.0, 2) AS VARCHAR)
            END AS revenue

        FROM generate_series(1, {row_count})
    ) TO '{csv_path}' (HEADER, DELIMITER ',');
"""
)

print(f"Created CSV file: {csv_path}")
