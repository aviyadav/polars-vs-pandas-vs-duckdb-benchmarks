"""Generate synthetic dirty event CSVs with timestamped file names.

Mirrors duck_impl_v2/generate_dirty_data.py but streams the data with
PyArrow and writes ``events_arr_YYYYMMDD_HHMMSS.csv`` into the shared
``data/`` landing folder so the bronze stage can land incrementally.
"""

import argparse
import random
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pv

PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parent

DATA_DIR = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

# Same anomaly vocabulary as the DuckDB v2 generator so both pipelines
# exercise identical data-quality rules.
DIRTY_DATES = ["INVALID_DATE", "9999-12-31", "", "2024/13/45", "N/A"]
VALID_COUNTRIES = ["US", "UK", "DE", "FR", "IN", "JP"]
DIRTY_COUNTRIES = ["us", "UNKNOWN", "123", "XX", "null"]
VALID_CHANNELS = ["search", "social", "email", "direct"]
DIRTY_CHANNELS = ["socail", "searhh", "EMail", "---", None]
DIRTY_USER_IDS = ["-1", "0", "GUEST_USER", "9999999999"]
DIRTY_ORDER_IDS = ["ORD-ERR-999", "NULL", "None", "-999"]
DIRTY_REVENUE = ["-$50.00", "FREE", "99999999.99", "ERROR", "$-10.50"]

COLUMNS = ["event_date", "country", "channel", "user_id", "order_id", "revenue"]

FIRST_DAY = time.mktime((2024, 1, 1, 0, 0, 0, 0, 0, -1))
LAST_DAY = time.mktime((2026, 7, 31, 0, 0, 0, 0, 0, -1))
DAY_SPAN = 86400


def _random_date() -> str:
    ts = FIRST_DAY + random.random() * (LAST_DAY - FIRST_DAY)
    d = time.gmtime(ts)
    return f"{d.tm_year:04d}-{d.tm_mon:02d}-{d.tm_mday:02d}"


def generate_batch(batch_size: int) -> pa.RecordBatch:
    """Build one batch of raw, ~5% dirty event strings."""
    event_dates = []
    countries = []
    channels = []
    user_ids = []
    order_ids = []
    revenues = []

    for _ in range(batch_size):
        # 1. Date
        if random.random() < 0.05:
            event_dates.append(random.choice(DIRTY_DATES))
        else:
            event_dates.append(_random_date())

        # 2. Country
        if random.random() < 0.05:
            countries.append(random.choice(DIRTY_COUNTRIES))
        else:
            countries.append(random.choice(VALID_COUNTRIES))

        # 3. Channel
        if random.random() < 0.05:
            channels.append(random.choice(DIRTY_CHANNELS))
        else:
            channels.append(random.choice(VALID_CHANNELS))

        # 4. User ID
        if random.random() < 0.05:
            user_ids.append(random.choice(DIRTY_USER_IDS))
        else:
            user_ids.append(str(random.randint(1, 200_000)))

        # 5. Order ID
        if random.random() < 0.05:
            order_ids.append(random.choice(DIRTY_ORDER_IDS))
        else:
            order_ids.append(str(random.randint(1, 900_000)))

        # 6. Revenue (two exponentials ~ Erlang/gamma shape, like DuckDB v2)
        r = random.random()
        if r < 0.05:
            revenues.append(random.choice(DIRTY_REVENUE))
        elif r < 0.15:
            revenues.append("0.0")
        else:
            rev = round(random.expovariate(1 / 30.0) + random.expovariate(1 / 30.0), 2)
            revenues.append(str(rev))

    return pa.RecordBatch.from_arrays(
        [
            pa.array(event_dates, pa.string()),
            pa.array(countries, pa.string()),
            pa.array(channels, pa.string()),
            pa.array(user_ids, pa.string()),
            pa.array(order_ids, pa.string()),
            pa.array(revenues, pa.string()),
        ],
        names=COLUMNS,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic dirty event CSV (PyArrow)"
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=1_000_000,
        help="Number of rows to generate (default: 1,000,000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="Batch size for streaming (default: 100,000)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Destination CSV path (default: data/events_arr_<timestamp>.csv)",
    )
    args = parser.parse_args()

    if args.output:
        out_path = Path(args.output)
    else:
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        out_path = DATA_DIR / f"events_arr_{timestamp}.csv"

    print(f"Generating {args.rows:,} dirty rows into {out_path} using PyArrow...")
    start = time.time()

    schema = pa.schema([(c, pa.string()) for c in COLUMNS])
    rows_written = 0
    with pv.CSVWriter(out_path, schema) as writer:
        while rows_written < args.rows:
            current_batch = min(args.batch_size, args.rows - rows_written)
            batch = generate_batch(current_batch)
            writer.write_batch(batch)
            rows_written += current_batch
            print(f"  Written {rows_written:,} / {args.rows:,} rows")

    elapsed = time.time() - start
    print(f"Created CSV file: {out_path}")
    print(f"Dataset generation complete in {elapsed:.2f}s")


if __name__ == "__main__":
    main()
