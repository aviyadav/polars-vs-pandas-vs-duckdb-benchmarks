from pathlib import Path
import random
import time
import argparse
import pyarrow as pa
import pyarrow.csv as pv

PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parent

DEFAULT_OUTPUT = REPO_ROOT / "data" / "events_arr.csv"

DIRTY_DATES = ['INVALID_DATE', '9999-12-31', '', '2024/13/45', 'N/A']
VALID_COUNTRIES = ['US', 'UK', 'DE', 'FR', 'IN', 'JP']
DIRTY_COUNTRIES = ['us', 'UNKNOWN', '123', 'XX', 'null']
VALID_CHANNELS = ['search', 'social', 'email', 'direct']
DIRTY_CHANNELS = ['socail', 'searhh', 'EMail', '---', None]
DIRTY_USER_IDS = ['-1', '0', 'GUEST_USER', '9999999999']
DIRTY_ORDER_IDS = ['ORD-ERR-999', 'NULL', 'None', '-999']
DIRTY_REVENUE = ['-50.00', 'FREE', '99999999.99', 'ERROR', '-10.50']

def generate_batch(batch_size: int):
    event_dates = []
    countries = []
    channels = []
    user_ids = []
    order_ids = []
    revenues = []

    base_date = 19723  # 2024-01-01 in days since epoch
    date_range = 942   # days up to 2026-07-31

    for _ in range(batch_size):
        # 1. Date
        r = random.random()
        if r < 0.05:
            event_dates.append(random.choice(DIRTY_DATES))
        else:
            days = base_date + random.randint(0, date_range)
            # YYYY-MM-DD format
            # Using simple math or date conversion
            d = time.gmtime(days * 86400)
            event_dates.append(f"{d.tm_year:04d}-{d.tm_mon:02d}-{d.tm_mday:02d}")

        # 2. Country
        r = random.random()
        if r < 0.05:
            countries.append(random.choice(DIRTY_COUNTRIES))
        else:
            countries.append(random.choice(VALID_COUNTRIES))

        # 3. Channel
        r = random.random()
        if r < 0.05:
            channels.append(random.choice(DIRTY_CHANNELS))
        else:
            channels.append(random.choice(VALID_CHANNELS))

        # 4. User ID
        r = random.random()
        if r < 0.05:
            user_ids.append(random.choice(DIRTY_USER_IDS))
        else:
            user_ids.append(str(random.randint(1, 200000)))

        # 5. Order ID
        r = random.random()
        if r < 0.05:
            order_ids.append(random.choice(DIRTY_ORDER_IDS))
        else:
            order_ids.append(str(random.randint(1, 900000)))

        # 6. Revenue
        r = random.random()
        if r < 0.05:
            revenues.append(random.choice(DIRTY_REVENUE))
        elif r < 0.15:
            revenues.append("0.0")
        else:
            # Exponential distribution revenue
            rev = round(random.expovariate(1 / 30.0), 2)
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
        names=["event_date", "country", "channel", "user_id", "order_id", "revenue"],
    )

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic dirty event dataset using PyArrow")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows to generate (default: 1,000,000)")
    parser.add_argument("--batch-size", type=int, default=100_000, help="Batch size for streaming (default: 100,000)")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT), help=f"Destination CSV path (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Generating {args.rows:,} dirty rows into {out_path} using PyArrow...")
    start = time.time()

    schema = pa.schema([
        ("event_date", pa.string()),
        ("country", pa.string()),
        ("channel", pa.string()),
        ("user_id", pa.string()),
        ("order_id", pa.string()),
        ("revenue", pa.string()),
    ])

    rows_written = 0
    with pv.CSVWriter(out_path, schema) as writer:
        while rows_written < args.rows:
            current_batch = min(args.batch_size, args.rows - rows_written)
            batch = generate_batch(current_batch)
            writer.write_batch(batch)
            rows_written += current_batch
            print(f"  Written {rows_written:,} / {args.rows:,} rows ({(rows_written / args.rows) * 100:.1f}%)")

    elapsed = time.time() - start
    print(f"Dataset generation complete! File size: {out_path.stat().st_size / (1024*1024):.2f} MB in {elapsed:.2f}s")

if __name__ == "__main__":
    main()
