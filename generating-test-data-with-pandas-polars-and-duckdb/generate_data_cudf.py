import argparse
import json
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import cudf
import cupy as cp
import numpy as np
import psutil
import pyarrow as pa
import pyarrow.csv as pv


def parse_args():
    parser = argparse.ArgumentParser(description="Generate test data with cuDF (GPU)")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument(
        "--json-out", type=str, default=None, help="Write metrics JSON to this path"
    )
    return parser.parse_args()


args = parse_args()
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
row_count = args.rows
csv_path = DATA_DIR / "events_cudf.csv"

# - - BACKGROUND MEMORY MONITOR LOGIC (HOST RAM) - -
peak_memory = 0
monitor_active = True


def monitor_memory():
    """Continuously tracks host process memory."""
    global peak_memory
    process = psutil.Process()
    while monitor_active:
        try:
            current_mem = process.memory_info().rss
            peak_memory = max(peak_memory, current_mem)
        except Exception:  # noqa: BLE001
            break
        time.sleep(0.01)


mem_thread = threading.Thread(target=monitor_memory, daemon=True)
mem_thread.start()

# - - START GENERATION TIMING - -
start_time = time.perf_counter()

# 1. Dates: generate random day offsets on GPU and convert to datetime64[s]
start_epoch_sec = (
    np.datetime64("2024-01-01", "D").astype("datetime64[s]").astype("int64")
)
end_epoch_sec = np.datetime64("2026-01-31", "D").astype("datetime64[s]").astype("int64")
num_days = int((end_epoch_sec - start_epoch_sec) // 86400) + 1

random_days = cp.random.randint(0, num_days, size=row_count, dtype=cp.int64)
event_dates_sec = start_epoch_sec + random_days * 86400
event_dates = cudf.Series(event_dates_sec).astype("datetime64[s]")

# 2. Categoricals: pick indices on GPU
country_list = ["US", "UK", "DE", "FR", "IN", "JP"]
channel_list = ["search", "social", "email", "direct"]

country_indices = cp.random.randint(0, len(country_list), size=row_count)
channel_indices = cp.random.randint(0, len(channel_list), size=row_count)

countries = cudf.Series(country_list).take(country_indices).reset_index(drop=True)
channels = cudf.Series(channel_list).take(channel_indices).reset_index(drop=True)

# 3. User & Order IDs
user_ids = cp.random.randint(1, 200_001, size=row_count, dtype=cp.int32)
order_ids = cp.random.randint(1, 900_001, size=row_count, dtype=cp.int32)

# 4. Revenue: Gamma distribution on GPU + 15% zero-out
revenue = cp.round(cp.random.gamma(shape=2.0, scale=30.0, size=row_count), 2)
revenue[cp.random.random(row_count) < 0.15] = 0.0

# Build cuDF DataFrame on GPU
df = cudf.DataFrame(
    {
        "event_date": event_dates,
        "country": countries,
        "channel": channels,
        "user_id": user_ids,
        "order_id": order_ids,
        "revenue": revenue,
    }
)
cp.cuda.Stream.null.synchronize()  # Wait for GPU kernels to finish
end_gen = time.perf_counter()
# - - END GENERATION TIMING - -

# - - START WRITE TIMING - -
# NOTE: cuDF's native to_csv() (kvikio-backed) hits a CUDA_ERROR_ILLEGAL_ADDRESS
# on Blackwell GPUs with CUDA 13 drivers. We instead do a zero-copy conversion
# to Arrow and write via PyArrow's CSV writer.
start_write = time.perf_counter()
table = df.to_arrow()
# Cast timestamps to date32 so the output matches the other benchmarks
# (date-only format, e.g. "2024-01-30" instead of "2024-01-30 00:00:00")
date_col_idx = table.schema.get_field_index("event_date")
table = table.set_column(
    date_col_idx,
    "event_date",
    table.column("event_date").cast(pa.date32()),
)
pv.write_csv(table, csv_path)
end_write = time.perf_counter()
# - - END WRITE TIMING - -

# Stop memory tracking thread
monitor_active = False
mem_thread.join()

# - - PRINT RESULTS - -
gen_duration = end_gen - start_time
write_duration = end_write - start_write
total_duration = gen_duration + write_duration
peak_mb = peak_memory / (1024 * 1024)

print("=" * 40)
print(f"cuDF Performance Metrics ({row_count:,} rows):")
print("=" * 40)
print(f"Data Generation Time: {gen_duration:.4f} seconds")
print(f"CSV Writing Time:     {write_duration:.4f} seconds")
print(f"Total Elapsed Time:   {total_duration:.4f} seconds")
print(f"Peak Host Memory:     {peak_mb:.2f} MB")
print("=" * 40)

if args.json_out:
    Path(args.json_out).write_text(
        json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "library": "cudf",
                "row_count": row_count,
                "gen_time_s": gen_duration,
                "write_time_s": write_duration,
                "total_time_s": total_duration,
                "peak_memory_mb": peak_mb,
            },
            indent=2,
        )
    )
