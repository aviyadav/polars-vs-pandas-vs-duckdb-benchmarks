import argparse
import json
import threading  # Added for background memory tracking
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import psutil  # Make sure to run: uv pip install psutil


def parse_args():
    parser = argparse.ArgumentParser(description="Generate test data with Pandas")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument(
        "--json-out", type=str, default=None, help="Write metrics JSON to this path"
    )
    return parser.parse_args()


args = parse_args()
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
row_count = args.rows
rng = np.random.default_rng(42)
# - - BACKGROUND MEMORY MONITOR LOGIC - -
peak_memory = 0
monitor_active = True


def monitor_memory():
    """Continuously tracks the process memory to catch the peak spike."""
    global peak_memory
    process = psutil.Process()
    while monitor_active:
        try:
            current_mem = process.memory_info().rss  # RSS = Physical RAM used
            peak_memory = max(peak_memory, current_mem)
        except Exception:  # noqa: BLE001
            break
        time.sleep(0.01)  # Sample every 10ms


# Start the memory tracking thread
mem_thread = threading.Thread(target=monitor_memory, daemon=True)
mem_thread.start()
# - - START GENERATION TIMING - -
start_gen = time.perf_counter()
df = pd.DataFrame(
    {
        "event_date": rng.choice(
            pd.date_range("2024-01-01", "2026-01-31", freq="D"),
            size=row_count,
        ),
        "country": rng.choice(["US", "UK", "DE", "FR", "IN", "JP"], size=row_count),
        "channel": rng.choice(["search", "social", "email", "direct"], size=row_count),
        "user_id": rng.integers(1, 200_000, size=row_count),
        "order_id": rng.integers(1, 900_000, size=row_count),
        "revenue": rng.gamma(shape=2.0, scale=30.0, size=row_count).round(2),
    }
)
# Include the 15% zero-out step in the generation time
df.loc[rng.random(row_count) < 0.15, "revenue"] = 0
end_gen = time.perf_counter()
# - - END GENERATION TIMING - -
# - - START WRITE TIMING - -
start_write = time.perf_counter()
df.to_csv(DATA_DIR / "events_pd.csv", index=False)
end_write = time.perf_counter()
# - - END WRITE TIMING - -
# Stop the memory tracking thread safely
monitor_active = False
mem_thread.join()
# - - PRINT RESULTS - -
gen_duration = end_gen - start_gen
write_duration = end_write - start_write
total_duration = gen_duration + write_duration
peak_mb = peak_memory / (1024 * 1024)  # Convert bytes to Megabytes
print("=" * 40)
print(f"Pandas Performance Metrics ({row_count:,} rows):")
print("=" * 40)
print(f"Data Generation Time: {gen_duration:.4f} seconds")
print(f"CSV Writing Time: {write_duration:.4f} seconds")
print(f"Total Elapsed Time: {total_duration:.4f} seconds")
print(f"Peak Memory Usage: {peak_mb:.2f} MB")
print("=" * 40)

if args.json_out:
    Path(args.json_out).write_text(
        json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "library": "pandas",
                "row_count": row_count,
                "gen_time_s": gen_duration,
                "write_time_s": write_duration,
                "total_time_s": total_duration,
                "peak_memory_mb": peak_mb,
            },
            indent=2,
        )
    )
