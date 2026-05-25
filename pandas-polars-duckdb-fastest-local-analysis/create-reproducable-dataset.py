from pathlib import Path
import os
import resource
import time
import numpy as np
import pandas as pd

# Start timing the entire operation
start_time = time.perf_counter()

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

row_count = 20_000_000
rng = np.random.default_rng(42)

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

df.loc[rng.random(row_count) < 0.15, "revenue"] = 0
df.to_csv(DATA_DIR / "events.csv", index=False)


print("Dataset created at", DATA_DIR / "events.csv")

# End timing the entire operation
end_time = time.perf_counter()
execution_time = end_time - start_time

# Retrieve system info
cpu_count = os.cpu_count()
peak_memory_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
peak_memory_mb = peak_memory_kb / 1024.0

print("\n--- Execution Metrics ---")
print(f"Execution Time:  {execution_time:.4f} seconds")
print(f"Peak Memory:     {peak_memory_mb:.2f} MB")
print(f"Processor Count: {cpu_count} CPU cores")
