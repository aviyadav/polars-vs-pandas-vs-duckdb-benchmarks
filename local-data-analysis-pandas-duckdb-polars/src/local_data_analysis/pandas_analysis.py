import pandas as pd
import time
import tracemalloc
from pathlib import Path

# 1. Start the timer and memory profiler
tracemalloc.start()
start_time = time.perf_counter()

DATA_DIR: Path = Path("data")

# 2. Run the pandas pipeline
df = pd.read_csv(DATA_DIR / "events.csv", parse_dates=["event_date"])
result = (
    df.loc[
        (df["event_date"] >= "2024-01-01")
        & (df["country"].isin(["UK", "US", "DE", "FR", "SA", "IT", "NZ"]))
        & (df["revenue"] > 0),
        ["country", "channel", "user_id", "order_id", "revenue"],
    ]
    .groupby(["country", "channel"], as_index=False)
    .agg(
        users=("user_id", "nunique"),
        orders=("order_id", "count"),
        revenue=("revenue", "sum"),
    )
    .sort_values("revenue", ascending=False)
    .head(10)
    .reset_index()
)

# 3. Stop the trackers and calculate the metrics
end_time = time.perf_counter()
current_mem, peak_mem = tracemalloc.get_traced_memory()
tracemalloc.stop()

# 4. print the results
print(" - - Query Result - -")
print(result)
print("\n" + "="*30)
print(" - - Performance Metrics - -")
print(f"Execution Time: {end_time - start_time:.4f} seconds")
print(f"Peak Memory Usage: {peak_mem / (1024 * 1024):.2f} MB")
print("="*30)
