import polars as pl
import time
import tracemalloc
from pathlib import Path

# 1. Start the timer and memory profiler
tracemalloc.start()
start_time = time.perf_counter()

DATA_DIR: Path = Path("data")

# 2. Run the pandas pipeline
result = (
    pl.scan_csv(DATA_DIR / "events.csv", try_parse_dates=True)
    .filter(
        (pl.col("event_date") >= pl.date(2024, 1, 1))
        & (pl.col("country").is_in(["UK", "US", "DE", "FR", "SA", "IT", "NZ"]))
        & (pl.col("revenue") > 0)
    )
    .select(["country", "channel", "user_id", "order_id", "revenue"])
    .group_by(["country", "channel"])
    .agg(
        pl.col("user_id").n_unique().alias("users"),
        pl.col("order_id").count().alias("orders"),
        pl.col("revenue").sum().alias("revenue"),
    )
    .sort("revenue", descending=True)
    .limit(10)
    .collect()
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
