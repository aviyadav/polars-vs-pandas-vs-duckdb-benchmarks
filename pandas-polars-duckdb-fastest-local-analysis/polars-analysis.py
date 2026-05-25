import os
import resource
import time

import polars as pl

# Start timing the entire operation
start_time = time.perf_counter()

result = (
    pl.scan_csv("data/events.csv", try_parse_dates=True)
    .filter(
        (pl.col("event_date") >= pl.date(2025, 1, 1))
        & (pl.col("country").is_in(["US", "UK", "DE"]))
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

print(result)

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