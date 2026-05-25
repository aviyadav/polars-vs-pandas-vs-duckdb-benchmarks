import os
import resource
import time
import pandas as pd

# Start timing the entire operation
start_time = time.perf_counter()

# Load and process the dataset
df = pd.read_csv("data/events.csv", parse_dates=["event_date"])

result = (
    df.loc[
        (df["event_date"] >= "2025-01-01")
        & (df["country"].isin(["US", "UK", "DE"]))
        & (df["revenue"] > 0),
        ["country", "channel", "user_id", "order_id", "revenue"],
    ]
    .groupby(["country", "channel"], as_index=False)
    .agg(
        users=('user_id', 'nunique'),
        orders=('order_id', 'count'),
        revenue=('revenue', 'sum'),
    )
    .sort_values(by='revenue', ascending=False)
    .head(10)
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