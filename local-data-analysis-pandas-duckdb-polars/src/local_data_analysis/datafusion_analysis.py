from datafusion import SessionContext
import time
import tracemalloc

query = """
    SELECT
        country,
        channel,
        COUNT(DISTINCT user_id) AS users,
        COUNT(order_id) AS orders,
        SUM(revenue) AS revenue
    FROM events
    WHERE CAST(event_date AS DATE) >= DATE '2024-01-01'
    AND country IN ('US', 'UK', 'DE')
    AND revenue > 0
    GROUP BY country, channel
    ORDER BY revenue DESC
    LIMIT 10;
"""

# 1. Start the memory and time trackers
tracemalloc.start()
start_time = time.perf_counter()

# 2. Create session, register the CSV, and run the query
ctx = SessionContext()
# ctx.register_csv("events", "data/events.csv")
ctx.register_parquet("events", "data/events.parquet")
result = ctx.sql(query).to_polars()

# 3. Stop the trackers and calculate metrics
end_time = time.perf_counter()
current_mem, peak_mem = tracemalloc.get_traced_memory()
tracemalloc.stop()

# 4. Print results and metrics
print(" - - Query Result - -")
print(result)
print("\n" + "=" * 30)
print(" - - Performance Metrics - -")
print(f"Execution Time: {end_time - start_time:.4f} seconds")
print(f"Peak Memory Usage: {peak_mem / (1024 * 1024):.2f} MB")
print("=" * 30)
