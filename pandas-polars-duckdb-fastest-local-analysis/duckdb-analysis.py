import os
import resource
import time

import duckdb

# Start timing the entire operation
start_time = time.perf_counter()

query = """
SELECT
    country,
    channel,
    COUNT(DISTINCT user_id) AS users,
    COUNT(order_id) AS orders,
    SUM(revenue) AS revenue
FROM read_csv('data/events.csv', header=True)
WHERE CAST(event_date AS DATE) >= DATE '2025-01-01'
    AND country IN ('US', 'UK', 'DE')
    AND revenue > 0
GROUP BY country, channel
ORDER BY revenue DESC
LIMIT 10
"""

result = duckdb.sql(query)

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