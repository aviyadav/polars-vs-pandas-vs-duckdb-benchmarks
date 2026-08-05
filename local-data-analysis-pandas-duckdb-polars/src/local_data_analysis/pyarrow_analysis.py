import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.compute as pc
import time
import tracemalloc
from datetime import date
from pathlib import Path

# 1. Start the timer and memory profiler
tracemalloc.start()
start_time = time.perf_counter()

DATA_DIR: Path = Path("data")

# 2. Read CSV with explicit date32 parsing for event_date
table = pcsv.read_csv(
    DATA_DIR / "events.csv",
    convert_options=pa.csv.ConvertOptions(
        column_types={"event_date": pa.date32()}
    ),
)

# 3. Build the filter mask and run the full pipeline
mask = pc.and_(
    pc.and_(
        pc.greater_equal(table["event_date"], pa.scalar(date(2024, 1, 1), type=pa.date32())),
        pc.is_in(
            table["country"],
            pa.array(["UK", "US", "DE", "FR", "SA", "IT", "NZ"]),
        ),
    ),
    pc.greater(table["revenue"], 0.0),
)

result = (
    table.filter(mask)
    .select(["country", "channel", "user_id", "order_id", "revenue"])
    .group_by(["country", "channel"])
    .aggregate(
        [
            ("user_id", "count_distinct"),
            ("order_id", "count"),
            ("revenue", "sum"),
        ]
    )
    .rename_columns(["country", "channel", "users", "orders", "revenue"])
    .sort_by([("revenue", "descending")])
    .slice(0, 10)
)

# 4. Stop the trackers and calculate the metrics
end_time = time.perf_counter()
current_mem, peak_mem = tracemalloc.get_traced_memory()
tracemalloc.stop()

# 5. print the results
print(" - - Query Result - -")
print(result)
print("\n" + "=" * 30)
print(" - - Performance Metrics - -")
print(f"Execution Time: {end_time - start_time:.4f} seconds")
print(f"Peak Memory Usage: {peak_mem / (1024 * 1024):.2f} MB")
print("=" * 30)
