"""
CLI Benchmark Suite for Pandas, Polars, DuckDB, and Apache DataFusion.
"""

import argparse
import os
import sys
import threading
import time
import duckdb
import psutil
import pandas as pd
import polars as pl
import datafusion


def print_dataset_summary(data_dir="data"):
    files = [
        os.path.join(data_dir, "iowa_sales.snappy.parquet"),
        os.path.join(data_dir, "iowa_category.snappy.parquet"),
        os.path.join(data_dir, "iowa_date.snappy.parquet"),
        os.path.join(data_dir, "iowa_store.snappy.parquet"),
        os.path.join(data_dir, "iowa_vendor.snappy.parquet"),
        os.path.join(data_dir, "iowa_item.snappy.parquet"),
    ]

    summary = []
    for file in files:
        if not os.path.exists(file):
            print(f"Warning: File {file} not found!")
            continue
        df = pd.read_parquet(file)
        summary.append(
            {
                "file": file,
                "rows": len(df),
                "columns": len(df.columns),
                "size_mb": round(os.path.getsize(file) / 1024 / 1024, 2),
            }
        )

    df_summary = pd.DataFrame(summary)
    print("\nDataset Summary:")
    print(df_summary.to_string(index=False))
    print()


def run_benchmark(func, name):
    process = psutil.Process(os.getpid())

    max_ram = 0
    max_cpu = 0
    running = True

    def monitor():
        nonlocal max_ram, max_cpu, running

        process.cpu_percent()  # warmup

        while running:
            ram = process.memory_info().rss / 1024**3
            cpu = process.cpu_percent()
            max_ram = max(max_ram, ram)
            max_cpu = max(max_cpu, cpu)
            time.sleep(0.1)

    monitor_thread = threading.Thread(target=monitor)
    monitor_thread.start()
    start = time.perf_counter()
    result = func()
    elapsed = time.perf_counter() - start
    running = False
    monitor_thread.join()

    return {
        "engine": name,
        "time": round(elapsed, 2),
        "peak_ram_gb": round(max_ram, 2),
        "peak_cpu_pct": round(max_cpu, 0),
        "rows": len(result),
    }


def save_results(results, filename="benchmark_results.csv"):
    if not results:
        return

    df_results = pd.DataFrame(results)

    avg_row = {
        "engine": df_results["engine"].iloc[0] + "_AVG",
        "time": round(df_results["time"].mean(), 2),
        "peak_ram_gb": round(df_results["peak_ram_gb"].mean(), 2),
        "peak_cpu_pct": round(df_results["peak_cpu_pct"].mean(), 0),
        "rows": int(df_results["rows"].mean()),
    }

    df_results = pd.concat(
        [df_results, pd.DataFrame([avg_row])],
        ignore_index=True,
    )

    df_results.to_csv(
        filename,
        mode="a",
        header=not os.path.exists(filename),
        index=False,
    )

    print(f"\n--- {df_results['engine'].iloc[0]} Benchmark Results ---")
    print(df_results.to_string(index=False))
    print()


def pandas_run(data_dir="data"):
    sales = pd.read_parquet(os.path.join(data_dir, "iowa_sales.snappy.parquet")).drop(
        columns=["Inserted"], errors="ignore"
    )
    category = pd.read_parquet(os.path.join(data_dir, "iowa_category.snappy.parquet")).drop(
        columns=["Inserted"], errors="ignore"
    )
    date = pd.read_parquet(os.path.join(data_dir, "iowa_date.snappy.parquet")).drop(
        columns=["Inserted"], errors="ignore"
    )
    store = pd.read_parquet(os.path.join(data_dir, "iowa_store.snappy.parquet")).drop(
        columns=["Inserted"], errors="ignore"
    )
    vendor = pd.read_parquet(os.path.join(data_dir, "iowa_vendor.snappy.parquet")).drop(
        columns=["Inserted"], errors="ignore"
    )
    item = pd.read_parquet(os.path.join(data_dir, "iowa_item.snappy.parquet")).drop(
        columns=["Inserted"], errors="ignore"
    )

    result = (
        sales.merge(category, on="CategoryID")
        .merge(date, on="DateID")
        .merge(store, on="StoreID")
        .merge(vendor, on="VendorID")
        .merge(item, on="ItemID")
        .query("CalendarYear >= 2022")
        .groupby(
            [
                "CalendarYear",
                "County",
                "StoreName",
                "VendorName",
                "CategoryName",
                "ItemName",
            ],
            as_index=False,
            dropna=False,
        )
        .agg(
            total_sales=("SaleDollars", "sum"),
            bottles_sold=("BottlesSold", "sum"),
            volume_liters=("VolumeSoldLiters", "sum"),
            avg_retail=("StateBottleRetail", "mean"),
            avg_cost=("StateBottleCost", "mean"),
            transactions=("ID", "count"),
        )
        .sort_values(
            [
                "CalendarYear",
                "County",
                "StoreName",
                "VendorName",
                "CategoryName",
                "ItemName",
                "total_sales",
                "bottles_sold",
            ],
            ascending=[True, True, True, True, True, True, False, False],
        )
    )

    return result


def polars_run(data_dir="data"):
    sales = pl.scan_parquet(os.path.join(data_dir, "iowa_sales.snappy.parquet")).drop(
        "Inserted", strict=False
    )
    category = pl.scan_parquet(os.path.join(data_dir, "iowa_category.snappy.parquet")).drop(
        "Inserted", strict=False
    )
    date = pl.scan_parquet(os.path.join(data_dir, "iowa_date.snappy.parquet")).drop(
        "Inserted", strict=False
    )
    store = pl.scan_parquet(os.path.join(data_dir, "iowa_store.snappy.parquet")).drop(
        "Inserted", strict=False
    )
    vendor = pl.scan_parquet(os.path.join(data_dir, "iowa_vendor.snappy.parquet")).drop(
        "Inserted", strict=False
    )
    item = pl.scan_parquet(os.path.join(data_dir, "iowa_item.snappy.parquet")).drop(
        "Inserted", strict=False
    )

    result = (
        sales.join(category, on="CategoryID")
        .join(date, on="DateID")
        .join(store, on="StoreID")
        .join(vendor, on="VendorID")
        .join(item, on="ItemID")
        .filter(pl.col("CalendarYear") >= 2022)
        .group_by(
            [
                "CalendarYear",
                "County",
                "StoreName",
                "VendorName",
                "CategoryName",
                "ItemName",
            ]
        )
        .agg(
            [
                pl.col("SaleDollars").sum().alias("total_sales"),
                pl.col("BottlesSold").sum().alias("bottles_sold"),
                pl.col("VolumeSoldLiters").sum().alias("volume_liters"),
                pl.col("StateBottleRetail").mean().alias("avg_retail"),
                pl.col("StateBottleCost").mean().alias("avg_cost"),
                pl.col("ID").count().alias("transactions"),
            ]
        )
        .sort(
            [
                "CalendarYear",
                "County",
                "StoreName",
                "VendorName",
                "CategoryName",
                "ItemName",
                "total_sales",
                "bottles_sold",
            ],
            descending=[False, False, False, False, False, False, True, True],
        )
        .collect()
    )

    return result


def duckdb_run(data_dir="data"):
    f_sales = os.path.join(data_dir, "iowa_sales.snappy.parquet").replace("\\", "/")
    f_cat = os.path.join(data_dir, "iowa_category.snappy.parquet").replace("\\", "/")
    f_date = os.path.join(data_dir, "iowa_date.snappy.parquet").replace("\\", "/")
    f_store = os.path.join(data_dir, "iowa_store.snappy.parquet").replace("\\", "/")
    f_vendor = os.path.join(data_dir, "iowa_vendor.snappy.parquet").replace("\\", "/")
    f_item = os.path.join(data_dir, "iowa_item.snappy.parquet").replace("\\", "/")

    query = f"""
    SELECT
        d.CalendarYear,
        s.County,
        s.StoreName,
        v.VendorName,
        c.CategoryName,
        i.ItemName,
        SUM(f.SaleDollars)       AS total_sales,
        SUM(f.BottlesSold)       AS bottles_sold,
        SUM(f.VolumeSoldLiters)  AS volume_liters,
        AVG(f.StateBottleRetail) AS avg_retail,
        AVG(f.StateBottleCost)   AS avg_cost,
        COUNT(f.ID)              AS transactions
    FROM read_parquet('{f_sales}') f
    JOIN read_parquet('{f_cat}') c
        ON f.CategoryID = c.CategoryID
    JOIN read_parquet('{f_date}') d
        ON f.DateID = d.DateID
    JOIN read_parquet('{f_store}') s
        ON f.StoreID = s.StoreID
    JOIN read_parquet('{f_vendor}') v
        ON f.VendorID = v.VendorID
    JOIN read_parquet('{f_item}') i
        ON f.ItemID = i.ItemID
    WHERE d.CalendarYear >= 2022
    GROUP BY
        d.CalendarYear,
        s.County,
        s.StoreName,
        v.VendorName,
        c.CategoryName,
        i.ItemName
    ORDER BY
        d.CalendarYear,
        s.County,
        s.StoreName,
        v.VendorName,
        c.CategoryName,
        i.ItemName,
        total_sales DESC,
        bottles_sold DESC
    """

    result = duckdb.sql(query).df()
    return result


def datafusion_run(data_dir="data"):
    ctx = datafusion.SessionContext()

    ctx.register_parquet("sales", os.path.join(data_dir, "iowa_sales.snappy.parquet"))
    ctx.register_parquet("category", os.path.join(data_dir, "iowa_category.snappy.parquet"))
    ctx.register_parquet("date", os.path.join(data_dir, "iowa_date.snappy.parquet"))
    ctx.register_parquet("store", os.path.join(data_dir, "iowa_store.snappy.parquet"))
    ctx.register_parquet("vendor", os.path.join(data_dir, "iowa_vendor.snappy.parquet"))
    ctx.register_parquet("item", os.path.join(data_dir, "iowa_item.snappy.parquet"))

    query = """
    SELECT
        d."CalendarYear",
        s."County",
        s."StoreName",
        v."VendorName",
        c."CategoryName",
        i."ItemName",
        SUM(f."SaleDollars")       AS total_sales,
        SUM(f."BottlesSold")       AS bottles_sold,
        SUM(f."VolumeSoldLiters")  AS volume_liters,
        AVG(f."StateBottleRetail") AS avg_retail,
        AVG(f."StateBottleCost")   AS avg_cost,
        COUNT(f."ID")              AS transactions
    FROM sales f
    JOIN category c ON f."CategoryID" = c."CategoryID"
    JOIN date d     ON f."DateID" = d."DateID"
    JOIN store s    ON f."StoreID" = s."StoreID"
    JOIN vendor v   ON f."VendorID" = v."VendorID"
    JOIN item i     ON f."ItemID" = i."ItemID"
    WHERE d."CalendarYear" >= 2022
    GROUP BY
        d."CalendarYear",
        s."County",
        s."StoreName",
        v."VendorName",
        c."CategoryName",
        i."ItemName"
    ORDER BY
        d."CalendarYear",
        s."County",
        s."StoreName",
        v."VendorName",
        c."CategoryName",
        i."ItemName",
        total_sales DESC,
        bottles_sold DESC
    """

    result = ctx.sql(query).to_polars()
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Data Processing Benchmark CLI (Pandas, Polars, DuckDB, Apache DataFusion)"
    )
    parser.add_argument(
        "--engine",
        "-e",
        choices=["pandas", "polars", "duckdb", "datafusion", "all"],
        default="all",
        help="Specify which engine to benchmark (default: all)",
    )
    parser.add_argument(
        "--runs",
        "-r",
        type=int,
        default=5,
        help="Number of iterations per engine (default: 5)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="benchmark_results.csv",
        help="Output CSV file path (default: benchmark_results.csv)",
    )
    parser.add_argument(
        "--data-dir",
        "-d",
        default="data",
        help="Path to directory containing Parquet datasets (default: data)",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print dataset row and column summary before benchmarking",
    )

    args = parser.parse_args()

    if args.summary or args.engine == "all":
        print_dataset_summary(args.data_dir)

    engines_to_run = []
    if args.engine == "all":
        engines_to_run = [
            ("Pandas", lambda: pandas_run(args.data_dir)),
            ("Polars", lambda: polars_run(args.data_dir)),
            ("DuckDB", lambda: duckdb_run(args.data_dir)),
            ("DataFusion", lambda: datafusion_run(args.data_dir)),
        ]
    elif args.engine == "pandas":
        engines_to_run = [("Pandas", lambda: pandas_run(args.data_dir))]
    elif args.engine == "polars":
        engines_to_run = [("Polars", lambda: polars_run(args.data_dir))]
    elif args.engine == "duckdb":
        engines_to_run = [("DuckDB", lambda: duckdb_run(args.data_dir))]
    elif args.engine == "datafusion":
        engines_to_run = [("DataFusion", lambda: datafusion_run(args.data_dir))]

    for engine_name, engine_func in engines_to_run:
        print(f"Running {engine_name} benchmark ({args.runs} iterations)...")
        results = []
        for i in range(args.runs):
            print(f"  Iteration {i+1}/{args.runs}...", end="", flush=True)
            res = run_benchmark(engine_func, engine_name)
            results.append(res)
            print(f" Done ({res['time']}s, {res['peak_ram_gb']} GB RAM)")
        save_results(results, filename=args.output)


if __name__ == "__main__":
    main()
