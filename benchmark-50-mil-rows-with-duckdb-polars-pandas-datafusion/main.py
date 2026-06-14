from __future__ import annotations

import argparse
import gc
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import datafusion
import duckdb
import numpy as np
import pandas as pd
import polars as pl
import psutil

DEFAULT_ROW_COUNT = 50_000_000
DEFAULT_PARQUET_PATH = Path("bench.parquet")
REGIONS = np.array(["US", "EU", "APAC", "LATAM", "MEA"])


@dataclass(frozen=True)
class JobMetrics:
    method: str
    elapsed_seconds: float
    cpu_time_seconds: float
    avg_cpu_percent: float
    avg_host_cpu_percent: float
    rss_start_mb: float
    rss_peak_mb: float
    rss_end_mb: float
    rss_delta_mb: float
    peak_memory_percent: float


@dataclass(frozen=True)
class BenchmarkResult:
    metrics: JobMetrics
    output: Any


BenchmarkJob = Callable[[Path], Any]


class MemorySampler:
    """Sample process RSS while a benchmark job runs."""

    def __init__(self, process: psutil.Process, interval_seconds: float) -> None:
        self._process = process
        self._interval_seconds = interval_seconds
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self.peak_rss_bytes = self._process.memory_info().rss

    def __enter__(self) -> MemorySampler:
        self._thread.start()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self._stopped.set()
        self._thread.join()

    def _sample(self) -> None:
        while not self._stopped.wait(self._interval_seconds):
            self.peak_rss_bytes = max(
                self.peak_rss_bytes,
                self._process.memory_info().rss,
            )


def bytes_to_mb(value: int) -> float:
    return value / (1024 * 1024)


def cpu_seconds(cpu_times: Any) -> float:
    return float(cpu_times.user + cpu_times.system)


def generate_data(parquet_path: Path, row_count: int = DEFAULT_ROW_COUNT) -> Path:
    rng = np.random.default_rng(42)

    pd.DataFrame(
        {
            "region": REGIONS[rng.integers(0, len(REGIONS), row_count)],
            "price": rng.random(row_count) * 100,
            "qty": rng.integers(0, 10, row_count),
        }
    ).to_parquet(parquet_path)

    return parquet_path


def pandas_job(parquet_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)
    df["revenue"] = df["price"] * df["qty"]

    filtered = df.loc[df["revenue"] > 0, ["region", "revenue"]]
    grouped = filtered.groupby("region", as_index=False).agg(revenue=("revenue", "sum"))
    return grouped.sort_values(by="revenue", ascending=False)


def polars_job(parquet_path: Path) -> pl.DataFrame:
    return (
        pl.scan_parquet(parquet_path)
        .with_columns((pl.col("price") * pl.col("qty")).alias("revenue"))
        .filter(pl.col("revenue") > 0)
        .group_by("region")
        .agg(pl.col("revenue").sum())
        .sort("revenue", descending=True)
        .collect()
    )


def duckdb_job(parquet_path: Path) -> pd.DataFrame:
    return duckdb.sql(
        """
        SELECT region, SUM(price * qty) AS revenue
        FROM read_parquet($parquet_path)
        WHERE price * qty > 0
        GROUP BY region
        ORDER BY revenue DESC
        """,
        params={"parquet_path": str(parquet_path)},
    ).df()


def datafusion_job(parquet_path: Path) -> pd.DataFrame:
    ctx = datafusion.SessionContext()
    ctx.register_parquet("bench", str(parquet_path))
    return ctx.sql(
        """
        SELECT region, SUM(price * qty) AS revenue
        FROM bench
        WHERE price * qty > 0
        GROUP BY region
        ORDER BY revenue DESC
        """
    ).to_pandas()


def measure_job(
    method: str,
    job: BenchmarkJob,
    parquet_path: Path,
    sample_interval_seconds: float,
) -> BenchmarkResult:
    process = psutil.Process(os.getpid())
    cpu_count = psutil.cpu_count() or 1

    gc.collect()
    rss_start = process.memory_info().rss
    cpu_start = cpu_seconds(process.cpu_times())
    started_at = time.perf_counter()

    with MemorySampler(process, sample_interval_seconds) as sampler:
        output = job(parquet_path)

    elapsed_seconds = time.perf_counter() - started_at
    cpu_end = cpu_seconds(process.cpu_times())
    rss_end = process.memory_info().rss
    rss_peak = max(sampler.peak_rss_bytes, rss_start, rss_end)
    cpu_time_seconds = cpu_end - cpu_start
    avg_cpu_percent = (
        (cpu_time_seconds / elapsed_seconds) * 100 if elapsed_seconds else 0.0
    )

    metrics = JobMetrics(
        method=method,
        elapsed_seconds=elapsed_seconds,
        cpu_time_seconds=cpu_time_seconds,
        avg_cpu_percent=avg_cpu_percent,
        avg_host_cpu_percent=avg_cpu_percent / cpu_count,
        rss_start_mb=bytes_to_mb(rss_start),
        rss_peak_mb=bytes_to_mb(rss_peak),
        rss_end_mb=bytes_to_mb(rss_end),
        rss_delta_mb=bytes_to_mb(rss_end - rss_start),
        peak_memory_percent=(rss_peak / psutil.virtual_memory().total) * 100,
    )

    gc.collect()
    return BenchmarkResult(metrics=metrics, output=output)


def metrics_to_dataframe(results: list[BenchmarkResult]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "method": result.metrics.method,
                "elapsed_s": result.metrics.elapsed_seconds,
                "cpu_time_s": result.metrics.cpu_time_seconds,
                "avg_cpu_%": result.metrics.avg_cpu_percent,
                "avg_host_cpu_%": result.metrics.avg_host_cpu_percent,
                "rss_start_mb": result.metrics.rss_start_mb,
                "rss_peak_mb": result.metrics.rss_peak_mb,
                "rss_end_mb": result.metrics.rss_end_mb,
                "rss_delta_mb": result.metrics.rss_delta_mb,
                "peak_mem_%": result.metrics.peak_memory_percent,
            }
            for result in results
        ]
    ).round(2)


def print_result(result: BenchmarkResult) -> None:
    print(f"\n{result.metrics.method} result")
    print(result.output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark DataFrame engines on a parquet dataset."
    )
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_PARQUET_PATH,
        help="Parquet file to generate/read.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_ROW_COUNT,
        help="Rows to generate when the parquet file is missing or --regenerate is used.",
    )
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Recreate the parquet file before running benchmarks.",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=0.1,
        help="Seconds between memory samples while each job runs.",
    )
    parser.add_argument(
        "--jobs",
        nargs="+",
        choices=sorted(JOBS),
        default=list(JOBS),
        help="Benchmark jobs to run.",
    )

    args = parser.parse_args()
    if args.rows <= 0:
        parser.error("--rows must be greater than 0")
    if args.sample_interval <= 0:
        parser.error("--sample-interval must be greater than 0")
    return args


JOBS: dict[str, BenchmarkJob] = {
    "pandas": pandas_job,
    "polars": polars_job,
    "duckdb": duckdb_job,
    "datafusion": datafusion_job,
}


def main() -> None:
    args = parse_args()
    results: list[BenchmarkResult] = []

    if args.regenerate or not args.path.exists():
        print(f"Generating {args.rows:,} rows at {args.path}...")
        generate_result = measure_job(
            "generate_data",
            lambda _path: generate_data(args.path, args.rows),
            args.path,
            args.sample_interval,
        )
        results.append(generate_result)
    else:
        print(f"Using existing parquet file: {args.path}")

    for job_name in args.jobs:
        print(f"\nRunning {job_name}...")
        result = measure_job(job_name, JOBS[job_name], args.path, args.sample_interval)
        print_result(result)
        results.append(result)

    print("\nBenchmark metrics")
    print(metrics_to_dataframe(results).to_string(index=False))
    print("\nCPU note: avg_cpu_% is process CPU where 100% means one fully used core.")


if __name__ == "__main__":
    main()
