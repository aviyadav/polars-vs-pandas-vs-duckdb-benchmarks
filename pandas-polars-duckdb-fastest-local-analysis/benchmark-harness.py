from time import perf_counter

import duckdb
import pandas as pd
import polars as pl


COUNTRIES = ["US", "UK", "DE"]


def pandas_workflow():
    df = pd.read_csv("data/events.csv", parse_dates=["event_date"])
    return (
        df.loc[
            (df["event_date"] >= "2025-01-01")
            & (df["country"].isin(COUNTRIES))
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
    )


def polars_workflow():
    return (
        pl.scan_csv("data/events.csv", try_parse_dates=True)
        .filter(
            (pl.col("event_date") >= pl.date(2025, 1, 1))
            & (pl.col("country").is_in(COUNTRIES))
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


def duckdb_workflow():
    return duckdb.sql(
        """
        SELECT
            country,
            channel,
            COUNT(DISTINCT user_id) AS users,
            COUNT(order_id) AS orders,
            SUM(revenue) AS revenue
        FROM read_csv('data/events.csv', header = true)
        WHERE CAST(event_date AS DATE) >= DATE '2025-01-01'
          AND country IN ('US', 'UK', 'DE')
          AND revenue > 0
        GROUP BY country, channel
        ORDER BY revenue DESC
        LIMIT 10;
        """
    ).df()


def benchmark(label, func, repeat=3):
    times = []
    output = None

    for _ in range(repeat):
        start = perf_counter()
        output = func()
        times.append(perf_counter() - start)

    print(f"{label:8} best={min(times):.3f}s avg={sum(times) / len(times):.3f}s")
    return output


if __name__ == "__main__":
    benchmark("pandas", pandas_workflow)
    benchmark("polars", polars_workflow)
    benchmark("duckdb", duckdb_workflow)