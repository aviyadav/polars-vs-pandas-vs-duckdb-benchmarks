import time
import tracemalloc

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
import seaborn as sns
from datafusion import SessionContext

# Dataset URL
URL = "https://huggingface.co/api/resolve-cache/datasets/scikit-learn/adult-census-income/fbeef6ec0e6fd88a5028b94683144000a6b380d5/adult.csv?%2Fdatasets%2Fscikit-learn%2Fadult-census-income%2Fresolve%2Fmain%2Fadult.csv=&etag=%225cf74ede1a6de37d85c96a61d30819a694dee749%22"

def measure_performance(func, *args, **kwargs):
    tracemalloc.start()
    start_time = time.perf_counter()

    result = func(*args, **kwargs)

    end_time = time.perf_counter()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return (
        result,
        end_time - start_time,
        peak / (1024 * 1024),
    )  # Time in seconds, Peak Memory in MB


def run_pandas():
    # Read
    df = pd.read_csv(URL, engine="pyarrow", dtype_backend="pyarrow")
    # Clean
    df.replace("?", pd.NA, inplace=True)
    df.dropna(inplace=True)
    # Aggregations
    res1 = df.groupby("education")["income"].value_counts().unstack()
    res2 = df.groupby("sex")["income"].value_counts().unstack()
    res3 = df["workclass"].value_counts().head(7)
    res4 = df[df["income"] == ">50K"]["occupation"].value_counts().head(10)
    return df, res1, res2, res3, res4


def run_polars():
    # Read
    df = pl.read_csv(URL, infer_schema_length=0)  # Read all as strings to handle "?"
    # Clean: Replace "?" with None and then cast numeric columns
    df = df.with_columns(pl.all().replace("?", None))
    df = df.drop_nulls()

    # Cast numeric columns back
    numeric_cols = [
        "age",
        "fnlwgt",
        "education.num",
        "capital.gain",
        "capital.loss",
        "hours.per.week",
    ]
    df = df.with_columns([pl.col(c).cast(pl.Int64, strict=False) for c in numeric_cols])

    # Aggregations
    res1 = df.pivot(
        index="education", on="income", values="income", aggregate_function="len"
    )
    res2 = df.pivot(index="sex", on="income", values="income", aggregate_function="len")
    res3 = df["workclass"].value_counts().sort("count", descending=True).head(7)
    res4 = (
        df.filter(pl.col("income") == ">50K")["occupation"]
        .value_counts()
        .sort("count", descending=True)
        .head(10)
    )
    return df, res1, res2, res3, res4


def run_duckdb():
    con = duckdb.connect()
    # Read and Clean in one go using SQL
    # Use double quotes for columns with dots
    df = con.execute(
        f"SELECT * FROM read_csv_auto('{URL}') WHERE \"workclass\" != '?' AND \"occupation\" != '?' AND \"native.country\" != '?'"
    ).df()

    # Aggregations
    res1 = (
        con.execute(
            "SELECT education, income, count(*) as count FROM df GROUP BY education, income"
        )
        .df()
        .pivot(index="education", columns="income", values="count")
    )
    res2 = (
        con.execute(
            "SELECT sex, income, count(*) as count FROM df GROUP BY sex, income"
        )
        .df()
        .pivot(index="sex", columns="income", values="count")
    )
    res3 = con.execute(
        "SELECT workclass, count(*) as count FROM df GROUP BY workclass ORDER BY count DESC LIMIT 7"
    ).df()
    res4 = con.execute(
        "SELECT occupation, count(*) as count FROM df WHERE income = '>50K' GROUP BY occupation ORDER BY count DESC LIMIT 10"
    ).df()
    return df, res1, res2, res3, res4


def run_datafusion():
    ctx = SessionContext()
    import requests

    r = requests.get(URL)
    with open("adult_temp.csv", "wb") as f:
        f.write(r.content)

    ctx.register_csv("adult", "adult_temp.csv")
    batches = ctx.sql("SELECT * FROM adult WHERE \"workclass\" != '?'").collect()
    df_pd = pd.concat([batch.to_pandas() for batch in batches])

    res1_batches = ctx.sql(
        "SELECT education, income, count(*) as count FROM adult GROUP BY education, income"
    ).collect()
    res1_pd = pd.concat([batch.to_pandas() for batch in res1_batches]).pivot(
        index="education", columns="income", values="count"
    )

    res2_batches = ctx.sql(
        "SELECT sex, income, count(*) as count FROM adult GROUP BY sex, income"
    ).collect()
    res2_pd = pd.concat([batch.to_pandas() for batch in res2_batches]).pivot(
        index="sex", columns="income", values="count"
    )

    res3_batches = ctx.sql(
        "SELECT workclass, count(*) as count FROM adult GROUP BY workclass ORDER BY count DESC LIMIT 7"
    ).collect()
    res3_pd = pd.concat([batch.to_pandas() for batch in res3_batches])

    res4_batches = ctx.sql(
        "SELECT occupation, count(*) as count FROM adult WHERE income = '>50K' GROUP BY occupation ORDER BY count DESC LIMIT 10"
    ).collect()
    res4_pd = pd.concat([batch.to_pandas() for batch in res4_batches])

    return df_pd, res1_pd, res2_pd, res3_pd, res4_pd


def plot_results(df, res1, res2, res3, res4, lib_name):
    print(f"Plotting for {lib_name}...")
    plt.figure(figsize=(15, 10))

    # 1. Income Distribution
    plt.subplot(2, 3, 1)
    sns.countplot(x="income", data=df)
    plt.title(f"{lib_name}: Income Distribution")

    # 2. Income by Education
    plt.subplot(2, 3, 2)
    res1_pd = res1.to_pandas() if hasattr(res1, "to_pandas") else res1
    res1_pd.plot(kind="bar", ax=plt.gca())
    plt.title(f"{lib_name}: Income by Education")
    plt.xticks(rotation=45)

    # 3. Hours per week by Income
    plt.subplot(2, 3, 3)
    sns.boxplot(x="income", y="hours.per.week", data=df)
    plt.title(f"{lib_name}: Hours vs Income")

    # 4. Income by Sex
    plt.subplot(2, 3, 4)
    res2_pd = res2.to_pandas() if hasattr(res2, "to_pandas") else res2
    res2_pd.plot(kind="bar", ax=plt.gca())
    plt.title(f"{lib_name}: Income by Sex")

    # 5. Top Workclasses
    plt.subplot(2, 3, 5)
    if hasattr(res3, "to_pandas"):
        res3_pd = res3.to_pandas()
    else:
        res3_pd = res3

    if isinstance(res3_pd, pd.Series):
        x_vals = res3_pd.index
        y_vals = res3_pd.values
    elif isinstance(res3_pd, pd.DataFrame):
        x_vals = res3_pd.iloc[:, 0]
        y_vals = res3_pd.iloc[:, 1]
    else:
        x_vals = res3_pd.iloc[:, 0]
        y_vals = res3_pd.iloc[:, 1]

    sns.barplot(x=x_vals, y=y_vals)
    plt.title(f"{lib_name}: Top Workclasses")
    plt.xticks(rotation=45)

    # 6. Age vs Income
    plt.subplot(2, 3, 6)
    sns.boxplot(x="income", y="age", data=df)
    plt.title(f"{lib_name}: Age vs Income")

    plt.tight_layout()
    plt.savefig(f"results_{lib_name}.png")
    plt.close()


def main():
    libs = {
        "Pandas": run_pandas,
        "Polars": run_polars,
        "DuckDB": run_duckdb,
        "DataFusion": run_datafusion,
    }

    stats = {}

    for name, func in libs.items():
        print(f"Benchmarking {name}...")
        try:
            (df, res1, res2, res3, res4), duration, memory = measure_performance(func)
            stats[name] = {"time": duration, "memory": memory}
            plot_results(df, res1, res2, res3, res4, name)
        except Exception as e:
            print(f"Error benchmarking {name}: {e}")

    print("\nPerformance Summary:")
    print(f"{'Library':<12} | {'Time (s)':<10} | {'Peak Memory (MB)':<15}")
    print("-" * 40)
    for name, s in stats.items():
        print(f"{name:<12} | {s['time']:<10.4f} | {s['memory']:<15.2f}")

        print(f"{name:<12} | {s['time']:<10.4f} | {s['memory']:<15.2f}")

if __name__ == "__main__":
    main()
