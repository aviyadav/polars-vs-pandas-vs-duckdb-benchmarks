# Data Processing Benchmark: Pandas vs Polars vs DuckDB vs Apache DataFusion

A benchmark suite for evaluating large-scale analytical data processing performance across four major Python data processing engines: **Pandas**, **Polars**, **DuckDB**, and **Apache DataFusion**.

The benchmark executes an analytical star-schema query over **50 Million synthetic sales records** (~1.45 GB snappy-compressed Parquet fact table) joined with **5 dimension tables** (Category, Date, Store, Vendor, Item), producing ~16.7M aggregated result rows.

---

## 📊 Benchmarking Engines

| Engine | Description | Execution Strategy |
| :--- | :--- | :--- |
| **Pandas** | Standard Python data analysis library | Eager, in-memory DataFrame merges & groupby aggregations |
| **Polars** | High-performance Rust-based DataFrame library | Lazy evaluation (`scan_parquet`), query optimization, multi-threaded execution |
| **DuckDB** | In-process analytical SQL database | Vectorized SQL query engine scanning Parquet directly |
| **Apache DataFusion** | Extensible Rust Arrow SQL query engine | Arrow `SessionContext` registration, multi-threaded SQL physical execution |

---

## 🏁 Key Findings

Measured on the 50M-row fact table (5-run averages, see `benchmark_results.csv`):

| Engine | Avg Time | Peak RAM | Verdict |
| :--- | :--- | :--- | :--- |
| **Polars** | ~8.3 s | ~20 GB | 🥇 Fastest — lazy streaming + aggressive multi-threading |
| **DuckDB** | ~14.0 s | ~17 GB | 🥈 Fast and memory-efficient, great for SQL workloads |
| **Pandas** | ~73.8 s | ~24 GB | 🐢 **Slow** — eager in-memory merges on 50M rows are ~9x slower than Polars and use the most RAM. Not recommended for data at this scale. |
| **DataFusion** | ⚠️ problematic | — | ❌ **Not a good candidate for larger volumes of data** (see below) |

### ⚠️ DataFusion caveats

- **Poor scalability on large joins/aggregations.** While Polars and DuckDB complete the full 50M-row star query in seconds, DataFusion takes dramatically longer on the same workload — a single iteration can run for many minutes, making it impractical for interactive benchmarking at this volume.
- **Correctness concerns observed.** In some runs DataFusion returned only ~33K result rows instead of the expected ~16.7M rows produced by the other engines, indicating the query was not computing the same result. Results from DataFusion should be validated row-for-row before trusting timings.
- DataFusion is a solid embeddable query engine for smaller/medium datasets and as a building block (it powers e.g. `dask-sql`, Ballista), but for standalone large-volume analytical queries in Python, **Polars or DuckDB are strongly preferred**.

### 🐢 Pandas caveat

Pandas performs all joins eagerly in memory with limited parallelism. At 50M rows it is ~5–9x slower than DuckDB/Polars and has the highest peak memory (~24 GB). Fine for small/medium data; avoid for large-scale aggregations.

---

## 📁 Repository Structure

```
├── benchmark.py            # CLI benchmark suite (argparse-based, per-engine runs, metrics logging)
├── benchmark33m.ipynb      # Jupyter notebook version of the benchmarks
├── generate_data.py        # Parallel synthetic data generator for Iowa Liquor Sales dataset (50M fact + 5 dimensions)
├── pyproject.toml          # Project dependencies (uv package manager)
├── uv.lock                 # Locked dependency versions
├── benchmark_results.csv   # Output CSV logging benchmark execution metrics (time, RAM, CPU, rows)
└── data/                   # Generated snappy Parquet datasets
    ├── iowa_sales.snappy.parquet     (50,000,000 rows, ~1.45 GB)
    ├── iowa_category.snappy.parquet  (300 rows)
    ├── iowa_date.snappy.parquet      (5,479 rows)
    ├── iowa_store.snappy.parquet     (2,500 rows)
    ├── iowa_vendor.snappy.parquet    (600 rows)
    └── iowa_item.snappy.parquet      (50,000 rows)
```

---

## ⚡ Quick Start

### 1. Prerequisites & Environment Setup

This project uses [`uv`](https://github.com/astral-sh/uv) for fast Python package management.

```bash
# Sync dependencies
uv sync
```

### 2. Generate Synthetic Benchmark Dataset

Generate 50 million sales fact records and 5 dimension tables using multi-process parallel generation:

```bash
uv run python generate_data.py
```

This creates `data/*.snappy.parquet` files and performs schema and query verification across Polars, DuckDB, and DataFusion.

### 3. Run Benchmarks

**CLI (recommended):**

```bash
# Run all engines, 5 iterations each (default)
uv run python benchmark.py

# Run a single engine
uv run python benchmark.py --engine polars --runs 3

# Options
uv run python benchmark.py --help
```

| Flag | Description | Default |
| :--- | :--- | :--- |
| `--engine, -e` | `pandas` \| `polars` \| `duckdb` \| `datafusion` \| `all` | `all` |
| `--runs, -r` | Iterations per engine | `5` |
| `--output, -o` | Results CSV path | `benchmark_results.csv` |
| `--data-dir, -d` | Parquet dataset directory | `data` |
| `--summary` | Print dataset summary before benchmarking | off |

**Notebook:** launch `uv run jupyter lab` and open `benchmark33m.ipynb`, then configure engine flags in Cell 2.

---

## 📈 Measured Metrics

Each benchmark run measures:

- **Elapsed Time (`time`)**: Wall-clock execution time in seconds.
- **Peak RAM (`peak_ram_gb`)**: Maximum RSS memory consumed during execution in GB (sampled via a background `psutil` monitor thread).
- **Peak CPU (`peak_cpu_pct`)**: Peak multi-core CPU usage percentage during execution.
- **Output Rows (`rows`)**: Total number of result rows generated — useful as a cross-engine correctness check (all engines should produce 16,666,268 rows).

Results are appended to `benchmark_results.csv` along with per-engine averages (`*_AVG`).

---

## 🔍 Analytical Query Description

The benchmark performs an identical star-schema join and aggregation in every engine:

1. Joins `FactSales` (50M rows) with `DimCategory`, `DimDate`, `DimStore`, `DimVendor`, and `DimItem`.
2. Filters sales for `CalendarYear >= 2022`.
3. Groups by `CalendarYear`, `County`, `StoreName`, `VendorName`, `CategoryName`, `ItemName`.
4. Aggregates:
   - `total_sales`: `SUM(SaleDollars)`
   - `bottles_sold`: `SUM(BottlesSold)`
   - `volume_liters`: `SUM(VolumeSoldLiters)`
   - `avg_retail`: `AVG(StateBottleRetail)`
   - `avg_cost`: `AVG(StateBottleCost)`
   - `transactions`: `COUNT(ID)`
5. Sorts results deterministically by dimensions ascending and sales metrics descending.

---

## 🧠 Recommendations

- **For large-scale analytical processing in Python:** use **Polars** (DataFrame API) or **DuckDB** (SQL). Both are fast, memory-efficient, and produced identical, correct results.
- **Pandas:** acceptable for small/medium datasets; too slow and memory-hungry at 50M+ rows.
- **DataFusion:** not recommended for large data volumes in this workload — extremely slow execution and inconsistent result cardinality were observed. If you use it, always verify output row counts against another engine.
