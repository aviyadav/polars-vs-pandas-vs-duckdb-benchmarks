# Local Data Analysis — Pandas vs DuckDB vs Polars vs PyArrow vs DataFusion

A benchmarking project that generates a synthetic events dataset and runs the
same analytical query against **Pandas**, **DuckDB**, **Polars**, **PyArrow**, and
**DataFusion**, measuring execution time and peak memory for each engine.

## Prerequisites

- Python ≥ 3.13
- [uv](https://docs.astral.sh/uv/) (package manager)

Install dependencies:

```sh
uv sync
```

## Project structure

```
src/local_data_analysis/
├── __init__.py            # Package init
├── generate_events.py     # Parallel CSV data generator
├── pandas_analysis.py     # Pandas-based analytics query
├── polars_analysis.py     # Polars lazy-API analytics query
├── duckdb_analysis.py     # DuckDB SQL analytics query
├── pyarrow_analysis.py    # PyArrow compute-based analytics query
└── datafusion_analysis.py # DataFusion SQL analytics query

data/
└── events.csv             # Generated dataset (100K rows x 13 cols)
```

## Step 1 — Generate the dataset

Creates `data/events.csv` with 100,000 fake records using multiprocessing
for speed and batch streaming for low memory usage.

```sh
uv run python -m local_data_analysis.generate_events
```

**CSV schema:** `country`, `channel`, `user_id`, `order_id`, `revenue`,
`event_date`, `event_type`, `device`, `currency`, `quantity`,
`session_duration_sec`, `is_new_customer`, `discount_pct`

## Step 2 — Run the benchmarks

Each script runs the same analytical query (filter by date, country, revenue;
group by country + channel; aggregate users, orders, revenue; top 10) and
reports execution time and peak memory.

```sh
uv run python src/local_data_analysis/pandas_analysis.py
uv run python src/local_data_analysis/polars_analysis.py
uv run python src/local_data_analysis/duckdb_analysis.py
uv run python src/local_data_analysis/pyarrow_analysis.py
uv run python src/local_data_analysis/datafusion_analysis.py
```

## Results (example)

| Engine  | Execution time | Peak memory |
| ------- | -------------- | ----------- |
| Pandas  | —              | —           |
| Polars  | —              | —           |
| DuckDB     | —              | —           |
| PyArrow    | —              | —           |
| DataFusion | —              | —           |

*Run the scripts above to populate your own numbers.*

## convert csv to parquet
```sh
uv run python -m local_data_analysis.convert_to_parquet

or 
uv run python src/local_data_analysis/convert_to_parquet.py
```
