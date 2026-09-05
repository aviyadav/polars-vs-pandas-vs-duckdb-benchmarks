# Generating Test Data: Pandas vs Polars vs DuckDB vs PyArrow vs DataFusion vs cuDF

This project benchmarks **six data libraries** — Pandas, Polars, DuckDB, PyArrow,
DataFusion, and cuDF (GPU) — for generating a synthetic event dataset and
writing it to CSV, comparing **execution time** and **peak memory usage**.
A Streamlit dashboard runs all benchmarks, captures the timings in a parquet
file, and displays them in plotly charts for comparison.

## Project Goal

Each program generates a dataset containing:

| Column       | Description                                              |
| ------------ | -------------------------------------------------------- |
| `event_date` | Random dates between 2024-01-01 and 2026-01-31           |
| `country`    | Randomly selected from `US, UK, DE, FR, IN, JP`          |
| `channel`    | Randomly selected from `search, social, email, direct`   |
| `user_id`    | Random integers between 1 and 200,000                    |
| `order_id`   | Random integers between 1 and 900,000                    |
| `revenue`    | Gamma-distributed (shape=2, scale=30) with 15% zeros     |

Each program measures:

- **Data Generation Time** — time to build the dataset in memory.
- **CSV Writing Time** — time to write the CSV file.
- **Total Elapsed Time** — generation + write.
- **Peak Memory Usage** — maximum physical RAM (RSS) used, sampled by a
  background thread every 10ms.

## Project Structure

| File                          | Library      | Engine   |
| ----------------------------- | ------------ | -------- |
| `generate_data_pandas.py`     | Pandas       | CPU      |
| `generate_data_polars.py`     | Polars       | CPU      |
| `generate_data_duckdb.py`     | DuckDB       | CPU      |
| `generate_data_pyarrow.py`    | PyArrow      | CPU      |
| `generate_data_datafusion.py` | DataFusion   | CPU      |
| `generate_data_cudf.py`       | cuDF + CuPy  | GPU      |
| `app.py`                      | Streamlit dashboard    | —        |

## Getting Started

### Prerequisites

- [uv](https://github.com/astral-sh/uv) for Python package management.
- An **NVIDIA GPU** is required only for the cuDF benchmark; all others run on CPU.

### Installation

The project is fully defined by `pyproject.toml` and `uv.lock` (the NVIDIA
package index for cuDF is already configured):

```bash
uv sync
```

## Running the Benchmarks

Every benchmark script accepts the same options:

| Option           | Description                                | Default     |
| ---------------- | ------------------------------------------ | ----------- |
| `--rows N`       | Number of rows to generate                 | `1,000,000` |
| `--json-out PATH`| Write metrics to a JSON file               | *disabled*  |

### Pandas

```bash
uv run generate_data_pandas.py
```

### Polars

```bash
uv run generate_data_polars.py
```

### DuckDB

DuckDB generates and writes in a single SQL statement, so it reports one
combined total time (no generation/write split).

```bash
uv run generate_data_duckdb.py
```

### PyArrow

```bash
uv run generate_data_pyarrow.py
```

### DataFusion

```bash
uv run generate_data_datafusion.py
```

### cuDF (GPU)

```bash
uv run generate_data_cudf.py
```

> **Note**: cuDF's native CSV writer hits a `CUDA_ERROR_ILLEGAL_ADDRESS` on
> Blackwell GPUs with CUDA 13 drivers, so this script writes via a zero-copy
> cuDF → Arrow → PyArrow CSV pipeline instead. GPU overhead dominates at small
> row counts — try 5–10M rows to see the GPU pull ahead of the CPU libraries.

### Examples

Run with a custom row count:

```bash
uv run generate_data_polars.py --rows 5000000
```

Run and capture metrics as JSON (used by the dashboard):

```bash
uv run generate_data_pandas.py --rows 1000000 --json-out metrics.json
```

## Streamlit Dashboard

Run all benchmarks from a dashboard, capture the timings in a parquet file
(`results/benchmark_results.parquet`), and compare them in plotly charts:

```bash
uv run streamlit run app.py
```

The dashboard lets you:

- Select which benchmarks to run and the row count.
- Run each benchmark in an isolated subprocess (clean memory measurements).
- Append results to a parquet history file.
- Compare generation / write / total time and peak memory per library with
  plotly charts (latest run, average, or minimum across runs).
- Browse and download the full results history.

## Output Files

Each benchmark writes its own CSV so runs don't overwrite each other:

| Program                       | Output                    |
| ----------------------------- | ------------------------- |
| `generate_data_pandas.py`     | `data/events_pd.csv`      |
| `generate_data_polars.py`     | `data/events_pl.csv`      |
| `generate_data_duckdb.py`     | `data/events_ddb.csv`     |
| `generate_data_pyarrow.py`    | `data/events_pa.csv`      |
| `generate_data_datafusion.py` | `data/events_df.csv`      |
| `generate_data_cudf.py`       | `data/events_cudf.csv`    |

The dashboard also maintains a benchmark history:

- `results/benchmark_results.parquet` — timing and memory metrics per run.

## Code Quality and Formatting

This project uses **Ruff** for linting and formatting.

### Linting

To check for linting errors:

```bash
uv run ruff check .
```

To automatically fix fixable linting errors:

```bash
uv run ruff check --fix .
```

### Formatting

To format the code according to the project style:

```bash
uv run ruff format .
```

To verify formatting without changing files:

```bash
uv run ruff format --check .
```
