# Local Analytics with DuckDB, Polars, FastAPI, and Parquet

This project is a local analytics environment for synthetic internet-traffic
data. It uses:

- **Python 3.14** managed with [`uv`](https://docs.astral.sh/uv/)
- **DuckDB** for local SQL analytics
- **Polars** for DataFrame operations
- **PyArrow** for streaming Parquet writes
- **FastAPI/Uvicorn** for serving the application

All commands in this README are intended to be run from the **project root
folder**.

## Project layout

```text
.
├── app/
│   ├── database.py                 # DuckDB connection helper
│   ├── main.py                     # Analytics entry point / FastAPI module
│   ├── service.py                  # Analytics service layer
│   └── traffic_repository.py       # DuckDB queries over Parquet
├── data/
│   └── jun_final_v2.parquet        # Generated traffic data
├── generate_data.py                # Parallel chunked data generator
├── pyproject.toml                  # Project metadata and dependencies
└── uv.lock                         # Locked dependency versions
```

## Setup

Install and sync dependencies:

```bash
uv sync
```

## Run the project from the project root

### Run the analytics script

The current analytics entry point can be run as a module:

```bash
uv run python -m app.main
```

It can also be run directly as a script:

```bash
uv run python app/main.py
```

Both commands read `data/jun_final_v2.parquet`, print sample rows, show
rejected traffic, and print monthly traffic totals.

### Run FastAPI with Uvicorn

Use this command from the project root:

```bash
uv run uvicorn app.main:app --reload
```

This is the supported FastAPI startup command for the project.

## Generate the default dataset

The default command generates **250,000 rows** and writes:

```text
data/jun_final_v2.parquet
```

Run it with:

```bash
uv run python generate_data.py
```

The program prints:

- Number of rows, chunks, and worker processes
- Progress as chunks are written
- Total elapsed time
- Rows per second
- Output file size
- A final Parquet row-count verification

## Generated data schema

The generated file contains the requested columns `year`, `month`, `byt`,
`src`, `dst`, and `act`, plus commonly used network-traffic columns:

| Column | Description |
|---|---|
| `timestamp` | Synthetic event timestamp |
| `year` | Event year |
| `month` | Event month, 1-12 |
| `day` | Day of month |
| `src` | Source IPv4 address stored as an unsigned 32-bit integer |
| `dst` | Destination IPv4 address stored as an unsigned 32-bit integer |
| `src_port` | Source TCP/UDP port |
| `dst_port` | Destination port, biased toward common service ports |
| `protocol` | `TCP`, `UDP`, or `ICMP` |
| `act` | `ALLOW`, `BLOCK`, or `DROP` |
| `byt` | Bytes transferred |

## Generator scenarios

### Generate a different number of rows

The generator accepts any positive row count. It writes incrementally, so the
whole dataset is not held in memory:

```bash
uv run python generate_data.py --rows 1000000
uv run python generate_data.py --rows 25000000
```

By default these commands overwrite `data/jun_final_v2.parquet`.

### Generate a custom output file

Use `--output` when you want to preserve the default file:

```bash
uv run python generate_data.py \
  --rows 1000000 \
  --output data/traffic_1m.parquet
```

Relative output paths are resolved from the project root. Parent directories
are created automatically.

### Control chunk size

Chunks are generated in worker processes and written one at a time. Smaller
chunks reduce the memory required per in-flight result; larger chunks may be
faster:

```bash
# Lower memory usage
uv run python generate_data.py --rows 1000000 --chunk-rows 25000

# Larger chunks for a machine with more available memory
uv run python generate_data.py --rows 1000000 --chunk-rows 250000
```

Approximate peak memory is proportional to:

```text
number of worker processes × a small number of in-flight chunks
```

It does not grow with total row count. If the machine is memory constrained,
use both a smaller chunk size and fewer processes.

### Control multiprocessing

The default worker count is limited by the number of chunks and otherwise uses
the available CPU count. Set it explicitly when sharing a machine:

```bash
uv run python generate_data.py --rows 1000000 --processes 2
uv run python generate_data.py --rows 1000000 --processes 8
```

### Generate a different year

```bash
uv run python generate_data.py --rows 500000 --year 2025
```

### Use a reproducible seed

The default seed is `42`. Use `--seed` to create a different deterministic
dataset. Running the same command with the same seed produces the same output:

```bash
uv run python generate_data.py --rows 500000 --seed 123
```

### Combine generator options

```bash
uv run python generate_data.py \
  --rows 10000000 \
  --chunk-rows 50000 \
  --processes 4 \
  --year 2025 \
  --seed 123 \
  --output data/traffic_10m_2025.parquet
```

### See all generator options

```bash
uv run python generate_data.py --help
```

## Read and query the Parquet file

### Query with DuckDB from the command line

```bash
uv run python -c "import duckdb; con = duckdb.connect(); print(con.sql(\"SELECT * FROM 'data/jun_final_v2.parquet' LIMIT 10\"))"
```

Count rows and inspect the schema:

```bash
uv run python -c "import duckdb; con = duckdb.connect(); print(con.sql(\"DESCRIBE SELECT * FROM 'data/jun_final_v2.parquet'\")); print(con.sql(\"SELECT count(*) FROM 'data/jun_final_v2.parquet'\"))"
```

Run an aggregation:

```bash
uv run python -c "import duckdb; con = duckdb.connect(); print(con.sql(\"SELECT year, month, sum(byt) AS total_bytes FROM 'data/jun_final_v2.parquet' GROUP BY year, month ORDER BY year, month\"))"
```

### Read the file with Polars

```bash
uv run python -c "import polars as pl; df = pl.read_parquet('data/jun_final_v2.parquet'); print(df.head()); print(df.shape); print(df.schema)"
```

For larger files, use a lazy scan instead of loading the entire file at once:

```bash
uv run python -c "import polars as pl; result = pl.scan_parquet('data/jun_final_v2.parquet').group_by(['year', 'month']).agg(pl.col('byt').sum().alias('total_bytes')).sort(['year', 'month']).collect(); print(result)"
```

## Validate the generated file

The generator automatically validates the row count after writing. You can
also validate an existing Parquet file with DuckDB:

```bash
uv run python -c "import duckdb; con = duckdb.connect(); print(con.sql(\"SELECT count(*) AS rows, min(year) AS min_year, max(year) AS max_year, count(DISTINCT src) AS unique_sources, count(DISTINCT dst) AS unique_destinations FROM 'data/jun_final_v2.parquet'\"))"
```

Inspect the file metadata and row groups with PyArrow:

```bash
uv run python -c "import pyarrow.parquet as pq; m = pq.read_metadata('data/jun_final_v2.parquet'); print(f'rows={m.num_rows:,}, row_groups={m.num_row_groups}, columns={m.num_columns}')"
```

## Timing and memory behavior

Every generation run is timed with `time.perf_counter()` and reports rows per
second. Generation uses `multiprocessing` with the `spawn` context and writes
chunks through `pyarrow.parquet.ParquetWriter`.

This means:

1. Worker processes generate independent chunks in parallel.
2. The parent process receives a bounded stream of chunks.
3. Each chunk is written as a Parquet row group immediately.
4. Total output size can grow without requiring the complete dataset in RAM.

For a memory-constrained run:

```bash
uv run python generate_data.py \
  --rows 25000000 \
  --chunk-rows 25000 \
  --processes 2
```

For a faster run on a larger machine:

```bash
uv run python generate_data.py \
  --rows 25000000 \
  --chunk-rows 250000 \
  --processes 8
```

## Dependency management

Add or update dependencies with `uv`, rather than installing packages manually:

```bash
uv add numpy
uv add <package-name>
uv lock
uv sync
```

The data generator currently relies on NumPy, PyArrow, and the existing project
environment. Generated Parquet files are runtime data and should generally not
be committed if they become large.
