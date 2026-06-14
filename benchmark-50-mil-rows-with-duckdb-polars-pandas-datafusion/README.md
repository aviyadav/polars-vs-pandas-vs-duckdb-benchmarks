# 50M Row DataFrame Benchmark

Benchmark a simple aggregation workload across Pandas, Polars, DuckDB, and DataFusion using the same Parquet dataset.

The benchmark generates or reuses `bench.parquet`, computes `revenue = price * qty`, filters positive revenue rows, groups by `region`, and sorts by total revenue descending.

## Engines Compared

- **Pandas**: eager DataFrame processing with `read_parquet`
- **Polars**: lazy Parquet scan with query optimization
- **DuckDB**: SQL query over Parquet via `read_parquet`
- **DataFusion**: SQL query over a registered Parquet table

## Metrics Captured

Each generation or benchmark job is measured independently and reported in a summary table:

| Metric | Description |
| --- | --- |
| `elapsed_s` | Wall-clock runtime for the job |
| `cpu_time_s` | Process user + system CPU time consumed during the job |
| `avg_cpu_%` | Process CPU utilization where `100%` means one fully used CPU core |
| `avg_host_cpu_%` | `avg_cpu_%` normalized by logical CPU count |
| `rss_start_mb` | Process resident memory before the job |
| `rss_peak_mb` | Peak sampled resident memory while the job ran |
| `rss_end_mb` | Process resident memory after the job |
| `rss_delta_mb` | End memory minus start memory |
| `peak_mem_%` | Peak process RSS as a percentage of total system memory |

Memory is sampled periodically while each job runs. Lower `--sample-interval` values may catch shorter memory spikes, but add slightly more measurement overhead.

## Requirements

- Python `>=3.14`
- [`uv`](https://docs.astral.sh/uv/) for dependency management

Project dependencies are declared in `pyproject.toml` and locked in `uv.lock`.

## Setup

```bash
uv sync
```

## Usage

Run the full benchmark using the default `bench.parquet` file:

```bash
uv run python main.py
```

Regenerate the default 50 million row dataset and run all jobs:

```bash
uv run python main.py --regenerate
```

Run a smaller benchmark for quick local testing:

```bash
uv run python main.py --path smoke.parquet --rows 1000 --regenerate
```

Run only selected engines:

```bash
uv run python main.py --jobs polars duckdb datafusion
```

Adjust memory sampling frequency:

```bash
uv run python main.py --sample-interval 0.05
```

## CLI Options

| Option | Default | Description |
| --- | --- | --- |
| `--path` | `bench.parquet` | Parquet file to generate or read |
| `--rows` | `50000000` | Number of rows to generate when data is missing or `--regenerate` is used |
| `--regenerate` | `false` | Recreate the Parquet file before benchmarking |
| `--sample-interval` | `0.1` | Seconds between memory samples |
| `--jobs` | all engines | One or more jobs: `pandas`, `polars`, `duckdb`, `datafusion` |

## Project Structure

```text
.
├── main.py          # Data generation, benchmark jobs, and metrics harness
├── pyproject.toml  # Project metadata and dependencies
├── uv.lock         # Locked dependency versions
└── bench.parquet   # Generated benchmark dataset
```

## Notes

- The default benchmark size is intentionally large and can require significant memory and disk space.
- Existing Parquet data is reused unless `--regenerate` is passed.
- CPU percentages are process-level measurements from `psutil`, not whole-system profiler traces.
- Peak memory is sampled RSS, so very short-lived spikes between samples may not be captured.
