# Rust DataFrame Benchmark

Rust equivalent of the Python DataFrame benchmark. Generates a 50 million row Parquet dataset and runs the same aggregation workload across **Polars**, **DuckDB**, and **Apache DataFusion** using their native Rust crates.

The workload: compute `revenue = price * qty`, filter for `revenue > 0`, group by `region`, sum revenue per region, sort descending.

## Engines Compared

| Engine | Approach |
| --- | --- |
| **Polars** | Lazy `LazyFrame` with Parquet scanning and query optimization |
| **DuckDB** | Embedded analytical SQL engine querying the Parquet file directly via `read_parquet` |
| **DataFusion** | Apache Arrow-native query engine executing SQL over a registered Parquet table |

## Dependencies

| Crate | Version | Purpose |
| --- | --- | --- |
| `polars` | 0.46 | DataFrame engine (features: `parquet`, `lazy`, `dtype-full`, `performant`) |
| `datafusion` | 45.0 | Arrow-based SQL query engine |
| `duckdb` | 1.1 | Embedded OLAP database (feature: `bundled` — statically links libduckdb) |
| `tokio` | 1 | Async runtime for DataFusion and the benchmark harness |
| `sysinfo` | 0.33 | Process-level CPU and RSS memory sampling |
| `rand` | 0.8 | Random data generation |
| `anyhow` | 1.0 | Ergonomic error handling |
| `async-trait` | 0.1 | Async methods on trait objects |

## Metrics Captured

Each job is wrapped in `measure_job`, an async harness that snapshots process state before and after execution using `sysinfo`:

| Metric | Description |
| --- | --- |
| `elapsed_s` | Wall-clock runtime for the job |
| `cpu_time_s` | Estimated process CPU time: `(avg_cpu_% / 100) × elapsed_s` |
| `avg_cpu_%` | Process CPU utilization at job end — `100%` means one fully used core |
| `avg_host_cpu_%` | `avg_cpu_%` divided by the logical CPU count |
| `rss_start_mb` | Process RSS before the job |
| `rss_peak_mb` | `max(rss_start, rss_end)` — best-effort peak RSS |
| `rss_end_mb` | Process RSS after the job |
| `rss_delta_mb` | `rss_end − rss_start` |
| `peak_mem_%` | `rss_peak_mb` as a percentage of total system memory |

## Requirements

- **Rust** edition 2021, toolchain `stable` 1.75 or higher
- **C / C++ compiler** — required by the `duckdb` bundled feature (statically compiles libduckdb)

Install or update the Rust toolchain via [rustup](https://rustup.rs):

```bash
rustup update stable
```

## Setup and Compilation

Navigate to the `rust-benchmark` directory and build in release mode:

```bash
cd rust-benchmark
cargo build --release
```

Or build from the parent directory using `--manifest-path`:

```bash
cargo build --release --manifest-path rust-benchmark/Cargo.toml
```

> **Note:** The first build downloads and compiles Polars, DataFusion, and DuckDB from source. Expect 5–15 minutes depending on your hardware. Subsequent builds are fast because Cargo caches compiled artifacts.

## Usage

Run the benchmark from inside the `rust-benchmark` directory:

```bash
cd rust-benchmark
cargo run --release
```

Or from the parent directory:

```bash
cargo run --release --manifest-path rust-benchmark/Cargo.toml
```

### What happens on each run

1. **Generates** `bench.parquet` (50 million rows, ~1 GB) in the working directory — the dataset is always regenerated fresh on every run and its generation time is included in the metrics table.
2. **Runs** the Polars, DuckDB, and DataFusion jobs sequentially, printing each engine's result to the console.
3. **Prints** a formatted summary table of timing and resource metrics for all jobs including data generation.

### Example output

```
Generating 50000000 rows → bench.parquet...

Running polars...
shape: (5, 2)
┌────────┬──────────┐
│ region ┆ revenue  │
│ ---    ┆ ---      │
│ str    ┆ f64      │
╞════════╪══════════╡
│ APAC   ┆ 2.2512e9 │
│ US     ┆ 2.2505e9 │
│ LATAM  ┆ 2.2504e9 │
│ EU     ┆ 2.2499e9 │
│ MEA    ┆ 2.2494e9 │
└────────┴──────────┘

Running duckdb...
DuckDB Result:
  APAC     2251230208.48
  ...

Running datafusion...
+--------+--------------------+
| region | revenue            |
+--------+--------------------+
| APAC   | 2251230208.48...   |
  ...

method          elapsed_s  cpu_time_s  avg_cpu_%  avg_host_cpu_%  rss_start_mb  rss_peak_mb   rss_end_mb   rss_delta_mb  peak_mem_%
-------------------------------------------------------------------------------------------------------------------------------------
generate_data   4.42       4.56        103.23     12.90           20.24         42.49         42.49        22.25         0.18
polars          1.23       4.83        391.94     48.99           42.55         53.26         53.26        10.71         0.22
duckdb          1.18       0.78        66.17      8.27            53.29         99.80         99.80        46.51         0.42
datafusion      0.30       1.94        635.39     79.42           100.02        224.45        224.45       124.43        0.93

CPU note: avg_cpu_% is process CPU; 100% = one fully used core.
```

## Project Structure

```text
rust-benchmark/
├── Cargo.toml          # Project metadata and pinned dependency versions
├── Cargo.lock          # Exact resolved dependency tree
├── README.md           # This file
└── src/
    └── main.rs         # Job trait, all four jobs, measure_job harness, main
```

## Architecture

### Benchmark job trait

Each engine is a struct implementing the `BenchmarkJob` trait:

```rust
#[async_trait]
trait BenchmarkJob {
    fn name(&self) -> String;
    async fn run(&self, path: &str) -> Result<()>;
}
```

Structs: `GenerateDataJob`, `PolarsJob`, `DuckDbJob`, `DataFusionJob`.

### Measurement harness

`measure_job` is an `async fn` that snapshots `sysinfo` process state before and after calling `job.run(path).await`, then records wall-clock time with `std::time::Instant`:

```rust
async fn measure_job<T: BenchmarkJob + Send + Sync + ?Sized>(
    job: &T,
    path: &str,
) -> Result<JobMetrics>
```

The `?Sized` bound is required to accept `dyn BenchmarkJob + Send + Sync` (fat pointer / unsized trait object) in addition to concrete types.

### Single tokio runtime

`main` is annotated `#[tokio::main]`, which creates one tokio runtime for the entire process. `measure_job` is `async` and awaits `job.run()` directly inside that runtime. An earlier version of this code created a **new** `tokio::runtime::Runtime` inside a sync `measure_job` — that compiles but panics at runtime with *"Cannot start a runtime from within a runtime"*.

### Explicit imports

`datafusion::prelude` and `polars::prelude` both export symbols named `DataFrame`, `col`, and `lit`. Glob-importing both (`use …::prelude::*`) causes a compiler error. All imports are therefore explicit:

```rust
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use polars::prelude::{col, lit, DataFrame, LazyFrame, NamedFrom, ParquetWriter, Series, SortMultipleOptions};
```

### Polars 0.46 API notes

- `Series::new` takes `PlSmallStr` as the column name — `&str` must be converted with `.into()`.
- `DataFrame::new` takes `Vec<Column>`, not `Vec<Series>` — each `Series` must be converted with `.into()`.

### sysinfo 0.33 API notes

`RefreshKind::new()` and `ProcessRefreshKind::new()` were renamed in 0.33. The correct constructors are:

```rust
RefreshKind::nothing().with_processes(ProcessRefreshKind::nothing().with_cpu().with_memory())
```

`Process::cpu_usage()` returns `f32`; it is cast to `f64` at the call site to match `JobMetrics` fields.

## Notes

- Always run with `--release`. Debug builds of Polars and DataFusion are significantly slower and will not reflect real-world performance.
- `bench.parquet` is written to the **working directory** where you run the binary, not relative to `Cargo.toml`. Run from inside `rust-benchmark/` or set your working directory accordingly.
- RSS is sampled only before and after each job. Very short-lived allocation spikes that occur entirely within the job and are freed before it returns will not be captured in `rss_peak_mb`.
- CPU usage is read from a single `sysinfo` refresh after the job completes. For very fast jobs the sample may not be representative.
