use anyhow::Result;
use async_trait::async_trait;
// Explicit imports eliminate the `col` / `lit` / `DataFrame` ambiguity that arises when
// both `datafusion::prelude::*` and `polars::prelude::*` are glob-imported together.
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use polars::prelude::{
    col, lit, DataFrame, LazyFrame, NamedFrom, ParquetWriter, Series, SortMultipleOptions,
};
use rand::Rng;
use std::time::Instant;
use sysinfo::{ProcessRefreshKind, RefreshKind, System};

const DEFAULT_ROWS: usize = 50_000_000;
const DEFAULT_PATH: &str = "bench.parquet";

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct JobMetrics {
    method: String,
    elapsed_s: f64,
    cpu_time_s: f64,
    avg_cpu_pct: f64,
    avg_host_cpu_pct: f64,
    rss_start_mb: f64,
    rss_peak_mb: f64,
    rss_end_mb: f64,
    rss_delta_mb: f64,
    peak_mem_pct: f64,
}

// ---------------------------------------------------------------------------
// Benchmark job trait
// ---------------------------------------------------------------------------

#[async_trait]
trait BenchmarkJob {
    fn name(&self) -> String;
    async fn run(&self, path: &str) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

struct GenerateDataJob {
    rows: usize,
}

#[async_trait]
impl BenchmarkJob for GenerateDataJob {
    fn name(&self) -> String {
        "generate_data".to_string()
    }

    async fn run(&self, path: &str) -> Result<()> {
        let mut rng = rand::thread_rng();
        let regions = ["US", "EU", "APAC", "LATAM", "MEA"];

        let mut region_col: Vec<&str> = Vec::with_capacity(self.rows);
        let mut price_col: Vec<f64> = Vec::with_capacity(self.rows);
        let mut qty_col: Vec<i32> = Vec::with_capacity(self.rows);

        for _ in 0..self.rows {
            region_col.push(regions[rng.gen_range(0..5)]);
            price_col.push(rng.gen::<f64>() * 100.0);
            qty_col.push(rng.gen_range(0..10));
        }

        // Polars 0.46: DataFrame::new takes Vec<Column>; Series::new first arg is
        // PlSmallStr so &str needs .into(), and each Series needs .into() for Column.
        let mut df = DataFrame::new(vec![
            Series::new("region".into(), region_col).into(),
            Series::new("price".into(), price_col).into(),
            Series::new("qty".into(), qty_col).into(),
        ])?;

        let file = std::fs::File::create(path)?;
        ParquetWriter::new(file).finish(&mut df)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Polars job
// ---------------------------------------------------------------------------

struct PolarsJob;

#[async_trait]
impl BenchmarkJob for PolarsJob {
    fn name(&self) -> String {
        "polars".to_string()
    }

    async fn run(&self, path: &str) -> Result<()> {
        let df = LazyFrame::scan_parquet(path, Default::default())?
            .with_column((col("price") * col("qty")).alias("revenue"))
            .filter(col("revenue").gt(lit(0.0_f64)))
            .group_by([col("region")])
            .agg([col("revenue").sum()])
            .sort_by_exprs(
                [col("revenue")],
                SortMultipleOptions::default().with_order_descending(true),
            )
            .collect()?;

        println!("{}", df);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DuckDB job
// ---------------------------------------------------------------------------

struct DuckDbJob;

#[async_trait]
impl BenchmarkJob for DuckDbJob {
    fn name(&self) -> String {
        "duckdb".to_string()
    }

    async fn run(&self, path: &str) -> Result<()> {
        let conn = duckdb::Connection::open_in_memory()?;
        let query = format!(
            "SELECT region, SUM(price * qty) AS revenue \
             FROM read_parquet('{}') \
             WHERE price * qty > 0 \
             GROUP BY region \
             ORDER BY revenue DESC",
            path
        );
        let mut stmt = conn.prepare(&query)?;
        let mut rows = stmt.query([])?;

        println!("DuckDB Result:");
        while let Some(row) = rows.next()? {
            let region: String = row.get(0)?;
            let revenue: f64 = row.get(1)?;
            println!("  {:<8} {:.2}", region, revenue);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DataFusion job
// ---------------------------------------------------------------------------

struct DataFusionJob;

#[async_trait]
impl BenchmarkJob for DataFusionJob {
    fn name(&self) -> String {
        "datafusion".to_string()
    }

    async fn run(&self, path: &str) -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_parquet("bench", path, ParquetReadOptions::default())
            .await?;

        let df = ctx
            .sql(
                "SELECT region, SUM(price * qty) AS revenue \
                 FROM bench \
                 WHERE price * qty > 0 \
                 GROUP BY region \
                 ORDER BY revenue DESC",
            )
            .await?;

        df.show().await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Measurement harness
//
// `measure_job` is async so it runs inside the existing tokio runtime created
// by `#[tokio::main]`.  The previous implementation built a *new* Runtime
// inside a sync fn, which panics at runtime ("Cannot start a runtime from
// within a runtime").
//
// The `?Sized` bound lets the function accept both concrete types and
// trait objects (`dyn BenchmarkJob + Send + Sync`).
// ---------------------------------------------------------------------------

async fn measure_job<T: BenchmarkJob + Send + Sync + ?Sized>(
    job: &T,
    path: &str,
) -> Result<JobMetrics> {
    // sysinfo 0.33 renamed ::new() to ::nothing()
    let mut sys = System::new_with_specifics(
        RefreshKind::nothing().with_processes(ProcessRefreshKind::nothing().with_cpu().with_memory()),
    );
    sys.refresh_all();

    let pid = sysinfo::get_current_pid().unwrap();

    let rss_start_mb = {
        let p = sys.process(pid).unwrap();
        p.memory() as f64 / (1024.0 * 1024.0)
    };
    let total_memory_mb = sys.total_memory() as f64 / (1024.0 * 1024.0);
    let num_cpus = sys.cpus().len() as f64;

    let start = Instant::now();
    job.run(path).await?;
    let elapsed_s = start.elapsed().as_secs_f64();

    sys.refresh_all();
    let (rss_end_mb, avg_cpu_pct) = {
        let p = sys.process(pid).unwrap();
        // cpu_usage() returns f32; cast to f64 to match JobMetrics fields
        (
            p.memory() as f64 / (1024.0 * 1024.0),
            p.cpu_usage() as f64,
        )
    };

    let cpu_time_s = (avg_cpu_pct / 100.0) * elapsed_s;
    let avg_host_cpu_pct = avg_cpu_pct / num_cpus;
    let rss_peak_mb = rss_end_mb.max(rss_start_mb);
    let rss_delta_mb = rss_end_mb - rss_start_mb;
    let peak_mem_pct = (rss_peak_mb / total_memory_mb) * 100.0;

    Ok(JobMetrics {
        method: job.name(),
        elapsed_s,
        cpu_time_s,
        avg_cpu_pct,
        avg_host_cpu_pct,
        rss_start_mb,
        rss_peak_mb,
        rss_end_mb,
        rss_delta_mb,
        peak_mem_pct,
    })
}

// ---------------------------------------------------------------------------
// Summary table
// ---------------------------------------------------------------------------

fn print_metrics(metrics: &[JobMetrics]) {
    let w = [15, 10, 11, 10, 15, 13, 13, 12, 13, 11];
    println!(
        "\n{:<w0$} {:<w1$} {:<w2$} {:<w3$} {:<w4$} {:<w5$} {:<w6$} {:<w7$} {:<w8$} {:<w9$}",
        "method",
        "elapsed_s",
        "cpu_time_s",
        "avg_cpu_%",
        "avg_host_cpu_%",
        "rss_start_mb",
        "rss_peak_mb",
        "rss_end_mb",
        "rss_delta_mb",
        "peak_mem_%",
        w0 = w[0],
        w1 = w[1],
        w2 = w[2],
        w3 = w[3],
        w4 = w[4],
        w5 = w[5],
        w6 = w[6],
        w7 = w[7],
        w8 = w[8],
        w9 = w[9],
    );
    println!("{}", "-".repeat(w.iter().sum::<usize>() + w.len()));
    for m in metrics {
        println!(
            "{:<w0$} {:<w1$.2} {:<w2$.2} {:<w3$.2} {:<w4$.2} {:<w5$.2} {:<w6$.2} {:<w7$.2} {:<w8$.2} {:<w9$.2}",
            m.method,
            m.elapsed_s,
            m.cpu_time_s,
            m.avg_cpu_pct,
            m.avg_host_cpu_pct,
            m.rss_start_mb,
            m.rss_peak_mb,
            m.rss_end_mb,
            m.rss_delta_mb,
            m.peak_mem_pct,
            w0 = w[0],
            w1 = w[1],
            w2 = w[2],
            w3 = w[3],
            w4 = w[4],
            w5 = w[5],
            w6 = w[6],
            w7 = w[7],
            w8 = w[8],
            w9 = w[9],
        );
    }
    println!("\nCPU note: avg_cpu_% is process CPU; 100% = one fully used core.");
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let path = DEFAULT_PATH;

    // Always regenerate so the dataset is fresh each run.
    // Remove this block (and just keep the job loop) to reuse an existing file.
    println!("Generating {} rows → {}...", DEFAULT_ROWS, path);
    let gen_metrics = measure_job(&GenerateDataJob { rows: DEFAULT_ROWS }, path).await?;

    let mut all_metrics = vec![gen_metrics];

    let jobs: Vec<(&str, Box<dyn BenchmarkJob + Send + Sync>)> = vec![
        ("polars",     Box::new(PolarsJob)),
        ("duckdb",     Box::new(DuckDbJob)),
        ("datafusion", Box::new(DataFusionJob)),
    ];

    for (label, job) in &jobs {
        println!("\nRunning {}...", label);
        // job.as_ref() is Box<dyn …> → &dyn …, which is accepted because of ?Sized
        let metrics = measure_job(job.as_ref(), path).await?;
        all_metrics.push(metrics);
    }

    print_metrics(&all_metrics);
    Ok(())
}
