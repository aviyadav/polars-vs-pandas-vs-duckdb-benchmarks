# ⚡ Local Data Analysis Benchmarks: Pandas vs. Polars vs. DuckDB

A high-performance local data analysis benchmark comparing three of the most popular data manipulation engines in Python: **Pandas**, **Polars**, and **DuckDB**. 

This project simulates a real-world analytics workload by generating a large dataset (**20,000,000 rows, ~795 MB CSV**) and running complex analytical operations (filtering, grouping, multi-column aggregation, sorting, and limiting) to find the fastest engine for local data science tasks.

---

## 📊 Benchmark Results

Here are the actual execution times observed on this system:

| Engine | Best Time (s) | Average Time (s) | Speedup vs. Pandas | Performance Rating |
| :--- | :---: | :---: | :---: | :---: |
| **🚀 Polars** | **0.537s** | **0.556s** | **13.3x faster** | 🟢 Ultra Fast (Rust-powered) |
| **🦆 DuckDB** | **0.554s** | **0.630s** | **12.9x faster** | 🟢 Ultra Fast (SQL / Vectorized) |
| **🐼 Pandas** | **7.140s** | **7.206s** | *Baseline* | 🔴 Slow (Single-threaded eager) |

### 📈 Speed Comparison Visualized
```text
Polars  ██ (0.537s)
DuckDB  ██ (0.554s)
Pandas  ██████████████████████████████████████████████████ (7.140s)
```

---

## 🛠️ The Benchmark Workload

### 1. Dataset Generation (`create-reproducable-dataset.py`)
Generates a highly realistic, reproducible tabular dataset with **20,000,000 rows** (~795 MB CSV) saved under `data/events.csv`.
* **Seed:** `42` for exact reproducibility.
* **Columns:**
  * `event_date`: Dates spanning `2024-01-01` to `2026-01-31`.
  * `country`: Categorical strings (`US`, `UK`, `DE`, `FR`, `IN`, `JP`).
  * `channel`: Acquisition channel (`search`, `social`, `email`, `direct`).
  * `user_id`: Integer range `[1, 200,000)`.
  * `order_id`: Integer range `[1, 900,000)`.
  * `revenue`: Float values drawn from a Gamma distribution (representing typical transaction size behaviors), with $15\%$ of transactions having zero revenue.

### 2. Analytical Task
Each framework executes the equivalent of the following analytical SQL query:
```sql
SELECT
    country,
    channel,
    COUNT(DISTINCT user_id) AS users,
    COUNT(order_id) AS orders,
    SUM(revenue) AS revenue
FROM read_csv('data/events.csv')
WHERE event_date >= '2025-01-01'
  AND country IN ('US', 'UK', 'DE')
  AND revenue > 0
GROUP BY country, channel
ORDER BY revenue DESC
LIMIT 10;
```

---

## 🧠 Technical Deep-Dive: Why the Gap?

### 🚀 Polars (`polars-analysis.py`)
* **Lazy Evaluation:** Polars uses a lazy execution query planner (`scan_csv`). It analyzes the query graph before execution and applies **filter pushdown** (filtering rows before reading full columns) and **projection pushdown** (only loading needed columns into memory).
* **Multithreading:** Built from the ground up in Rust, Polars leverages extremely efficient lock-free multithreading to scale operations across all available CPU cores.
* **Apache Arrow:** Uses Apache Arrow as its native memory layout, minimizing overhead and maximizing cache locality.

### 🦆 DuckDB (`duckdb-analysis.py`)
* **Vectorized Execution:** DuckDB processes data in dynamic vectors rather than row-by-row or single giant arrays, allowing modern CPUs to leverage SIMD (Single Instruction Multiple Data) instruction sets.
* **Out-of-Core Processing:** DuckDB is designed to handle datasets larger than RAM by streaming blocks of data directly from the CSV file.
* **Smart CSV Parser:** DuckDB’s parallel CSV reader is highly optimized, guessing schemas and reading chunks in parallel natively.

### 🐼 Pandas (`pandas-analysis.py`)
* **Eager Execution & RAM Bloat:** Pandas loads the entire 795 MB CSV file into memory all at once before applying filters, creating large intermediate copies.
* **Single-Threaded Bottleneck:** Core DataFrame operations in Pandas run on a single CPU core, leaving modern multi-core processors largely idle.
* **GIL Limitations:** Python's Global Interpreter Lock (GIL) limits parallel scaling during CPU-bound DataFrame operations.

---

## 📁 Repository Structure

```text
├── data/
│   └── events.csv               # Generated 20M row dataset (~795 MB)
├── benchmark-harness.py         # Multi-run benchmark runner comparing all three
├── create-reproducable-dataset.py # Generates the 20M row events CSV
├── pandas-analysis.py           # Individual Pandas implementation
├── polars-analysis.py           # Individual Polars implementation
├── duckdb-analysis.py           # Individual DuckDB implementation
├── pyproject.toml               # Project metadata & Python dependencies
└── README.md                    # Project documentation & results
```

---

## 🚀 Getting Started

### 📋 Prerequisites
* **Python:** `>= 3.13`
* **Package Manager:** `uv` (recommended for ultra-fast installs) or standard `pip`

### 🔧 Installation

Using **uv**:
```bash
# Clone the repository and navigate inside
cd pandas-polars-duckdb-fastest-local-analysis

# Run using uv to automatically set up the virtual environment and install packages
uv run benchmark-harness.py
```

Using standard **pip**:
```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r pyproject.toml
```

### 🏃 Running the Benchmarks

1. **Generate the Dataset (One-time setup):**
   ```bash
   python create-reproducable-dataset.py
   ```
   *This will generate `data/events.csv` and output generation metrics like peak memory and time.*

2. **Run the Full Benchmark Harness:**
   ```bash
   python benchmark-harness.py
   ```
   *Runs each framework 3 times and reports the best and average execution times.*

3. **Run Individual Analysis Scripts:**
   ```bash
   python pandas-analysis.py
   python polars-analysis.py
   python duckdb-analysis.py
   ```
