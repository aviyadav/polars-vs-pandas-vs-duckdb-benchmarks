# PyArrow + Apache DataFusion Medallion Lakehouse — Incremental (v2)

A **rerunnable, incremental** PyArrow/DataFusion implementation of the Bronze → Silver → Gold lakehouse. It is the PyArrow + DataFusion counterpart of [`duck_impl_v2`](../duck_impl_v2/), which keeps the same pipeline semantics in a persistent DuckDB file.

Like the base [PyArrow implementation](../arrow_impl/), all state is open **Apache Parquet**. Unlike the v1 pipeline — which re-created each layer from scratch on every run — v2 only processes **new** data:

- `storage/bronze/` — one Parquet file per newly landed CSV (raw, all-string rows with `_ingested_at`, `_source_file`)
- `storage/silver/` — one Parquet file per promoted Bronze file (typed, cleaned, validated rows with `_processed_at`)
- `storage/gold/` — analytical models recomputed from Silver: `fct_orders.parquet`, `dim_users.parquet`, `daily_channel_performance.parquet`

Re-running the pipeline never duplicates rows: already-landed CSVs and already-promoted Bronze files are skipped.

---

## Layout

```text
arrow_impl_v2/
├── storage/
│   ├── bronze/                            # one raw Parquet file per landed CSV
│   ├── silver/                            # one cleaned Parquet file per promoted Bronze file
│   └── gold/                              # recomputed marts (fact / dim / daily mart)
├── generate_dirty_data.py                 # writes data/events_arr_<timestamp>.csv
├── load_bronze.py                         # lands only new events_arr_*.csv files
├── load_silver.py                         # promotes only unprocessed Bronze files
├── load_gold.py                           # recomputes Gold models from Silver
├── run_pipeline.py                        # bronze → silver → gold orchestrator (with logs)
└── README.md
```

---

## Run

From the repository root:

```bash
# 1. Generate a timestamped raw CSV into ../data/ (optional if one already exists)
uv run python arrow_impl_v2/generate_dirty_data.py --rows 1000000

# 2. Land new CSVs → Bronze Parquet, promote → Silver, model → Gold
uv run python arrow_impl_v2/run_pipeline.py
```

Individual stages can be run directly:

```bash
uv run python arrow_impl_v2/load_bronze.py
uv run python arrow_impl_v2/load_silver.py
uv run python arrow_impl_v2/load_gold.py
```

---

## Incremental Semantics

| Stage | DuckDB v2 | PyArrow/DataFusion v2 |
|---|---|---|
| Bronze | `INSERT ... WHERE _source_file NOT IN (...)` | skip CSV if `storage/bronze/<name>.parquet` exists |
| Silver | watermark on `MAX(_ingested_at)` + anti-join on `order_id` | promote only Bronze files with no Silver counterpart + anti-join on `order_id` |
| Gold | `PRIMARY KEY` + `ON CONFLICT` / `QUALIFY` incremental upserts | deterministic recompute + copy-on-write Parquet overwrite |

Because Silver is append-only and an `order_id` is inserted at most once, the final Gold state after full recompute matches DuckDB's incremental `ON CONFLICT` result.

```bash
# Rerunning is a no-op for Bronze/Silver and keeps Gold identical
uv run python arrow_impl_v2/run_pipeline.py
```

---

## 🔔 Failure Alerts via HTTP Health Endpoint

The orchestrator pings a monitoring dashboard at run start, on stage failure,
and on successful completion. Configure the endpoint with an environment
variable (Healthchecks.io-style ping URLs, Uptime Kuma push monitors, or any
HTTP endpoint that accepts POSTs):

| Env Variable | Description |
|---|---|
| `PIPELINE_HEALTH_URL` | Base ping URL, e.g. `https://hc-ping.com/<uuid>`. Empty/unset = alerts disabled. |
| `PIPELINE_HEALTH_TIMEOUT` | Request timeout in seconds (default: `10`). |

Signals sent by [`run_pipeline.py`](run_pipeline.py):

| Signal | Endpoint | Meaning |
|---|---|---|
| Start | `POST <url>/start` | run began (with `run_id`) |
| Failure | `POST <url>/fail` | a stage failed (stage, error, elapsed included in JSON body) |
| Success | `POST <url>` | run completed (total elapsed included in JSON body) |

```bash
# Example with Healthchecks.io
PIPELINE_HEALTH_URL="https://hc-ping.com/your-uuid" \
    uv run python arrow_impl_v2/run_pipeline.py
```

Health pings are best-effort: if the URL is unset or the endpoint is
down, the pipeline logs a warning and continues. The alerting logic lives in
[`health.py`](health.py) (`notify_start`, `notify_success`, `notify_failure`).

---

## Querying the results

```python
from datafusion import SessionContext

ctx = SessionContext()
ctx.register_parquet("fact", "arrow_impl_v2/storage/gold/fct_orders.parquet")
ctx.register_parquet(
    "mart", "arrow_impl_v2/storage/gold/daily_channel_performance.parquet"
)

ctx.sql("""
    SELECT country, channel,
           SUM(total_revenue) AS revenue,
           SUM(total_orders)  AS orders
    FROM mart
    GROUP BY country, channel
    ORDER BY revenue DESC
    LIMIT 5
""").show()
```

Parquet is engine-agnostic, so the same files can be queried by DuckDB, Polars, Spark, etc.
