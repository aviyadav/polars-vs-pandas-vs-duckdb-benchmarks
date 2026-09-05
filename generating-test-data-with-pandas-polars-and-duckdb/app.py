"""Streamlit dashboard to run data-generation benchmarks and compare results.

Runs each benchmark script as a subprocess (clean memory measurement and
library isolation), appends the metrics to a parquet file, and renders
plotly comparison charts.
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_DIR = Path(__file__).resolve().parent
RESULTS_DIR = PROJECT_DIR / "results"
RESULTS_FILE = RESULTS_DIR / "benchmark_results.parquet"
DATA_DIR = PROJECT_DIR / "data"

BENCHMARKS = {
    "pandas": "generate_data_pandas.py",
    "polars": "generate_data_polars.py",
    "duckdb": "generate_data_duckdb.py",
    "pyarrow": "generate_data_pyarrow.py",
    "datafusion": "generate_data_datafusion.py",
    "cudf": "generate_data_cudf.py",
}

RESULT_COLUMNS = [
    "timestamp",
    "library",
    "row_count",
    "gen_time_s",
    "write_time_s",
    "total_time_s",
    "peak_memory_mb",
]

st.set_page_config(
    page_title="Data Generation Benchmarks", page_icon="⚡", layout="wide"
)


NUMERIC_COLUMNS = ["gen_time_s", "write_time_s", "total_time_s", "peak_memory_mb"]


def load_results() -> pd.DataFrame:
    if RESULTS_FILE.exists():
        df = pd.read_parquet(RESULTS_FILE)
    else:
        df = pd.DataFrame(columns=RESULT_COLUMNS)
    # duckdb reports None for the gen/write split; coerce to NaN so that
    # aggregation and plotting behave consistently.
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def save_results(df: pd.DataFrame) -> None:
    RESULTS_DIR.mkdir(exist_ok=True)
    df.to_parquet(RESULTS_FILE, index=False)


def run_benchmark(name: str, script: str, row_count: int) -> dict:
    """Run one benchmark script in a subprocess and return its metrics dict."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        json_path = Path(tmp.name)
    try:
        proc = subprocess.run(
            [
                sys.executable,
                str(PROJECT_DIR / script),
                "--rows",
                str(row_count),
                "--json-out",
                str(json_path),
            ],
            capture_output=True,
            text=True,
            cwd=PROJECT_DIR,
            timeout=600,
            check=False,
        )
        if proc.returncode != 0:
            stderr_tail = (
                proc.stderr.strip().splitlines()[-1] if proc.stderr else "unknown error"
            )
            raise RuntimeError(stderr_tail)
        return json.loads(json_path.read_text())
    finally:
        json_path.unlink(missing_ok=True)


def aggregate(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "Latest":
        return (
            df.sort_values("timestamp")
            .groupby("library", as_index=False)[NUMERIC_COLUMNS]
            .last()
        )
    if mode == "Minimum":
        return df.groupby("library", as_index=False)[NUMERIC_COLUMNS].min()
    return df.groupby("library", as_index=False)[NUMERIC_COLUMNS].mean()


# - - SIDEBAR: CONTROLS - -
st.sidebar.title("⚡ Benchmark Runner")

selected = st.sidebar.multiselect(
    "Benchmarks to run",
    options=list(BENCHMARKS),
    default=["pandas", "polars", "duckdb", "pyarrow", "datafusion", "cudf"],
)
row_count = st.sidebar.number_input(
    "Row count",
    min_value=1_000,
    max_value=50_000_000,
    value=1_000_000,
    step=100_000,
)
run_clicked = st.sidebar.button("🚀 Run Benchmarks", type="primary", width="stretch")

st.sidebar.divider()
if st.sidebar.button("🗑️ Clear Results History", width="stretch"):
    RESULTS_FILE.unlink(missing_ok=True)
    st.rerun()

# - - RUN BENCHMARKS - -
results_df = load_results()

if run_clicked:
    if not selected:
        st.error("Select at least one benchmark to run.")
        st.stop()

    DATA_DIR.mkdir(exist_ok=True)
    new_rows = []
    progress = st.progress(0.0, text="Running benchmarks...")

    for i, name in enumerate(selected):
        with st.status(
            f"Running **{name}** ({row_count:,} rows)...", expanded=False
        ) as status:
            try:
                metrics = run_benchmark(name, BENCHMARKS[name], row_count)
                new_rows.append(metrics)
                status.update(
                    label=(
                        f"**{name}** done — "
                        f"total {metrics['total_time_s']:.4f}s, "
                        f"peak mem {metrics['peak_memory_mb']:.1f} MB"
                    ),
                    state="complete",
                    expanded=False,
                )
            except Exception as exc:  # noqa: BLE001
                status.update(label=f"**{name}** failed", state="error", expanded=True)
                st.error(f"{name}: {exc}")
        progress.progress(
            (i + 1) / len(selected), text=f"Completed {i + 1}/{len(selected)}"
        )

    if new_rows:
        results_df = pd.concat([results_df, pd.DataFrame(new_rows)], ignore_index=True)
        save_results(results_df)
        st.toast(f"Saved {len(new_rows)} result(s) to {RESULTS_FILE.name}", icon="✅")

# - - MAIN AREA: COMPARISON CHARTS - -
st.title("Data Generation Benchmarks")
st.caption(
    "Generates synthetic event data (dates, countries, channels, IDs, revenue) "
    "with different libraries and compares generation time, CSV write time, "
    "and peak memory usage."
)

if results_df.empty:
    st.info(
        "No results yet. Select benchmarks in the sidebar and click **Run Benchmarks**."
    )
    st.stop()

# - - FILTERS - -
filter_col, agg_col, spacer = st.columns([1, 1, 2])
available_rows = sorted(results_df["row_count"].unique())
selected_rows = filter_col.selectbox(
    "Row count",
    options=available_rows,
    index=len(available_rows) - 1,
    format_func=lambda r: f"{r:,}",
)
agg_mode = agg_col.radio(
    "Aggregation across runs",
    options=["Latest", "Average", "Minimum"],
    horizontal=True,
)

filtered = results_df[results_df["row_count"] == selected_rows].copy()
n_runs = filtered.groupby("library")["timestamp"].count()

# duckdb reports no gen/write split; use NaN so its bars are simply absent
agg = aggregate(filtered, agg_mode)

# Chart 1: timings (grouped bars: generation / write / total)
chart_df = agg.rename(
    columns={
        "gen_time_s": "Generation (s)",
        "write_time_s": "Write (s)",
        "total_time_s": "Total (s)",
    }
)
time_cols = ["Generation (s)", "Write (s)", "Total (s)"]
for col in time_cols:
    chart_df[col] = chart_df[col].astype(float)

fig_time = px.bar(
    chart_df,
    x="library",
    y=time_cols,
    barmode="group",
    title=f"Timings by library — {selected_rows:,} rows ({agg_mode.lower()} of runs)",
    labels={"variable": "Phase", "value": "Seconds", "library": "Library"},
    category_orders={"variable": time_cols},
)
fig_time.update_layout(legend_title_text="Phase", hovermode="x unified")

# Chart 2: peak memory
fig_mem = px.bar(
    agg,
    x="library",
    y="peak_memory_mb",
    color="library",
    title=f"Peak memory by library — {selected_rows:,} rows ({agg_mode.lower()} of runs)",
    labels={"peak_memory_mb": "Peak Memory (MB)", "library": "Library"},
)
fig_mem.update_layout(showlegend=False)

chart1, chart2 = st.columns(2)
chart1.plotly_chart(fig_time, width="stretch")
chart2.plotly_chart(fig_mem, width="stretch")

runs_note = ", ".join(f"{lib}: {n}" for lib, n in n_runs.items())
st.caption(f"Runs included per library — {runs_note}")

# - - HISTORY - -
with st.expander("📋 Results history (all runs)", expanded=False):
    st.dataframe(
        results_df.sort_values("timestamp", ascending=False),
        width="stretch",
        column_config={
            "timestamp": st.column_config.TextColumn("Timestamp (UTC)"),
            "row_count": st.column_config.NumberColumn("Rows", format="%,d"),
            "gen_time_s": st.column_config.NumberColumn("Gen (s)", format="%.4f"),
            "write_time_s": st.column_config.NumberColumn("Write (s)", format="%.4f"),
            "total_time_s": st.column_config.NumberColumn("Total (s)", format="%.4f"),
            "peak_memory_mb": st.column_config.NumberColumn(
                "Peak Mem (MB)", format="%.2f"
            ),
        },
    )
    st.download_button(
        "⬇️ Download results as parquet",
        data=RESULTS_FILE.read_bytes() if RESULTS_FILE.exists() else b"",
        file_name="benchmark_results.parquet",
        disabled=not RESULTS_FILE.exists(),
    )
