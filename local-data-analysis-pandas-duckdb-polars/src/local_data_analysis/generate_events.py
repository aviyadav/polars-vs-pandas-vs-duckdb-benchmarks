"""
Parallel batch-based CSV data generation for events data.

Generates 100,000 fake event records across multiple CPU cores,
streaming output to disk to keep memory usage low throughout.
"""

from __future__ import annotations

import atexit
import logging
import os
import shutil
import tempfile
import time
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import NamedTuple

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TOTAL_RECORDS: int = 1_000_000
BATCH_SIZE: int = 50_000  # records per batch – keeps per-core memory ~2-4 MB
DATA_DIR: Path = Path("data")
OUTPUT_FILE: Path = DATA_DIR / "events.csv"

COUNTRIES: list[str] = [
    "US", "GB", "DE", "FR", "JP", "CA", "AU", "IN", "BR", "MX",
    "IT", "ES", "NL", "SE", "KR", "CN", "RU", "ZA", "AR", "SG",
    "CH", "NO", "DK", "FI", "AT", "BE", "PT", "NZ", "IE", "PL",
    "TR", "TH", "ID", "MY", "PH", "VN", "CO", "CL", "PE", "EG",
    "NG", "KE", "AE", "SA", "IL", "HK", "TW", "CZ", "RO", "GR",
]

CHANNELS: list[str] = [
    "direct", "social", "email", "organic", "paid_search",
    "referral", "affiliate", "display",
]

EVENT_TYPES: list[str] = [
    "purchase", "refund", "view", "add_to_cart", "checkout",
    "wishlist_add", "review_submit", "account_create",
]

DEVICES: list[str] = ["mobile", "desktop", "tablet"]

CURRENCIES: list[str] = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "INR"]

START_DATE: datetime = datetime(2023, 1, 1)
END_DATE: datetime = datetime(2026, 8, 6)
DAYS_RANGE: int = (END_DATE - START_DATE).days


class BatchArgs(NamedTuple):
    batch_id: int
    batch_size: int
    seed: int
    temp_dir: str


# ---------------------------------------------------------------------------
# Single-batch generator (runs in worker process)
# ---------------------------------------------------------------------------

def generate_batch(args: BatchArgs) -> Path:
    """Generate a single batch of fake event data and write it to a temp CSV.

    Returns the path to the temp file so the main process can concatenate
    all batches afterwards.
    """
    batch_id, batch_size, seed, temp_dir = args
    rng = np.random.default_rng(seed)

    # -- Categorical columns (pulled from pools) --
    countries = rng.choice(COUNTRIES, batch_size)
    channels = rng.choice(CHANNELS, batch_size)
    event_types = rng.choice(EVENT_TYPES, batch_size)
    devices = rng.choice(DEVICES, batch_size)
    currencies = rng.choice(CURRENCIES, batch_size)

    # -- Numeric columns --
    user_ids = rng.integers(100_000, 999_999, batch_size, dtype=np.int64)
    order_ids = rng.integers(10_000_000, 99_999_999, batch_size, dtype=np.int64)
    revenues = np.round(rng.uniform(0.99, 1_999.99, batch_size), 2).astype(np.float64)
    quantities = rng.integers(1, 10, batch_size, dtype=np.int32)
    session_durations = np.round(rng.uniform(5.0, 3_600.0, batch_size), 1).astype(np.float64)

    # -- Date column --
    random_days = rng.integers(0, DAYS_RANGE, batch_size)
    # vectorized date generation via numpy datetime64
    base_date = np.datetime64("2023-01-01")
    date_arr = base_date + random_days.astype("timedelta64[D]")

    # Convert numpy datetime64 to string in %Y-%m-%d format
    date_strs = date_arr.astype(str)

    # -- Additional columns for realism --
    # is_new_customer: weighted towards True for 'account_create' events
    is_new_customer = rng.choice(
        [True, False], batch_size, p=[0.3, 0.7]
    )

    # discount_pct: 0 for most, random for some
    discount_pct = np.where(
        rng.random(batch_size) < 0.4,
        np.round(rng.uniform(0.0, 50.0, batch_size), 1),
        0.0,
    ).astype(np.float64)

    # -- Build Polars DataFrame --
    df = pl.DataFrame(
        {
            "country": countries,
            "channel": channels,
            "user_id": user_ids,
            "order_id": order_ids,
            "revenue": revenues,
            "event_date": date_strs,
            "event_type": event_types,
            "device": devices,
            "currency": currencies,
            "quantity": quantities,
            "session_duration_sec": session_durations,
            "is_new_customer": is_new_customer,
            "discount_pct": discount_pct,
        },
    )

    # Write batch to its own temp CSV
    temp_path = Path(temp_dir) / f"batch_{batch_id:04d}.csv"
    df.write_csv(temp_path, include_header=True)

    logger.info("Batch %d: wrote %d records → %s", batch_id, batch_size, temp_path.name)
    return temp_path


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run() -> None:
    """Run the full data-generation pipeline."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger.info("Starting events data generation…")
    t0 = time.perf_counter()

    # ---- Prepare output directory & temp workspace ----
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Use a temp directory that lives beside the output so we can do an
    # atomic rename at the end if desired.  We'll clean it up on exit.
    temp_work_dir = tempfile.mkdtemp(dir=DATA_DIR, prefix=".gen_")

    def _cleanup() -> None:
        if os.path.isdir(temp_work_dir):
            shutil.rmtree(temp_work_dir, ignore_errors=True)

    atexit.register(_cleanup)

    # ---- Calculate batches ----
    num_full_batches, remainder = divmod(TOTAL_RECORDS, BATCH_SIZE)
    base_seed = 42
    num_workers = min(cpu_count() or 4, 8)  # cap at 8 to avoid thrashing

    batch_args: list[BatchArgs] = []
    for i in range(num_full_batches):
        batch_args.append(BatchArgs(i, BATCH_SIZE, base_seed + i, temp_work_dir))
    if remainder:
        batch_args.append(BatchArgs(num_full_batches, remainder, base_seed + num_full_batches, temp_work_dir))

    logger.info(
        "Spawning %d batches (%d full + %d remainder) across %d workers",
        len(batch_args), num_full_batches, 1 if remainder else 0, num_workers,
    )

    # ---- Parallel generation ----
    t_gen_start = time.perf_counter()
    with Pool(processes=num_workers) as pool:
        temp_files = pool.map(generate_batch, batch_args)
    t_gen_end = time.perf_counter()

    logger.info(
        "All %d batches generated in %.2f s",
        len(temp_files), t_gen_end - t_gen_start,
    )

    # ---- Streaming concatenation (Polars lazy scan + streaming sink) ----
    t_cat_start = time.perf_counter()

    # Build a glob pattern to pick up all temp CSVs
    glob_pattern = str(Path(temp_work_dir) / "batch_*.csv")
    lf = pl.scan_csv(glob_pattern)

    # sink_csv streams rows through; no materialisation of the full dataset
    lf.sink_csv(OUTPUT_FILE)

    t_cat_end = time.perf_counter()
    logger.info("Streaming concatenation finished in %.2f s", t_cat_end - t_cat_start)

    # ---- Clean up temp files (already registered via atexit, but do it now) ----
    _cleanup()
    atexit.unregister(_cleanup)

    # ---- Final stats ----
    total_elapsed = time.perf_counter() - t0
    file_size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)

    logger.info("=" * 50)
    logger.info("Data generation complete!")
    logger.info("  Output      : %s", OUTPUT_FILE.resolve())
    logger.info("  Records     : %d", TOTAL_RECORDS)
    logger.info("  File size   : %.2f MB", file_size_mb)
    logger.info("  Total time  : %.2f s", total_elapsed)
    logger.info("=" * 50)

    # ---- Quick verification (scan first few rows) ----
    verify = pl.scan_csv(OUTPUT_FILE).collect().head(5)
    logger.info("First 5 rows for sanity check:\n%s", verify)


# ---------------------------------------------------------------------------
# CLI entry-point (for `python -m ...` or direct invocation)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()
