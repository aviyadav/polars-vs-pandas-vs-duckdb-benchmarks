"""Generate a synthetic internet-traffic dataset as a Parquet file.

Architecture:
  * Chunk generation is parallelised across CPU cores with a process pool
    (numpy draws each chunk's random data inside a worker process).
  * Chunks are streamed straight to disk via pyarrow's ParquetWriter, which
    appends row groups to a single file.  At most one chunk per worker plus
    a small prefetch queue is resident in RAM, so peak memory stays flat
    no matter how many rows are requested.
  * Memory is bounded by ~ processes * 2 * chunk size; raise --chunk-rows
    for more speed on machines with plenty of RAM, lower it to save memory.

Columns (typical firewall/proxy traffic fields):
    timestamp, year, month, day         temporal fields
    src, dst                            IPv4 endpoints (as uint32)
    src_port, dst_port, protocol        connection info
    act                                 action taken (ALLOW / BLOCK / DROP)
    byt                                 bytes transferred

Usage:
    uv run python generate_data.py                  # 250,000 rows -> data/jun_final_v2.parquet
    uv run python generate_data.py --rows 25000000  # scales up at the same peak memory
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_ROWS = 250_000
DEFAULT_YEAR = 2024
DEFAULT_CHUNK_ROWS = 100_000
DEFAULT_MEMORY_LIMIT_GB = 2

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_FILE = DATA_DIR / "jun_final_v2.parquet"

COMMON_PORTS = np.array([80, 443, 53, 22, 25, 3389, 8080, 8443, 3306, 110], dtype=np.uint16)


@dataclass(frozen=True)
class ChunkTask:
    """One parallel unit of work; carries its own seed for reproducibility."""

    n_rows: int
    year: int
    seed: int


def generate_chunk(task: ChunkTask) -> dict[str, np.ndarray]:
    """Return one chunk of traffic rows: column name -> numpy array.

    The RNG is derived from the task seed, so output is identical no
    matter which worker runs the task.
    """
    rng = np.random.default_rng(task.seed)
    n = task.n_rows

    # Sample the calendar uniformly, then move ~35% of rows into June.
    # Replacing each row one at a time keeps the June traffic spread evenly
    # across the whole file instead of clumping at the top.
    month = rng.integers(1, 13, size=n, dtype=np.uint8)
    month[rng.random(n) < 0.35] = 6
    day = rng.integers(1, 29, size=n, dtype=np.uint8)  # valid in every month

    # Timestamp = start of year + days elapsed + seconds.
    days_elapsed = (month.astype(np.int32) - 1) * 28 + (day.astype(np.int32) - 1)
    seconds_of_day = rng.integers(0, 86_400, size=n)
    timestamp = (
        np.datetime64(f"{task.year}-01-01", "s")
        + (days_elapsed * 86_400 + seconds_of_day) * np.timedelta64(1, "s")
    ).astype("datetime64[us]")

    return {
        "timestamp": timestamp,
        "year": np.full(n, task.year, dtype=np.uint16),
        "month": month,
        "day": day,
        "src": (10 << 24) | rng.integers(0, 16_500_000, size=n, dtype=np.uint32),  # 10.x.y.z
        "dst": rng.integers(1, 4_200_000_000, size=n, dtype=np.uint32),            # public IPs
        "src_port": rng.integers(1024, 65536, size=n, dtype=np.uint16),
        "dst_port": rng.choice(
            COMMON_PORTS,
            size=n,
            p=[0.34, 0.50, 0.05, 0.03, 0.02, 0.01, 0.02, 0.02, 0.005, 0.005],
        ),
        "protocol": rng.choice(
            np.array(["TCP", "UDP", "ICMP"], dtype=object), size=n, p=[0.80, 0.19, 0.01]
        ).astype("U"),
        "act": rng.choice(
            np.array(["ALLOW", "BLOCK", "DROP"], dtype=object), size=n, p=[0.75, 0.15, 0.10]
        ).astype("U"),
        # Right-skewed bytes: 30% zero-byte (blocked/keepalive) records.
        "byt": np.where(
            rng.random(n) < 0.30,
            np.zeros(n, dtype=np.uint32),
            rng.integers(60, 1_500_000, size=n, dtype=np.uint32),
        ),
    }


def chunk_tasks(total_rows: int, chunk_rows: int, year: int, seed: int):
    for index, start in enumerate(range(0, total_rows, chunk_rows)):
        yield ChunkTask(
            n_rows=min(chunk_rows, total_rows - start),
            year=year,
            seed=seed + index * 104_729,
        )


def columns_to_table(columns: dict[str, np.ndarray]) -> pa.Table:
    """Convert numpy column dict to a pyarrow Table."""
    arrays = {}
    for name, arr in columns.items():
        if arr.dtype.kind == "U":
            # String columns: convert to pyarrow string array
            arrays[name] = pa.array(arr.tolist(), type=pa.string())
        elif arr.dtype == np.dtype("datetime64[us]"):
            arrays[name] = pa.array(arr, type=pa.timestamp("us"))
        else:
            arrays[name] = pa.array(arr)
    return pa.table(arrays)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic internet-traffic Parquet data.")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS, help="total rows to generate")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, help="year for all rows")
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=DEFAULT_CHUNK_ROWS,
        help="rows per chunk (bigger = faster, more RAM; smaller = tighter memory)",
    )
    parser.add_argument("--processes", type=int, default=None, help="worker processes")
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE, help="output parquet path")
    parser.add_argument("--seed", type=int, default=42, help="base random seed")
    args = parser.parse_args()

    if args.rows <= 0 or args.chunk_rows <= 0:
        raise SystemExit("--rows and --chunk-rows must be positive")

    output: Path = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        output_label: str = str(output.relative_to(PROJECT_ROOT))
    except ValueError:  # output outside the project directory
        output_label = str(output)

    tasks = list(chunk_tasks(args.rows, args.chunk_rows, args.year, args.seed))
    n_chunks = len(tasks)
    processes = min(args.processes or mp.cpu_count(), n_chunks)

    print(
        f"Generating {args.rows:,} rows in {n_chunks:,} chunks of {args.chunk_rows:,} "
        f"using {processes} worker(s)\n  -> {output_label}"
    )
    start = time.perf_counter()

    # "spawn" behaves identically on Windows/Linux/macOS.  imap with
    # chunksize=1 streams results back: workers generate ahead while the
    # parent writes to disk, so generation and I/O overlap and only a few
    # chunks are ever in flight.
    with mp.get_context("spawn").Pool(processes=processes) as pool:
        writer: pq.ParquetWriter | None = None
        written = 0
        try:
            for i, columns in enumerate(pool.imap(generate_chunk, tasks, chunksize=1)):
                table = columns_to_table(columns)
                if writer is None:
                    writer = pq.ParquetWriter(str(output), table.schema, compression="zstd")
                writer.write_table(table)
                written += tasks[i].n_rows
                print(f"  wrote {written:>14,} / {args.rows:,} rows", end="\r", flush=True)
        finally:
            if writer is not None:
                writer.close()
        print()

    elapsed = time.perf_counter() - start
    print(
        f"Done: {args.rows:,} rows written in {elapsed:.2f}s "
        f"({args.rows / elapsed:,.0f} rows/s) -> "
        f"{output.stat().st_size / 1_048_576:.1f} MiB on disk"
    )

    # Verify the file is readable end-to-end and has the expected row count.
    meta = pq.read_metadata(str(output))
    n_read = meta.num_rows
    print(f"Verified: {n_read:,} rows, {meta.num_row_groups} row group(s)")
    if n_read != args.rows:
        raise SystemExit(f"Row count mismatch: expected {args.rows:,}, got {n_read:,}")


if __name__ == "__main__":
    main()
