import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


def run_step(script_name: str, desc: str):
    script_path = PROJECT_ROOT / script_name
    print(f"\n{'=' * 70}")
    print(f"🚀 Running: {desc} ({script_name})")
    print(f"{'=' * 70}")
    t0 = time.time()
    subprocess.run([sys.executable, str(script_path)], check=True)
    elapsed = time.time() - t0
    print(f"✅ Finished {script_name} in {elapsed:.2f}s")
    return elapsed


def main():
    total_start = time.time()
    print("\n" + "=" * 70)
    print("🎯 STARTING DUCKDB MEDALLION LAKEHOUSE PIPELINE")
    print("=" * 70)

    t_bronze = run_step("load_bronze.py", "Bronze Ingestion (Schema-on-Read)")
    t_silver = run_step("load_silver.py", "Silver Cleansing & Quality Gates")
    t_gold = run_step("load_gold.py", "Gold Modeling (Marts & Star Schema)")

    total_time = time.time() - total_start
    print("\n" + "=" * 70)
    print("🎉 FULL PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    print(f"  🥉 Bronze Stage: {t_bronze:.2f}s")
    print(f"  🥈 Silver Stage: {t_silver:.2f}s")
    print(f"  🥇 Gold Stage:   {t_gold:.2f}s")
    print(f"  ⚡ Total Time:   {total_time:.2f}s")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
