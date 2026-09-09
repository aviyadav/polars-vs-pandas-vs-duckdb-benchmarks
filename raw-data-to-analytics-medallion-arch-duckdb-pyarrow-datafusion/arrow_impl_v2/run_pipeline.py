# run_pipeline.py - Arrow/DataFusion incremental lakehouse orchestrator
import logging
import sys
import time
import uuid
from pathlib import Path

import health

PIPELINE_NAME = "arrow_impl_v2"

# ----------------------------------------------------------------------
# Path Configurations & Environment Setup
# ----------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parent

LOGS_DIR = PROJECT_ROOT / "logs"

# Ensure runtime directories exist
LOGS_DIR.mkdir(parents=True, exist_ok=True)
(REPO_ROOT / "data").mkdir(parents=True, exist_ok=True)
(PROJECT_ROOT / "storage").mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------
# Logging Setup (Console Output + Daily Log File)
# ----------------------------------------------------------------------
log_file_path = LOGS_DIR / f"pipeline_{time.strftime('%Y%m%d')}.log"

logger = logging.getLogger("arrow_lakehouse_pipeline")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler = logging.FileHandler(log_file_path)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def run_stage(script_name: str, run_id: str) -> None:
    """Executes a pipeline stage script in isolation within the project context."""
    script_path = PROJECT_ROOT / script_name
    if not script_path.exists():
        logger.error(f"Script missing: {script_path}")
        health.notify_failure(
            pipeline=PIPELINE_NAME,
            run_id=run_id,
            stage=script_name,
            error=f"Script missing: {script_path}",
            elapsed=0.0,
        )
        sys.exit(1)

    logger.info(f"▶▶ Starting Stage: {script_name}")
    start_time = time.time()

    try:
        # Read and execute script isolated within its target path variables
        with open(script_path, "r", encoding="utf-8") as f:
            code = compile(f.read(), str(script_path), "exec")
            # Set __file__ so scripts correctly resolve PROJECT_ROOT using Path(__file__)
            exec(code, {"__file__": str(script_path), "__name__": "__main__"})  # noqa: S102

        elapsed = time.time() - start_time
        logger.info(f"✔ Completed Stage: {script_name} in {elapsed:.2f}s\n")

    except Exception as err:
        elapsed = time.time() - start_time
        logger.exception(f"❌ Failed Stage: {script_name} after {elapsed:.2f}s")
        health.notify_failure(
            pipeline=PIPELINE_NAME,
            run_id=run_id,
            stage=script_name,
            error=str(err),
            elapsed=elapsed,
        )
        # Abort downstream stages on failure to maintain lakehouse consistency
        sys.exit(1)


# ----------------------------------------------------------------------
# Pipeline Entrypoint
# ----------------------------------------------------------------------
if __name__ == "__main__":
    total_start_time = time.time()
    run_id = f"{time.strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4().hex[:8]}"
    logger.info(
        "========================================================================"
    )
    logger.info("Starting Incremental Arrow/DataFusion Medallion Pipeline")
    logger.info(f"Run ID: {run_id}")
    logger.info(
        "========================================================================"
    )

    health.notify_start(PIPELINE_NAME, run_id)

    # 1. Landing -> Bronze Layer
    run_stage("load_bronze.py", run_id)

    # 2. Bronze -> Silver Layer
    run_stage("load_silver.py", run_id)

    # 3. Silver -> Gold Layer
    run_stage("load_gold.py", run_id)

    total_elapsed = time.time() - total_start_time
    health.notify_success(PIPELINE_NAME, run_id, total_elapsed)
    logger.info(
        "========================================================================"
    )
    logger.info(f"Pipeline Run Completed Successfully in {total_elapsed:.2f} seconds.")
    logger.info(
        "========================================================================\n"
    )
