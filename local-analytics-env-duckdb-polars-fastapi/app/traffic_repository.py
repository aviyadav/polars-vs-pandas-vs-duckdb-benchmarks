from pathlib import Path
import polars as pl
from app.database import db

import logging
import time

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRAFFIC_FILE = (PROJECT_ROOT / "data" / "jun_final_v2.parquet").as_posix()

class TrafficRepository:
    def __init__(self):
        self.conn = db.connection()

    def monthly(self) -> pl.DataFrame:
        sql = f"""
        SELECT
            year,
            month,
            round(SUM(byt) / 1024 / 1024 / 1024, 2) AS total_gb
        FROM read_parquet('{TRAFFIC_FILE}')
        GROUP BY year, month
        ORDER BY year DESC, month DESC;
        """

        start = time.perf_counter()
        result = self.conn.execute(sql).pl()
        end = time.perf_counter()
        logger.info(
            f"Query 'monthly' completed in {end - start:.2f} seconds (Rows: {len(result)})"
        )
        return result


    def top_rejects(self) -> pl.DataFrame:
        sql = f"""
        SELECT
            src,
            dst,
            round(SUM(byt) / 1024 / 1024, 2) AS total_mb
        FROM read_parquet('{TRAFFIC_FILE}')
        WHERE act IN ('REJECT', 'BLOCK', 'DROP')
        GROUP BY src, dst
        ORDER BY total_mb DESC
        LIMIT 10;
        """

        start = time.perf_counter()
        result = self.conn.execute(sql).pl()
        end = time.perf_counter()
        logger.info(
            f"Query 'top_rejects' completed in {end - start:.2f} seconds (Rows: {len(result)})"
        )
        return result
