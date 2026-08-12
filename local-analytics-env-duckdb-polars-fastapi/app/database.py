from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DuckDatabase:
    def __init__(self):
        self.conn = duckdb.connect(
            database=str(PROJECT_ROOT / "analytics.duckdb"),
            read_only=False,
        )

    def connection(self) -> duckdb.DuckDBPyConnection:
        return self.conn

db = DuckDatabase()
