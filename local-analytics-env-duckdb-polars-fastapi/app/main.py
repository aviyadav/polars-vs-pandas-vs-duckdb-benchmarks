import sys
from pathlib import Path

import logging
from fastapi import FastAPI
from app.service import TrafficAnalyticsService
from app.database import db

# Allow both `python -m app.main` and `python app/main.py` from the root.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRAFFIC_FILE = (PROJECT_ROOT / "data" / "jun_final_v2.parquet").as_posix()

# Configure logging format and level
logging.basicConfig(
    filename="../app.log",
    filemode="a",  # 'a' appends to file, 'w' overwrites on restart
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

conn = db.connection()
query = f"""
SELECT *
FROM read_parquet('{TRAFFIC_FILE}')
LIMIT 10
"""

# # Execute the result as a Polars DataFrame
# rows = conn.execute(query).pl()
# print(rows)

# # 1. Initialize the TrafficRepository
# # repo = TrafficRepository()
# service = TrafficAnalyticsService()


# # 2. Call the repository method to get your Polars DataFrame
# # top_rejected_rows = service.repository.top_rejects()
# dashboard = service.executive_dashboard()

# # # 3. Print the top rejected rows
# # print("Top rejected rows:")
# # print(top_rejected_rows)

# # # 4. Get Monthly Traffic
# # monthly_rows = repo.monthly()

# # # 5. Output Monthly Traffic
# # print("Monthly Traffic")
# # print(monthly_rows)

# # 3.1 Access individual components
# print("=== TOP REJECTS ===")
# print(dashboard["top_rejects"])

# print("\n=== MONTHLY TRAFFIC ===")
# print(dashboard["monthly_traffic"])


app = FastAPI(title="Network Traffic Analytics API")
analytics = TrafficAnalyticsService()

@app.get("/")
async def root():
    return {"message": "Welcome to the Network Traffic Analytics API"}

@app.get("/dashboard")
async def get_dashboard() -> dict:
    return analytics.executive_dashboard()
