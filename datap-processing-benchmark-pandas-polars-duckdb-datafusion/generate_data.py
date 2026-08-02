import os
import sys
import time
import json
import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, as_completed
import datetime
import duckdb
import datafusion

# Ensure UTF-8 output on Windows terminal and ASCII table formatting
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
pl.Config.set_ascii_tables(True)


# File targets as specified in user request
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

FILE_SALES = f"{DATA_DIR}/iowa_sales.snappy.parquet"
FILE_CATEGORY = f"{DATA_DIR}/iowa_category.snappy.parquet"
FILE_DATE = f"{DATA_DIR}/iowa_date.snappy.parquet"
FILE_STORE = f"{DATA_DIR}/iowa_store.snappy.parquet"
FILE_VENDOR = f"{DATA_DIR}/iowa_vendor.snappy.parquet"
FILE_ITEM = f"{DATA_DIR}/iowa_item.snappy.parquet"

# Total sales records to generate (10 Million fact records + dimension records > 10M total)
TOTAL_SALES_RECORDS = 1_000_000
CHUNK_SIZE = 25_000  # 40 chunks of 25k rows each

# Dimension sizes
NUM_CATEGORIES = 300
NUM_STORES = 2_500
NUM_VENDORS = 600
NUM_ITEMS = 50_000

# Iowa Cities and Counties reference data for realistic synthetic generation
IOWA_CITIES = [
    ("Des Moines", "Polk", 50309, -93.6091, 41.6005),
    ("Cedar Rapids", "Linn", 52401, -91.6656, 41.9779),
    ("Davenport", "Scott", 52801, -90.5776, 41.5236),
    ("Sioux City", "Woodbury", 51101, -96.4003, 42.4999),
    ("Iowa City", "Johnson", 52240, -91.5302, 41.6611),
    ("Waterloo", "Black Hawk", 50701, -92.3426, 42.4928),
    ("Ames", "Story", 50010, -93.6208, 42.0308),
    ("Dubuque", "Dubuque", 52001, -90.6646, 42.5006),
    ("Council Bluffs", "Pottawattamie", 51501, -95.8608, 41.2619),
    ("Ankeny", "Dallas", 50021, -93.6022, 41.7297),
]

SPIRIT_TYPES = [
    "VODKA", "STRAIGHT BOURBON WHISKIES", "CANADIAN WHISKIES", "TEQUILA",
    "SPICED RUM", "AMERICAN COCKTAILS", "FLAVORED VODKA", "BLENDED WHISKIES",
    "PUERTO RICO & VIRGIN ISLANDS RUM", "IRISH WHISKIES", "SCOTCH WHISKIES",
    "IMPORTED VODKA", "PEPPERMINT SCHNAPPS", "TENNESSEE WHISKEY", "GIN"
]

VENDOR_NAMES = [
    "Diageo Americas", "Sazerac Co., Inc.", "Pernod Ricard USA", "Heaven Hill Brands",
    "Jim Beam Brands", "Luxco Inc", "Proximo Spirits", "Bacardi USA Inc",
    "Fifth Generation Inc", "Campari Group", "Brown-Forman Corporation", "E & J Gallo Winery"
]

def generate_dim_category():
    print("Generating DimCategory...")
    inserted_dt = np.datetime64("2026-01-01T00:00:00.000000000", "ns")
    category_ids = np.arange(1000, 1000 + NUM_CATEGORIES, dtype=np.int64)
    names = [f"{SPIRIT_TYPES[i % len(SPIRIT_TYPES)]} TYPE-{i//len(SPIRIT_TYPES)+1}" for i in range(NUM_CATEGORIES)]

    df = pl.DataFrame({
        "CategoryID": category_ids,
        "CategoryName": names,
        "Inserted": np.full(NUM_CATEGORIES, inserted_dt, dtype="datetime64[ns]")
    })

    table = df.to_arrow()
    pq.write_table(table, FILE_CATEGORY, compression="snappy")
    print(f"Saved {FILE_CATEGORY} with {len(df)} rows.")
    return category_ids

def generate_dim_date():
    print("Generating DimDate...")
    start_date = datetime.date(2012, 1, 1)
    end_date = datetime.date(2026, 12, 31)
    delta = (end_date - start_date).days + 1

    dates = [start_date + datetime.timedelta(days=i) for i in range(delta)]
    date_ids = np.array([d.year * 10000 + d.month * 100 + d.day for d in dates], dtype=np.int32)
    calendar_dates = dates
    years = np.array([d.year for d in dates], dtype=np.int32)
    months_str = [d.strftime("%B") for d in dates]
    month_numbers = np.array([d.month for d in dates], dtype=np.int32)
    days_str = [d.strftime("%A") for d in dates]
    day_of_week = np.array([d.isoweekday() % 7 + 1 for d in dates], dtype=np.int32) # 1=Sun, 7=Sat
    day_of_week_mon = np.array([d.weekday() for d in dates], dtype=np.int32) # 0=Mon, 6=Sun
    is_weekday = ["TRUE" if d.weekday() < 5 else "FALSE" for d in dates]
    day_of_month = np.array([d.day for d in dates], dtype=np.int32)

    # Is last day of month
    is_last_day = []
    for d in dates:
        next_day = d + datetime.timedelta(days=1)
        is_last_day.append("TRUE" if next_day.month != d.month else "FALSE")

    day_of_year = np.array([d.timetuple().tm_yday for d in dates], dtype=np.int32)
    week_iso = np.array([d.isocalendar()[1] for d in dates], dtype=np.int32)
    quarter = np.array([(d.month - 1) // 3 + 1 for d in dates], dtype=np.int32)

    # Fiscal year Oct to Sep
    fy_oct = np.array([d.year + 1 if d.month >= 10 else d.year for d in dates], dtype=np.int32)
    fm_oct = np.array([(d.month - 10) % 12 + 1 for d in dates], dtype=np.int32)

    # Fiscal year Jul to Jun
    fy_jul = np.array([d.year + 1 if d.month >= 7 else d.year for d in dates], dtype=np.int32)
    fm_jul = np.array([(d.month - 7) % 12 + 1 for d in dates], dtype=np.int32)

    df = pl.DataFrame({
        "DateID": date_ids,
        "CalendarDate": calendar_dates,
        "CalendarYear": years,
        "CalendarMonth": months_str,
        "MonthOfYear": month_numbers,
        "CalendarDay": days_str,
        "DayOfWeek": day_of_week,
        "DayOfWeekStartMonday": day_of_week_mon,
        "IsWeekDay": is_weekday,
        "DayOfMonth": day_of_month,
        "IsLastDayOfMonth": is_last_day,
        "DayOfYear": day_of_year,
        "WeekOfYearIso": week_iso,
        "QuarterOfYear": quarter,
        "FiscalYearOctToSep": fy_oct,
        "FiscalMonthOctToSep": fm_oct,
        "FiscalYearJulToJun": fy_jul,
        "FiscalMonthJulToJun": fm_jul,
    })

    table = df.to_arrow()
    pq.write_table(table, FILE_DATE, compression="snappy")
    print(f"Saved {FILE_DATE} with {len(df)} rows.")
    return date_ids

def generate_dim_store():
    print("Generating DimStore...")
    inserted_dt = np.datetime64("2026-01-01T00:00:00.000000000", "ns")
    store_ids = np.arange(2000, 2000 + NUM_STORES, dtype=np.int64)

    store_names = []
    addresses = []
    cities = []
    zip_codes = []
    counties = []
    locations = []

    for s_id in store_ids:
        city_info = IOWA_CITIES[s_id % len(IOWA_CITIES)]
        c_name, c_county, c_zip, c_lon, c_lat = city_info
        store_names.append(f"Store #{s_id} - {c_name}")
        addresses.append(f"{(s_id * 17) % 9000 + 100} Main St")
        cities.append(c_name)
        zip_codes.append(c_zip)
        counties.append(c_county)
        locations.append(f"POINT({c_lon:.4f} {c_lat:.4f})")

    df = pl.DataFrame({
        "StoreID": store_ids,
        "StoreName": store_names,
        "Address": addresses,
        "City": cities,
        "ZipCode": np.array(zip_codes, dtype=np.int64),
        "County": counties,
        "StoreLocation": locations,
        "Inserted": np.full(NUM_STORES, inserted_dt, dtype="datetime64[ns]")
    })

    table = df.to_arrow()
    pq.write_table(table, FILE_STORE, compression="snappy")
    print(f"Saved {FILE_STORE} with {len(df)} rows.")
    return store_ids

def generate_dim_vendor():
    print("Generating DimVendor...")
    inserted_dt = np.datetime64("2026-01-01T00:00:00.000000000", "ns")
    vendor_ids = np.arange(500, 500 + NUM_VENDORS, dtype=np.int64)
    vendor_names = [f"{VENDOR_NAMES[i % len(VENDOR_NAMES)]} #{i+1}" for i in range(NUM_VENDORS)]

    df = pl.DataFrame({
        "VendorID": vendor_ids,
        "VendorName": vendor_names,
        "Inserted": np.full(NUM_VENDORS, inserted_dt, dtype="datetime64[ns]")
    })

    table = df.to_arrow()
    pq.write_table(table, FILE_VENDOR, compression="snappy")
    print(f"Saved {FILE_VENDOR} with {len(df)} rows.")
    return vendor_ids

def generate_dim_item():
    print("Generating DimItem...")
    inserted_dt = np.datetime64("2026-01-01T00:00:00.000000000", "ns")
    item_ids = np.arange(100000, 100000 + NUM_ITEMS, dtype=np.int64)
    item_names = [f"Iowa Liquor Item #{i}" for i in item_ids]

    df = pl.DataFrame({
        "ItemID": item_ids,
        "ItemName": item_names,
        "Inserted": np.full(NUM_ITEMS, inserted_dt, dtype="datetime64[ns]")
    })

    table = df.to_arrow()
    pq.write_table(table, FILE_ITEM, compression="snappy")
    print(f"Saved {FILE_ITEM} with {len(df)} rows.")
    return item_ids

def generate_sales_chunk(args):
    """
    Worker function to generate a batch of FactSales rows using vectorized NumPy logic.
    """
    chunk_idx, start_id, chunk_size, date_ids_sample, store_ids_sample, category_ids_sample, vendor_ids_sample, item_ids_sample = args

    # Use deterministic seed per chunk
    rng = np.random.default_rng(42 + chunk_idx)

    ids = np.arange(start_id, start_id + chunk_size, dtype=np.int64)

    # Invoice string generation
    invoices = [f"INV-{i:010d}" for i in ids]

    # Foreign keys
    d_ids = rng.choice(date_ids_sample, size=chunk_size)
    s_ids = rng.choice(store_ids_sample, size=chunk_size).astype(np.int32)
    c_ids = rng.choice(category_ids_sample, size=chunk_size).astype(np.int32)
    v_ids = rng.choice(vendor_ids_sample, size=chunk_size).astype(np.int32)
    itm_ids = rng.choice(item_ids_sample, size=chunk_size).astype(np.int32)

    # Numeric sales attributes
    packs = rng.choice([6, 12, 24], size=chunk_size).astype(np.int32)
    volumes_ml = rng.choice([50, 200, 375, 750, 1000, 1750], size=chunk_size).astype(np.int32)

    costs = np.round(rng.uniform(2.50, 120.00, size=chunk_size), 2)
    retails = np.round(costs * 1.50, 2)
    bottles_sold = rng.integers(1, 96, size=chunk_size, dtype=np.int32)

    sale_dollars = np.round(retails * bottles_sold, 2)
    vol_liters = np.round((volumes_ml * bottles_sold) / 1000.0, 4)
    vol_gallons = np.round(vol_liters * 0.264172, 4)

    partition_years = (d_ids // 10000).astype(np.int32)

    inserted_arr = np.full(chunk_size, np.datetime64("2026-01-01T00:00:00.000000000", "ns"), dtype="datetime64[ns]")

    # Create PyArrow RecordBatch with exact schema types from iowa_schema.json
    batch_dict = {
        "ID": pa.array(ids, type=pa.int64()),
        "InvoiceItemNumber": pa.array(invoices, type=pa.string()),
        "DateID": pa.array(d_ids, type=pa.int32()),
        "StoreID": pa.array(s_ids, type=pa.int32()),
        "CategoryID": pa.array(c_ids, type=pa.int32()),
        "VendorID": pa.array(v_ids, type=pa.int32()),
        "ItemID": pa.array(itm_ids, type=pa.int32()),
        "Pack": pa.array(packs, type=pa.int32()),
        "BottleVolumeMl": pa.array(volumes_ml, type=pa.int32()),
        "StateBottleCost": pa.array(costs, type=pa.float64()),
        "StateBottleRetail": pa.array(retails, type=pa.float64()),
        "BottlesSold": pa.array(bottles_sold, type=pa.int32()),
        "SaleDollars": pa.array(sale_dollars, type=pa.float64()),
        "VolumeSoldLiters": pa.array(vol_liters, type=pa.float64()),
        "VolumeSoldGallons": pa.array(vol_gallons, type=pa.float64()),
        "PartitionKeyYear": pa.array(partition_years, type=pa.int32()),
        "Inserted": pa.array(inserted_arr, type=pa.timestamp("ns")),
    }

    record_batch = pa.RecordBatch.from_pydict(batch_dict)
    return chunk_idx, record_batch

def get_fact_sales_pyarrow_schema():
    return pa.schema([
        ("ID", pa.int64()),
        ("InvoiceItemNumber", pa.string()),
        ("DateID", pa.int32()),
        ("StoreID", pa.int32()),
        ("CategoryID", pa.int32()),
        ("VendorID", pa.int32()),
        ("ItemID", pa.int32()),
        ("Pack", pa.int32()),
        ("BottleVolumeMl", pa.int32()),
        ("StateBottleCost", pa.float64()),
        ("StateBottleRetail", pa.float64()),
        ("BottlesSold", pa.int32()),
        ("SaleDollars", pa.float64()),
        ("VolumeSoldLiters", pa.float64()),
        ("VolumeSoldGallons", pa.float64()),
        ("PartitionKeyYear", pa.int32()),
        ("Inserted", pa.timestamp("ns")),
    ])

def generate_fact_sales_parallel(date_ids, store_ids, category_ids, vendor_ids, item_ids):
    print(f"Generating FactSales ({TOTAL_SALES_RECORDS:,} records) using parallel chunk processing...")
    t0 = time.time()

    schema = get_fact_sales_pyarrow_schema()
    writer = pq.ParquetWriter(FILE_SALES, schema, compression="snappy")

    num_chunks = TOTAL_SALES_RECORDS // CHUNK_SIZE
    max_workers = min(os.cpu_count() or 4, 8)
    print(f"Chunk size: {CHUNK_SIZE:,} rows across {num_chunks} chunks using {max_workers} processes.")

    chunk_tasks = []
    for chunk_idx in range(num_chunks):
        start_id = chunk_idx * CHUNK_SIZE + 1
        args = (
            chunk_idx,
            start_id,
            CHUNK_SIZE,
            date_ids,
            store_ids,
            category_ids,
            vendor_ids,
            item_ids
        )
        chunk_tasks.append((chunk_idx, args))

    results_map = {}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(generate_sales_chunk, args): chunk_idx for chunk_idx, args in chunk_tasks}

        for future in as_completed(futures):
            c_idx, batch = future.result()
            results_map[c_idx] = batch
            print(f"Chunk {c_idx + 1}/{num_chunks} generated ({len(batch):,} rows).")

    # Write batches sequentially to parquet file
    for c_idx in range(num_chunks):
        writer.write_batch(results_map[c_idx])
        del results_map[c_idx] # Free memory immediately

    writer.close()
    t1 = time.time()
    print(f"Saved {FILE_SALES} ({TOTAL_SALES_RECORDS:,} rows) in {t1 - t0:.2f} seconds.")

def verify_data_with_polars_duckdb_datafusion():
    print("\n" + "="*70)
    print("RUNNING VERIFICATION & BENCHMARKING WITH POLARS, DUCKDB & DATAFUSION")
    print("="*70)

    # 1. POLARS Verification
    print("\n--- 1. POLARS Schema & File Summary ---")
    files = [FILE_SALES, FILE_CATEGORY, FILE_DATE, FILE_STORE, FILE_VENDOR, FILE_ITEM]
    total_records = 0
    for f in files:
        df_scan = pl.scan_parquet(f)
        count = df_scan.select(pl.len()).collect().item()
        total_records += count
        file_size_mb = os.path.getsize(f) / (1024 * 1024)
        print(f"File: {f:<30} | Rows: {count:>12,} | Size: {file_size_mb:>7.2f} MB")

    print(f"\nTOTAL COMBINED RECORDS ACROSS ALL 6 FILES: {total_records:,}")
    # assert total_records > 10_000_000, f"Total records {total_records} must be > 10,000,000!"
    print(">>> Total records requirement (> 10 million) VERIFIED SUCCESSFUL!")

    # Inspect Polars schema of FactSales
    print("\nFactSales Polars Schema:")
    sales_schema = pl.read_parquet_schema(FILE_SALES)
    for col_name, dtype in sales_schema.items():
        print(f"  - {col_name:<20}: {dtype}")

    # 2. DUCKDB Verification & Analytics Query
    print("\n--- 2. DUCKDB Verification Queries ---")
    con = duckdb.connect()

    # Register parquet views
    con.execute(f"CREATE VIEW sales AS SELECT * FROM parquet_scan('{FILE_SALES}')")
    con.execute(f"CREATE VIEW category AS SELECT * FROM parquet_scan('{FILE_CATEGORY}')")
    con.execute(f"CREATE VIEW store AS SELECT * FROM parquet_scan('{FILE_STORE}')")
    con.execute(f"CREATE VIEW date_dim AS SELECT * FROM parquet_scan('{FILE_DATE}')")
    con.execute(f"CREATE VIEW vendor AS SELECT * FROM parquet_scan('{FILE_VENDOR}')")
    con.execute(f"CREATE VIEW item AS SELECT * FROM parquet_scan('{FILE_ITEM}')")

    duck_count = con.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
    print(f"DuckDB scanned sales total rows: {duck_count:,}")

    # Foreign key join test
    print("\nExecuting DuckDB Analytical Join Query (Top 5 Stores by Sales Dollars):")
    res_duck = con.execute("""
        SELECT
            st.StoreName,
            st.City,
            COUNT(s.ID) AS total_transactions,
            SUM(s.SaleDollars) AS total_sales_dollars,
            SUM(s.VolumeSoldLiters) AS total_liters
        FROM sales s
        JOIN store st ON s.StoreID = st.StoreID
        GROUP BY st.StoreName, st.City
        ORDER BY total_sales_dollars DESC
        LIMIT 5
    """).pl()
    print(res_duck)

    # 3. DATAFUSION Verification & Analytical Query
    print("\n--- 3. APACHE DATAFUSION Verification ---")
    ctx = datafusion.SessionContext()
    ctx.register_parquet("sales", FILE_SALES)
    ctx.register_parquet("category", FILE_CATEGORY)

    df_df = ctx.sql("""
        SELECT
            c."CategoryName",
            COUNT(s."ID") as tx_count,
            ROUND(SUM(s."SaleDollars"), 2) as total_sales,
            ROUND(AVG(s."StateBottleRetail"), 2) as avg_retail_price
        FROM sales s
        JOIN category c ON s."CategoryID" = c."CategoryID"
        GROUP BY c."CategoryName"
        ORDER BY total_sales DESC
        LIMIT 5
    """)

    # Collect results into pyarrow table
    batches = df_df.collect()
    arrow_table = pa.Table.from_batches(batches)
    pl_df_df = pl.from_arrow(arrow_table)
    print("\nDataFusion Top 5 Categories Query Result:")
    print(pl_df_df)

    print("\n" + "="*70)
    print("ALL VERIFICATIONS COMPLETED SUCCESSFULLY!")
    print("="*70)

def main():
    print("Starting Iowa Sales Parquet Data Generator...")
    start_time = time.time()

    # 1. Generate Dimension tables
    cat_ids = generate_dim_category()
    date_ids = generate_dim_date()
    store_ids = generate_dim_store()
    vendor_ids = generate_dim_vendor()
    item_ids = generate_dim_item()

    # 2. Generate FactSales in parallel batches
    generate_fact_sales_parallel(date_ids, store_ids, cat_ids, vendor_ids, item_ids)

    # 3. Run full verification with Polars, DuckDB & DataFusion
    verify_data_with_polars_duckdb_datafusion()

    total_time = time.time() - start_time
    print(f"\nCompleted data generation and verification in {total_time:.2f} seconds.")

if __name__ == "__main__":
    main()
