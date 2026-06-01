import sys
from pathlib import Path
from datetime import date
import time
import duckdb

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

print("=" * 60)
print("Benchmarking Migrated Inventory Queries (Read-Only)")
print("=" * 60)

# Use in-memory DuckDB connection to avoid lock conflicts
data_lake_root = '/data-lake'
conn = duckdb.connect(database=':memory:')

# Test dates
snapshot_date = date(2026, 5, 26)
start_date = date(2026, 5, 20)
end_date = date(2026, 5, 26)

# Benchmark inventory snapshot query
print("\n1. Benchmarking inventory snapshot query")
try:
    start = time.time()
    result = conn.execute(f"""
        SELECT product_id, SUM(quantity) as qty_on_hand
        FROM read_parquet('{data_lake_root}/star-schema/fact_stock_on_hand_snapshot/**/*.parquet', hive_partitioning=true)
        WHERE snapshot_date = '{snapshot_date}'
        GROUP BY product_id
    """).fetchdf()
    elapsed = time.time() - start
    print(f"   Rows: {len(result)}")
    print(f"   Time: {elapsed:.3f}s")
except Exception as e:
    print(f"   Error: {e}")

# Benchmark sales by product query
print("\n2. Benchmarking sales by product query")
try:
    start = time.time()
    result = conn.execute(f"""
        SELECT product_id, SUM(revenue) as revenue, SUM(quantity) as units_sold
        FROM read_parquet('{data_lake_root}/star-schema/agg_sales_daily_by_product/**/*.parquet', hive_partitioning=true)
        WHERE date >= '{start_date}' AND date <= '{end_date}'
        GROUP BY product_id
    """).fetchdf()
    elapsed = time.time() - start
    print(f"   Rows: {len(result)}")
    print(f"   Time: {elapsed:.3f}s")
except Exception as e:
    print(f"   Error: {e}")

# Benchmark combined query (simulating _query_stock_levels)
print("\n3. Benchmarking combined stock + sales query")
try:
    start = time.time()
    stock_df = conn.execute(f"""
        SELECT product_id, SUM(quantity) as qty_on_hand
        FROM read_parquet('{data_lake_root}/star-schema/fact_stock_on_hand_snapshot/**/*.parquet', hive_partitioning=true)
        WHERE snapshot_date = '{snapshot_date}'
        GROUP BY product_id
    """).fetchdf()
    
    sales_df = conn.execute(f"""
        SELECT product_id, SUM(quantity) as units_sold
        FROM read_parquet('{data_lake_root}/star-schema/agg_sales_daily_by_product/**/*.parquet', hive_partitioning=true)
        WHERE date >= '{start_date}' AND date <= '{end_date}'
        GROUP BY product_id
    """).fetchdf()
    
    elapsed = time.time() - start
    print(f"   Stock rows: {len(stock_df)}")
    print(f"   Sales rows: {len(sales_df)}")
    print(f"   Time: {elapsed:.3f}s")
except Exception as e:
    print(f"   Error: {e}")

conn.close()

print("\n" + "=" * 60)
print("Benchmark Complete")
print("=" * 60)
