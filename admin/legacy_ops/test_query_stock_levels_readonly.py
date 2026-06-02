import sys
from pathlib import Path
from datetime import date
import duckdb
import os

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
snapshot_path = f"{data_lake_root}/star-schema/fact_stock_on_hand_snapshot/**/*.parquet"
agg_path = f"{data_lake_root}/star-schema/agg_sales_daily_by_product/**/*.parquet"

snapshot_date = date(2026, 5, 26)
lookback_start = date(2026, 5, 20)
lookback_end = date(2026, 5, 26)

print(f"Testing _query_stock_levels with read-only DuckDB")
print(f"Snapshot date: {snapshot_date}")
print(f"Lookback range: {lookback_start} to {lookback_end}")

try:
    # Use read-only connection to avoid lock conflicts
    conn = duckdb.connect(database=':memory:', read_only=False)
    
    # Get inventory snapshot
    query = f"""
    SELECT
        product_id,
        SUM(quantity) AS qty_on_hand
    FROM read_parquet('{snapshot_path}', hive_partitioning=1)
    WHERE snapshot_date = ?
    GROUP BY product_id
    """
    on_hand_df = conn.execute(query, [snapshot_date]).fetchdf()
    on_hand_df = on_hand_df.rename(columns={'qty_on_hand': 'on_hand_qty'})
    print(f"Inventory snapshot: {len(on_hand_df)} rows")
    
    # Get sales aggregates
    query = f"""
    SELECT
        product_id,
        SUM(quantity) AS units_sold,
        SUM(revenue) AS revenue
    FROM read_parquet('{agg_path}', hive_partitioning=1)
    WHERE date >= ? AND date <= ?
    GROUP BY product_id
    """
    sales_df = conn.execute(query, [lookback_start, lookback_end]).fetchdf()
    sales_df = sales_df.rename(columns={'units_sold': 'units_sold'})
    print(f"Sales aggregates: {len(sales_df)} rows")
    
    # Join data
    import pandas as pd
    result = on_hand_df.merge(sales_df, on='product_id', how='left')
    result['units_sold'] = result['units_sold'].fillna(0)
    result['revenue'] = result['revenue'].fillna(0)
    
    print(f"Success! Joined result: {len(result)} rows")
    print(f"Sample data:\n{result.head()}")
    print(f"\nSummary stats:")
    print(f"Total on_hand_qty: {result['on_hand_qty'].sum()}")
    print(f"Total units_sold: {result['units_sold'].sum()}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
