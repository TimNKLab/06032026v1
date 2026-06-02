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
lookback_days = 7

print(f"Testing query_inventory_summary with read-only DuckDB")
print(f"Snapshot date: {snapshot_date}")
print(f"Lookback days: {lookback_days}")

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
    stock_df = conn.execute(query, [snapshot_date]).fetchdf()
    stock_df = stock_df.rename(columns={'qty_on_hand': 'on_hand_qty'})
    print(f"Inventory snapshot: {len(stock_df)} rows")
    
    # Calculate lookback start
    from datetime import timedelta
    lookback_start = snapshot_date - timedelta(days=lookback_days - 1)
    
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
    sales_df = conn.execute(query, [lookback_start, snapshot_date]).fetchdf()
    print(f"Sales aggregates: {len(sales_df)} rows")
    
    # Calculate avg_daily_sold from sales data
    if not sales_df.empty:
        sales_df['avg_daily_sold'] = sales_df['units_sold'] / lookback_days
    else:
        sales_df = sales_df = __import__('pandas').DataFrame(columns=['product_id', 'units_sold', 'revenue', 'avg_daily_sold'])
    
    # Join stock and sales data
    import pandas as pd
    combined = stock_df.merge(sales_df, on='product_id', how='left')
    
    # Fill missing values
    combined['units_sold'] = combined['units_sold'].fillna(0)
    combined['revenue'] = combined['revenue'].fillna(0)
    combined['avg_daily_sold'] = combined['avg_daily_sold'].fillna(0)
    
    # Calculate days_of_cover
    combined['days_of_cover'] = combined.apply(
        lambda row: row['on_hand_qty'] / row['avg_daily_sold'] if row['avg_daily_sold'] > 0 else 999999,
        axis=1
    )
    
    # Classify stock_status
    def classify_stock_status(row):
        if row['units_sold'] == 0:
            return 'dead_stock'
        elif row['days_of_cover'] < 14:  # low_stock_days
            return 'low_stock'
        elif row['days_of_cover'] > 90:  # overstock_days
            return 'overstock'
        else:
            return 'healthy'
    
    combined['stock_status'] = combined.apply(classify_stock_status, axis=1)
    
    # Calculate est_stock_value
    combined['est_stock_value'] = combined.apply(
        lambda row: row['on_hand_qty'] * (row['revenue'] / row['units_sold']) if row['units_sold'] > 0 else 0,
        axis=1
    )
    
    # Calculate summary metrics
    total_sku_count = len(combined)
    total_inventory_value = combined['est_stock_value'].sum()
    dead_stock_count = (combined['stock_status'] == 'dead_stock').sum()
    low_stock_count = (combined['stock_status'] == 'low_stock').sum()
    overstock_sku_count = (combined['stock_status'] == 'overstock').sum()
    overstock_value = combined[combined['stock_status'] == 'overstock']['est_stock_value'].sum()
    
    print(f"\nSuccess! Summary metrics:")
    print(f"total_sku_count: {int(total_sku_count)}")
    print(f"total_inventory_value: {float(total_inventory_value):,.2f}")
    print(f"dead_stock_count: {int(dead_stock_count)}")
    print(f"low_stock_count: {int(low_stock_count)}")
    print(f"overstock_sku_count: {int(overstock_sku_count)}")
    print(f"overstock_value: {float(overstock_value):,.2f}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
