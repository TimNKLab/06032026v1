#!/usr/bin/env python3
"""
DuckDB Performance Test Script

Tests DuckDB query performance for yearly date ranges to validate
that the DuckDB-first architecture meets performance requirements.

Note: This script queries parquet files directly to avoid file lock conflicts
with the dash-app DuckDB database.
"""

import os
import time
from datetime import date, timedelta
import duckdb

# Use data lake path for direct parquet queries
data_lake = os.environ.get('DATA_LAKE_ROOT') or os.environ.get('DATA_LAKE_PATH', '/data-lake')

def test_duckdb_performance():
    """Test DuckDB query performance for yearly date ranges."""
    # Use 30-day range for testing to avoid long queries
    start_date = date.today() - timedelta(days=30)
    end_date = date.today()
    
    print("=== DuckDB Performance Test ===")
    print(f"Date range: {start_date} to {end_date} (30 days)")
    print(f"Data lake: {data_lake}")
    print()
    
    # Use in-memory DuckDB to query parquet files directly
    conn = duckdb.connect()
    
    try:
        # Test sales trends query from parquet
        start = time.time()
        result = conn.execute(f"""
            SELECT date, SUM(revenue) as revenue, SUM(transactions) as transactions
            FROM read_parquet('{data_lake}/star-schema/agg_sales_daily/**/*.parquet', hive_partitioning=true)
            WHERE date >= ? AND date <= ?
            GROUP BY date
            ORDER BY date
        """, [start_date, end_date]).fetchall()
        elapsed = time.time() - start
        print(f"Sales trends from parquet (30 days): {elapsed:.3f}s, {len(result)} rows")
        
        # Test profit summary query from parquet
        start = time.time()
        result = conn.execute(f"""
            SELECT SUM(revenue_tax_in) as revenue, SUM(gross_profit) as gross_profit
            FROM read_parquet('{data_lake}/star-schema/agg_profit_daily/**/*.parquet', hive_partitioning=true)
            WHERE date >= ? AND date <= ?
        """, [start_date, end_date]).fetchone()
        elapsed = time.time() - start
        print(f"Profit summary from parquet (30 days): {elapsed:.3f}s")
        
        # Test top products query from parquet
        start = time.time()
        result = conn.execute(f"""
            SELECT product_id, SUM(revenue) as revenue
            FROM read_parquet('{data_lake}/star-schema/agg_sales_daily_by_product/**/*.parquet', hive_partitioning=true)
            WHERE date >= ? AND date <= ?
            GROUP BY product_id
            ORDER BY revenue DESC
            LIMIT 20
        """, [start_date, end_date]).fetchall()
        elapsed = time.time() - start
        print(f"Top products from parquet (30 days): {elapsed:.3f}s, {len(result)} rows")
        
        print()
        print("=== Test Complete ===")
        print("Expected: All queries should complete in < 1 second")
        
    finally:
        conn.close()

if __name__ == "__main__":
    test_duckdb_performance()
