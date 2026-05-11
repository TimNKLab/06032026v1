#!/usr/bin/env python3
"""Test script to check schema of stock snapshot parquet files"""

import duckdb
import os

def test_stock_snapshot_schema():
    conn = duckdb.connect()
    
    # Test reading one file
    test_file = "/data-lake/star-schema/fact_stock_on_hand_snapshot/year=2025/month=09/day=22/fact_stock_on_hand_snapshot_2025-09-22.parquet"
    
    print("Testing schema of:", test_file)
    
    try:
        # Get schema
        result = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{test_file}')").fetchall()
        print("Schema:")
        for row in result:
            print(f"  {row[0]}: {row[1]}")
        
        # Try to read a few rows
        print("\nSample data:")
        result = conn.execute(f"SELECT * FROM read_parquet('{test_file}') LIMIT 3").fetchall()
        for row in result:
            print(f"  {row}")
            
        # Test the problematic SQL
        print("\nTesting CTE with correct column names:")
        sql = f"""
        WITH latest_stock AS (
            SELECT
                product_id,
                quantity as on_hand_qty,
                (quantity - reserved_quantity) as available_qty,
                location_id,
                ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY snapshot_date DESC) as rn
            FROM read_parquet('{test_file}')
            WHERE product_id IS NOT NULL
        )
        SELECT product_id, on_hand_qty FROM latest_stock WHERE rn = 1 LIMIT 3
        """
        result = conn.execute(sql).fetchall()
        print("CTE result:")
        for row in result:
            print(f"  {row}")
            
        # Test the full mv_inventory_status SQL
        print("\nTesting full mv_inventory_status SQL:")
        full_sql = f"""
        WITH latest_stock AS (
            SELECT
                product_id,
                quantity as on_hand_qty,
                (quantity - reserved_quantity) as available_qty,
                location_id,
                ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY snapshot_date DESC) as rn
            FROM read_parquet('{test_file}')
            WHERE product_id IS NOT NULL
        ),
        velocity AS (
            SELECT
                product_id,
                AVG(quantity) as avg_daily_sold
            FROM read_parquet('/data-lake/star-schema/fact_sales/**/*.parquet', union_by_name=True, hive_partitioning=1)
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
              AND quantity IS NOT NULL
              AND quantity > 0
            GROUP BY product_id
        )
        SELECT
            ls.product_id,
            ls.on_hand_qty,
            ls.available_qty,
            ls.location_id,
            COALESCE(v.avg_daily_sold, 0) as avg_daily_sold,
            CASE
                WHEN COALESCE(v.avg_daily_sold, 0) > 0 THEN ls.on_hand_qty / NULLIF(v.avg_daily_sold, 0)
                WHEN ls.on_hand_qty > 0 THEN 999999
                ELSE 0
            END as days_of_cover
        FROM latest_stock ls
        LEFT JOIN velocity v ON ls.product_id = v.product_id
        WHERE ls.rn = 1
        LIMIT 3
        """
        try:
            result = conn.execute(full_sql).fetchall()
            print("Full SQL result:")
            for row in result:
                print(f"  {row}")
        except Exception as e:
            print(f"Full SQL Error: {e}")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_stock_snapshot_schema()
