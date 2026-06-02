#!/usr/bin/env python3
"""
Drop legacy materialized view tables from DuckDB database.

These MVs were migrated to SQLite and are no longer used in DuckDB.
This script cleans up the legacy tables to free up disk space.
"""

import os
import duckdb
import sys

def drop_legacy_mvs():
    """Drop legacy MV tables from DuckDB."""
    data_lake = os.environ.get('DATA_LAKE_ROOT') or os.environ.get('DATA_LAKE_PATH', '/data-lake')
    db_path = f"{data_lake}/cache/nkdash.duckdb"
    
    # Legacy MVs that were migrated to SQLite
    legacy_mvs = [
        'mv_profit_daily',
        'mv_sales_by_product',
        'mv_sales_by_principal',
        'mv_refresh_metadata',
        'mv_inventory_daily',
        'mv_product_velocity',
        'mv_inventory_status'
    ]
    
    if not os.path.exists(db_path):
        print(f"DuckDB database not found at {db_path}")
        return
    
    print(f"Connecting to DuckDB at {db_path}...")
    conn = duckdb.connect(database=db_path, read_only=False)
    
    try:
        # Check which tables exist
        tables_result = conn.execute("SHOW TABLES").fetchall()
        existing_tables = {t[0] for t in tables_result}
        print(f"Existing tables: {sorted(existing_tables)}")
        
        # Drop legacy MVs
        dropped_count = 0
        for mv_name in legacy_mvs:
            if mv_name in existing_tables:
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {mv_name}")
                    print(f"✓ Dropped {mv_name}")
                    dropped_count += 1
                except Exception as e:
                    print(f"✗ Failed to drop {mv_name}: {e}")
            else:
                print(f"- {mv_name} not found (skipping)")
        
        print(f"\nDropped {dropped_count} legacy MV tables")
        
        # Show remaining tables
        remaining_result = conn.execute("SHOW TABLES").fetchall()
        remaining_tables = {t[0] for t in remaining_result}
        print(f"Remaining tables: {sorted(remaining_tables)}")
        
    finally:
        conn.close()
        print("DuckDB connection closed")

if __name__ == "__main__":
    drop_legacy_mvs()
