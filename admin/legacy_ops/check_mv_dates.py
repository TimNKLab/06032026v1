#!/usr/bin/env python3
"""
Diagnostic: compare MV date ranges vs parquet date ranges.
Run: docker-compose exec dash-app python scripts/check_mv_dates.py
"""
import os
import sys
import glob
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.duckdb_connector import DuckDBManager
from etl.config import (
    AGG_PROFIT_DAILY_PATH, AGG_SALES_DAILY_PATH,
    AGG_SALES_DAILY_BY_PRODUCT_PATH, AGG_SALES_DAILY_BY_PRINCIPAL_PATH,
)

MV_TO_PATH = {
    "mv_profit_daily": str(AGG_PROFIT_DAILY_PATH),
    "mv_sales_daily": str(AGG_SALES_DAILY_PATH),
    "mv_sales_by_product": str(AGG_SALES_DAILY_BY_PRODUCT_PATH),
    "mv_sales_by_principal": str(AGG_SALES_DAILY_BY_PRINCIPAL_PATH),
}


def check():
    manager = DuckDBManager()
    # Use read_only to avoid conflicting with dash-app's write lock
    data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    db_path = f"{data_lake}/cache/nkdash.duckdb"
    import duckdb as _duckdb
    conn = _duckdb.connect(database=db_path, read_only=True)

    print("=" * 60)
    print("MV DATE RANGE DIAGNOSTIC")
    print("=" * 60)

    for mv_name, parquet_path in MV_TO_PATH.items():
        print(f"\n--- {mv_name} ---")

        # Check MV
        try:
            result = conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{mv_name}' AND table_type = 'BASE TABLE'
            """).fetchone()
            if result[0] == 0:
                print(f"  MV: DOES NOT EXIST")
            else:
                row = conn.execute(f"SELECT COUNT(*), MIN(date), MAX(date) FROM {mv_name}").fetchone()
                print(f"  MV rows: {row[0]}, min_date: {row[1]}, max_date: {row[2]}")
        except Exception as e:
            print(f"  MV error: {e}")

        # Check parquet
        try:
            files = glob.glob(f"{parquet_path}/**/*.parquet", recursive=True)
            if not files:
                print(f"  Parquet: NO FILES at {parquet_path}")
            else:
                row = conn.execute(f"""
                    SELECT COUNT(*), MIN(date), MAX(date)
                    FROM read_parquet('{parquet_path}/**/*.parquet', 
                                      union_by_name=True, hive_partitioning=1)
                """).fetchone()
                print(f"  Parquet rows: {row[0]}, min_date: {row[1]}, max_date: {row[2]}")
                print(f"  Parquet files: {len(files)}")
        except Exception as e:
            print(f"  Parquet error: {e}")

    # Check lru_cache state
    print("\n--- CACHE STATE ---")
    try:
        from services.profit_metrics import query_profit_trends, query_profit_summary
        print(f"  query_profit_trends cache: {query_profit_trends.cache_info()}")
        print(f"  query_profit_summary cache: {query_profit_summary.cache_info()}")
    except Exception as e:
        print(f"  Cache check error: {e}")

    try:
        from services.duckdb_connector import query_sales_trends
        print(f"  query_sales_trends cache: {query_sales_trends.cache_info()}")
    except Exception as e:
        print(f"  Sales cache check error: {e}")

    print("\n--- mv_refresh_metadata ---")
    try:
        rows = conn.execute("SELECT * FROM mv_refresh_metadata ORDER BY last_refresh_date DESC").fetchall()
        if rows:
            for row in rows:
                print(f"  {row}")
        else:
            print("  (empty)")
    except Exception as e:
        print(f"  metadata error: {e}")

    print("\n--- _materialized_views tracking set ---")
    print("  (read_only mode — tracking set not available)")


if __name__ == "__main__":
    check()
