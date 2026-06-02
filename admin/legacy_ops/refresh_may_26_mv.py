#!/usr/bin/env python3
"""Refresh MVs for ALL dates (full refresh)"""

from services.sqlite_manager import SQLiteManager

manager = SQLiteManager()

print("Refreshing MVs with FULL refresh (all dates)...")

conn = manager.get_writer_conn()
try:
    # Refresh sales MVs - no date_range means full refresh
    print("  Refreshing mv_sales_daily...")
    manager.refresh_mv('mv_sales_daily', 'sales', conn)
    
    print("  Refreshing mv_sales_by_product...")
    manager.refresh_mv('mv_sales_by_product', 'sales', conn)
    
    print("  Refreshing mv_sales_by_principal...")
    manager.refresh_mv('mv_sales_by_principal', 'sales', conn)
    
    # Refresh profit MVs
    print("  Refreshing mv_profit_daily...")
    manager.refresh_mv('mv_profit_daily', 'profit', conn)
    
    print("  Refreshing mv_fact_sales_lines_profit...")
    manager.refresh_mv('mv_fact_sales_lines_profit', 'profit', conn)
finally:
    conn.close()

print("MV refresh complete")

# Verify
with manager.reader_conn() as conn:
    sales_count = conn.execute("SELECT COUNT(*) FROM mv_sales_daily WHERE date = '2026-05-26'").fetchone()[0]
    profit_count = conn.execute("SELECT COUNT(*) FROM mv_profit_daily WHERE date = '2026-05-26'").fetchone()[0]
    total_sales = conn.execute("SELECT COUNT(*) FROM mv_sales_daily").fetchone()[0]
    print(f"mv_sales_daily total rows: {total_sales}")
    print(f"mv_sales_daily rows for 2026-05-26: {sales_count}")
    print(f"mv_profit_daily rows for 2026-05-26: {profit_count}")
