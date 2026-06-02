from services.sqlite_manager import SQLiteManager
import sqlite3

mgr = SQLiteManager()
conn = mgr.get_writer_conn()

# Delete May 2-27 data from MVs first
print("Deleting May 2-27 from mv_sales_daily...")
conn.execute("DELETE FROM mv_sales_daily WHERE date >= '2026-05-02' AND date <= '2026-05-27'")
conn.commit()
print("Deleted.")

print("Deleting May 2-27 from mv_profit_daily...")
conn.execute("DELETE FROM mv_profit_daily WHERE date >= '2026-05-02' AND date <= '2026-05-27'")
conn.commit()
print("Deleted.")

# Now do incremental refresh for May 2-27
print("Refreshing sales daily MV for May 2-27 (incremental)...")
mgr.refresh_mv("mv_sales_daily", "sales", conn, date_range=('2026-05-02', '2026-05-27'))
print("Sales daily MV refresh complete.")

print("Refreshing profit daily MV for May 2-27 (incremental)...")
mgr.refresh_mv("mv_profit_daily", "profit", conn, date_range=('2026-05-02', '2026-05-27'))
print("Profit daily MV refresh complete.")
