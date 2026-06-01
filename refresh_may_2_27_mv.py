from services.sqlite_manager import SQLiteManager

mgr = SQLiteManager()
conn = mgr.get_writer_conn()

print("Refreshing sales daily MV for May 2-27...")
mgr.refresh_mv("mv_sales_daily", "sales", conn, date_range=('2026-05-02', '2026-05-27'))
print("Sales daily MV refresh complete.")

print("Refreshing profit daily MV for May 2-27...")
mgr.refresh_mv("mv_profit_daily", "profit", conn, date_range=('2026-05-02', '2026-05-27'))
print("Profit daily MV refresh complete.")
