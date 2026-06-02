import sqlite3
import os

# Use the same database path as SQLiteManager
data_lake = os.environ.get('DATA_LAKE_ROOT') or os.environ.get('DATA_LAKE_PATH', '/data-lake')
db_path = f"{data_lake}/cache/nkdash.sqlite"

print(f'Checking database: {db_path}')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check what tables exist
cursor.execute('SELECT name FROM sqlite_master WHERE type=\"table\"')
tables = cursor.fetchall()
print(f'Tables in database: {tables}')

# Check schema of mv_sales_daily
if ('mv_sales_daily',) in tables:
    cursor.execute('PRAGMA table_info(mv_sales_daily)')
    schema = cursor.fetchall()
    print(f'mv_sales_daily schema: {schema}')

    # Check all dates in MV
    cursor.execute('SELECT date, revenue FROM mv_sales_daily ORDER BY date')
    rows = cursor.fetchall()
    print(f'MV all dates and revenue: {rows}')

    # Check row count for May 1st
    cursor.execute('SELECT COUNT(*) FROM mv_sales_daily WHERE date = "2026-05-01"')
    count = cursor.fetchone()[0]
    print(f'MV May 1st row count: {count}')

    # Check distinct dates in MV
    cursor.execute('SELECT MIN(date), MAX(date), COUNT(DISTINCT date) FROM mv_sales_daily')
    min_date, max_date, distinct_dates = cursor.fetchone()
    print(f'MV date range: {min_date} to {max_date}, distinct dates: {distinct_dates}')

    # Check total row count in MV
    cursor.execute('SELECT COUNT(*) FROM mv_sales_daily')
    total_count = cursor.fetchone()[0]
    print(f'MV total row count: {total_count}')

    # Check total revenue
    cursor.execute('SELECT SUM(revenue) FROM mv_sales_daily')
    total_revenue = cursor.fetchone()[0]
    print(f'MV total revenue: {total_revenue}')
else:
    print('mv_sales_daily table does not exist!')

conn.close()
