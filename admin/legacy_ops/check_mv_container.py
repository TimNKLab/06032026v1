import sqlite3

conn = sqlite3.connect('/data-lake/nkdash.db')
cursor = conn.cursor()

# Check MV tables
cursor.execute('SELECT name FROM sqlite_master WHERE type="table" AND name LIKE "mv_%"')
print('MV Tables:', cursor.fetchall())

# Check mv_sales_daily
cursor.execute('SELECT COUNT(*) FROM mv_sales_daily')
print('mv_sales_daily count:', cursor.fetchone())

# Check mv_profit_daily
cursor.execute('SELECT COUNT(*) FROM mv_profit_daily')
print('mv_profit_daily count:', cursor.fetchone())

# Check mv_sales_by_product
cursor.execute('SELECT COUNT(*) FROM mv_sales_by_product')
print('mv_sales_by_product count:', cursor.fetchone())

# Check recent dates in mv_sales_daily
cursor.execute('SELECT date FROM mv_sales_daily ORDER BY date DESC LIMIT 10')
print('mv_sales_daily recent dates:', cursor.fetchall())

# Check recent dates in mv_profit_daily
cursor.execute('SELECT date FROM mv_profit_daily ORDER BY date DESC LIMIT 10')
print('mv_profit_daily recent dates:', cursor.fetchall())

conn.close()
