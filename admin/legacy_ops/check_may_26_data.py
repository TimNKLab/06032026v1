#!/usr/bin/env python3
"""Check if MVs have data for May 26th"""

from services.sqlite_manager import SQLiteManager

manager = SQLiteManager()
with manager.reader_conn() as conn:
    # Check sales daily MV
    result = conn.execute('SELECT date FROM mv_sales_daily ORDER BY date DESC LIMIT 5').fetchall()
    print('mv_sales_daily latest dates:', result)
    
    # Check profit daily MV
    result2 = conn.execute('SELECT date FROM mv_profit_daily ORDER BY date DESC LIMIT 5').fetchall()
    print('mv_profit_daily latest dates:', result2)
    
    # Check if May 26 exists in sales
    may26_sales = conn.execute("SELECT COUNT(*) FROM mv_sales_daily WHERE date = '2026-05-26'").fetchone()
    print(f'mv_sales_daily count for 2026-05-26: {may26_sales[0]}')
    
    # Check if May 26 exists in profit
    may26_profit = conn.execute("SELECT COUNT(*) FROM mv_profit_daily WHERE date = '2026-05-26'").fetchone()
    print(f'mv_profit_daily count for 2026-05-26: {may26_profit[0]}')
