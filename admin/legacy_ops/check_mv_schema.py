#!/usr/bin/env python3
"""Check schema of mv_profit_daily"""

from services.sqlite_manager import SQLiteManager

manager = SQLiteManager()
with manager.reader_conn() as conn:
    result = conn.execute('PRAGMA table_info(mv_profit_daily)').fetchall()
    print("mv_profit_daily schema:")
    for col in result:
        print(f"  {col}")
