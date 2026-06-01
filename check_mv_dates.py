#!/usr/bin/env python3
"""Check what dates are in MVs and parquet files"""

from services.sqlite_manager import SQLiteManager
import polars as pl
import os
import glob
from datetime import date

manager = SQLiteManager()

# Check MV dates
with manager.reader_conn() as conn:
    sales_dates = conn.execute('SELECT date FROM mv_sales_daily ORDER BY date').fetchall()
    profit_dates = conn.execute('SELECT date FROM mv_profit_daily ORDER BY date').fetchall()
    print('MV sales_daily dates:', sales_dates)
    print('MV profit_daily dates:', profit_dates)
    print(f'MV has {len(sales_dates)} rows total')

# Check parquet files
data_lake = os.environ.get('DATA_LAKE_ROOT', 'D:/data-lake')
sales_path = f'{data_lake}/star-schema/agg_sales_daily/**/*.parquet'
sales_files = glob.glob(sales_path, recursive=True)
print(f'\nTotal sales aggregate parquet files: {len(sales_files)}')

# Read all sales aggregate files to see what dates exist
all_dates = []
for f in sales_files:
    df = pl.read_parquet(f)
    if 'date' in df.columns:
        dates = df['date'].unique().to_list()
        all_dates.extend(dates)

print(f'Parquet files have {len(set(all_dates))} unique dates')
print(f'Dates in parquet files include 2026-05-26: {date(2026, 5, 26) in set(all_dates)}')
