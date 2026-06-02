#!/usr/bin/env python3
"""Check if parquet files exist for May 26th"""

import os
import glob

data_lake = os.environ.get('DATA_LAKE_ROOT', 'D:/data-lake')

# Check sales aggregates
sales_path = f'{data_lake}/star-schema/agg_sales_daily/year=2026/month=05/day=26/'
sales_files = glob.glob(sales_path + '*.parquet')
print(f'May 26 sales aggregates: {len(sales_files)} files')
if sales_files:
    print(f'  {sales_files[0]}')

# Check profit aggregates
profit_path = f'{data_lake}/star-schema/agg_profit_daily/year=2026/month=05/day=26/'
profit_files = glob.glob(profit_path + '*.parquet')
print(f'May 26 profit aggregates: {len(profit_files)} files')
if profit_files:
    print(f'  {profit_files[0]}')

# Check fact files
fact_sales_path = f'{data_lake}/star-schema/fact_sales/year=2026/month=05/day=26/'
fact_sales_files = glob.glob(fact_sales_path + '*.parquet')
print(f'May 26 fact_sales: {len(fact_sales_files)} files')
