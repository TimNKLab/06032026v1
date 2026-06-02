#!/usr/bin/env python3
"""
Create sample data for profit ETL validation.
Run with: python scripts/validate_profit_etl_sample_data.py
"""
import datetime as dt
import tempfile
import os
from pathlib import Path

import polars as pl

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl_tasks import (
    _partition_path,
    _write_partitioned,
    STAR_SCHEMA_PATH,
    FACT_PRODUCT_COST_LATEST_DAILY_PATH,
)


def create_sample_data():
    """Create sample data for a specific date in temp directory."""
    target_date = '2025-03-15'
    
    # Create temp directory structure
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f'Creating sample data in {tmpdir}')
        
        # 1. Sample purchases data (source for cost events)
        purchases_df = pl.DataFrame({
            'date': [dt.date(2025, 3, 15)] * 4,
            'move_id': [101, 102, 103, 104],
            'move_line_id': [1001, 1002, 1003, 1004],
            'product_id': [1, 2, 1, 3],
            'actual_price': [10.0, 20.0, 12.0, -5.0],  # one negative price (bonus)
            'quantity': [5.0, 3.0, 2.0, 1.0],
            'tax_id': [5, 7, 2, 5],
        })
        
        # 2. Sample sales data (POS)
        sales_df = pl.DataFrame({
            'date': [dt.date(2025, 3, 15)] * 3,
            'order_id': [501, 502, 503],
            'line_id': [2001, 2002, 2003],
            'product_id': [1, 2, 4],
            'quantity': [2.0, 1.0, 3.0],
            'revenue': [25.0, 22.2, 60.0],
        })
        
        # 3. Sample invoice sales data
        invoice_sales_df = pl.DataFrame({
            'date': [dt.date(2025, 3, 15)] * 2,
            'move_id': [601, 602],
            'move_line_id': [3001, 3002],
            'product_id': [1, 2],
            'quantity': [1.0, 2.0],
            'price_unit': [15.0, 18.0],
            'tax_id': [7, 5],
        })
        
        # Write to temp star-schema structure
        base_path = os.path.join(tmpdir, 'star-schema')
        
        # Write purchases
        purchases_path = _partition_path(os.path.join(base_path, 'fact_purchases'), target_date)
        os.makedirs(purchases_path, exist_ok=True)
        purchases_df.write_parquet(os.path.join(purchases_path, f'fact_purchases_{target_date}.parquet'))
        print(f'✅ Wrote purchases: {len(purchases_df)} rows')
        
        # Write sales
        for name, df in [('fact_sales', sales_df), ('fact_invoice_sales', invoice_sales_df)]:
            path = _partition_path(os.path.join(base_path, name), target_date)
            os.makedirs(path, exist_ok=True)
            df.write_parquet(os.path.join(path, f'{name}_{target_date}.parquet'))
            print(f'✅ Wrote {name}: {len(df)} rows')
        
        # Now run ETL functions with mocked paths
        import sys
        from unittest.mock import patch
        
        with patch('etl_tasks.STAR_SCHEMA_PATH', os.path.join(base_path)), \
             patch('etl_tasks.FACT_PRODUCT_COST_LATEST_DAILY_PATH', os.path.join(base_path, 'fact_product_cost_latest_daily')):
            
            # Import after patching
            from etl_tasks import (
                _build_product_cost_events,
                _build_product_cost_latest_daily,
                _build_sales_lines_profit,
                _build_profit_aggregates,
            )
            
            print('\n=== Running ETL Functions ===')
            
            # 1. Build cost events
            cost_events = _build_product_cost_events(target_date)
            print(f'Cost events: {len(cost_events)} rows')
            if not cost_events.is_empty():
                print(cost_events)
                # Write cost events
                cost_events_path = _write_partitioned(
                    cost_events, 
                    os.path.join(base_path, 'fact_product_cost_events'), 
                    target_date, 
                    'fact_product_cost_events'
                )
                print(f'✅ Wrote cost events to {cost_events_path}')
            
            # 2. Build latest daily cost (needs cost_events to exist)
            latest_cost = _build_product_cost_latest_daily(target_date)
            print(f'Latest cost: {len(latest_cost)} rows')
            if not latest_cost.is_empty():
                print(latest_cost)
                latest_cost_path = _write_partitioned(
                    latest_cost,
                    os.path.join(base_path, 'fact_product_cost_latest_daily'),
                    target_date,
                    'fact_product_cost_latest_daily'
                )
                print(f'✅ Wrote latest cost to {latest_cost_path}')
            
            # 3. Build sales line profit
            sales_profit = _build_sales_lines_profit(target_date)
            print(f'Sales profit: {len(sales_profit)} rows')
            if not sales_profit.is_empty():
                print(sales_profit)
                sales_profit_path = _write_partitioned(
                    sales_profit,
                    os.path.join(base_path, 'fact_sales_lines_profit'),
                    target_date,
                    'fact_sales_lines_profit'
                )
                print(f'✅ Wrote sales profit to {sales_profit_path}')
            
            # 4. Build aggregates
            if not sales_profit.is_empty():
                daily_agg, by_product_agg = _build_profit_aggregates(sales_profit)
                print(f'Daily aggregates: {len(daily_agg)} rows')
                print(f'By-product aggregates: {len(by_product_agg)} rows')
                
                if not daily_agg.is_empty():
                    print('Daily:')
                    print(daily_agg)
                    daily_path = _write_partitioned(
                        daily_agg,
                        os.path.join(base_path, 'agg_profit_daily'),
                        target_date,
                        'agg_profit_daily'
                    )
                    print(f'✅ Wrote daily aggregates to {daily_path}')
                
                if not by_product_agg.is_empty():
                    print('By-product:')
                    print(by_product_agg)
                    by_product_path = _write_partitioned(
                        by_product_agg,
                        os.path.join(base_path, 'agg_profit_daily_by_product'),
                        target_date,
                        'agg_profit_daily_by_product'
                    )
                    print(f'✅ Wrote by-product aggregates to {by_product_path}')
        
        print(f'\n📁 Sample data created in: {tmpdir}')
        print('You can now run: python scripts/validate_profit_etl.py --date 2025-03-15')
        print('(Note: This uses the real data lake, not the temp data)')


if __name__ == '__main__':
    create_sample_data()
