#!/usr/bin/env python3
"""
Manual validation script for profit ETL.
Run with: python scripts/validate_profit_etl.py --date 2025-03-15
"""
import argparse
import datetime as dt
import os
import sys
from pathlib import Path

import polars as pl

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl_tasks import (
    _build_product_cost_events,
    _build_product_cost_latest_daily,
    _build_sales_lines_profit,
    _build_profit_aggregates,
)
from services.duckdb_connector import DuckDBManager


def parse_args():
    parser = argparse.ArgumentParser(description='Validate profit ETL for a specific date')
    parser.add_argument('--date', required=True, help='Target date (YYYY-MM-DD)')
    parser.add_argument('--write-samples', action='store_true', help='Write sample CSV files for inspection')
    return parser.parse_args()


def validate_tax_multiplier():
    """Validate tax multiplier logic."""
    print('=== Validating Tax Multiplier ===')
    df = pl.DataFrame({'tax_id': [5, 2, 7, 6, 99]})
    from etl_tasks import _tax_multiplier_expr
    result = df.with_columns(multiplier=_tax_multiplier_expr('tax_id'))
    print(result)
    expected = [1.0, 1.0, 1.11, 1.11, 1.0]
    assert result['multiplier'].to_list() == expected, f'Tax multiplier failed: {result["multiplier"].to_list()} != {expected}'
    print('✅ Tax multiplier validation passed\n')


def validate_cost_events(target_date: str, write_samples: bool = False):
    """Validate cost events extraction."""
    print(f'=== Validating Cost Events for {target_date} ===')
    try:
        result = _build_product_cost_events(target_date)
        print(f'Cost events rows: {len(result)}')
        if not result.is_empty():
            print('Sample cost events:')
            print(result.head(5))
            # Verify no negative/zero prices
            assert (result['cost_unit_tax_in'] > 0).all(), 'Found non-positive cost_unit_tax_in'
            assert (result['product_id'] != 0).all(), 'Found product_id = 0'
            print('✅ Cost events validation passed')
            if write_samples:
                out_path = f'cost_events_{target_date}.csv'
                result.write_csv(out_path)
                print(f'📄 Wrote sample to {out_path}')
        else:
            print('ℹ️ No cost events found (empty result)')
    except Exception as e:
        print(f'❌ Cost events validation failed: {e}')
        raise
    print()


def validate_latest_daily_cost(target_date: str, write_samples: bool = False):
    """Validate latest daily cost snapshot."""
    print(f'=== Validating Latest Daily Cost for {target_date} ===')
    try:
        result = _build_product_cost_latest_daily(target_date)
        print(f'Latest cost rows: {len(result)}')
        if not result.is_empty():
            print('Sample latest cost:')
            print(result.head(5))
            print('✅ Latest daily cost validation passed')
            if write_samples:
                out_path = f'latest_cost_{target_date}.csv'
                result.write_csv(out_path)
                print(f'📄 Wrote sample to {out_path}')
        else:
            print('ℹ️ No latest cost found (empty result)')
    except Exception as e:
        print(f'❌ Latest daily cost validation failed: {e}')
        raise
    print()


def validate_sales_profit(target_date: str, write_samples: bool = False):
    """Validate sales line profit calculation."""
    print(f'=== Validating Sales Line Profit for {target_date} ===')
    try:
        result = _build_sales_lines_profit(target_date)
        print(f'Sales profit rows: {len(result)}')
        if not result.is_empty():
            print('Sample sales profit:')
            print(result.head(5))
            # Basic validation
            assert (result['gross_profit'] == result['revenue_tax_in'] - result['cogs_tax_in']).all(), 'Profit calculation mismatch'
            assert (result['cogs_tax_in'] == result['cost_unit_tax_in'] * result['quantity']).all(), 'COGS calculation mismatch'
            print('✅ Sales line profit validation passed')
            if write_samples:
                out_path = f'sales_profit_{target_date}.csv'
                result.write_csv(out_path)
                print(f'📄 Wrote sample to {out_path}')
        else:
            print('ℹ️ No sales profit found (empty result)')
    except Exception as e:
        print(f'❌ Sales line profit validation failed: {e}')
        raise
    print()


def validate_profit_aggregates(target_date: str, write_samples: bool = False):
    """Validate profit aggregates."""
    print(f'=== Validating Profit Aggregates for {target_date} ===')
    try:
        profit_df = _build_sales_lines_profit(target_date)
        if profit_df.is_empty():
            print('ℹ️ No profit data to aggregate')
            return
        
        daily, by_product = _build_profit_aggregates(profit_df)
        print(f'Daily aggregates rows: {len(daily)}')
        print(f'By-product aggregates rows: {len(by_product)}')
        
        if not daily.is_empty():
            print('Sample daily aggregates:')
            print(daily)
            # Validate daily sums
            assert daily['gross_profit'][0] == pytest.approx(profit_df['gross_profit'].sum()), 'Daily profit sum mismatch'
            print('✅ Daily aggregates validation passed')
            if write_samples:
                daily.write_csv(f'daily_agg_{target_date}.csv')
                print(f'📄 Wrote daily aggregates to daily_agg_{target_date}.csv')
        
        if not by_product.is_empty():
            print('Sample by-product aggregates:')
            print(by_product.head(5))
            print('✅ By-product aggregates validation passed')
            if write_samples:
                by_product.write_csv(f'by_product_agg_{target_date}.csv')
                print(f'📄 Wrote by-product aggregates to by_product_agg_{target_date}.csv')
    except Exception as e:
        print(f'❌ Profit aggregates validation failed: {e}')
        raise
    print()


def validate_duckdb_views(target_date: str):
    """Validate DuckDB views are accessible."""
    print(f'=== Validating DuckDB Views for {target_date} ===')
    try:
        conn = DuckDBManager().get_connection()
        
        # Test each new view
        views = [
            'fact_product_cost_events',
            'fact_product_cost_latest_daily',
            'fact_sales_lines_profit',
            'agg_profit_daily',
            'agg_profit_daily_by_product',
        ]
        
        for view in views:
            try:
                result = conn.execute(f'SELECT COUNT(*) as cnt FROM {view}').fetchone()
                print(f'{view}: {result[0]} rows')
            except Exception as e:
                print(f'❌ View {view} failed: {e}')
                raise
        
        print('✅ DuckDB views validation passed')
    except Exception as e:
        print(f'❌ DuckDB views validation failed: {e}')
        raise
    print()


def main():
    args = parse_args()
    target_date = args.date
    
    # Validate date format
    try:
        dt.date.fromisoformat(target_date)
    except ValueError:
        print(f'Invalid date format: {target_date}. Use YYYY-MM-DD')
        sys.exit(1)
    
    print(f'🔍 Validating Profit ETL for {target_date}')
    print('=' * 50)
    
    try:
        validate_tax_multiplier()
        validate_cost_events(target_date, args.write_samples)
        validate_latest_daily_cost(target_date, args.write_samples)
        validate_sales_profit(target_date, args.write_samples)
        validate_profit_aggregates(target_date, args.write_samples)
        validate_duckdb_views(target_date)
        
        print('🎉 All validations passed!')
        if args.write_samples:
            print('📄 Sample CSV files written for manual inspection')
    except Exception as e:
        print(f'💥 Validation failed: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()
