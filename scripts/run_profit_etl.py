#!/usr/bin/env python3
"""
Run profit ETL for a specific date.
Usage: python scripts/run_profit_etl.py --date 2025-03-15 [--dry-run]
"""
import argparse
import datetime as dt
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl_tasks import (
    update_product_cost_events,
    update_product_cost_latest_daily,
    update_sales_lines_profit,
    update_profit_aggregates,
)


def parse_args():
    parser = argparse.ArgumentParser(description='Run profit ETL for a specific date')
    parser.add_argument('--date', required=True, help='Target date (YYYY-MM-DD)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be run without executing')
    return parser.parse_args()


def main():
    args = parse_args()
    target_date = args.date
    
    # Validate date format
    try:
        dt.date.fromisoformat(target_date)
    except ValueError:
        print(f'Invalid date format: {target_date}. Use YYYY-MM-DD')
        sys.exit(1)
    
    print(f'🚀 Running Profit ETL for {target_date}')
    print('=' * 50)
    
    # Define the pipeline steps
    steps = [
        ('Cost Events', update_product_cost_events, target_date),
        ('Latest Daily Cost', update_product_cost_latest_daily, target_date),
        ('Sales Line Profit', update_sales_lines_profit, target_date),
        ('Profit Aggregates', update_profit_aggregates, target_date),
    ]
    
    results = {}
    
    for step_name, task_func, task_arg in steps:
        print(f'\n📋 Step: {step_name}')
        print(f'   Task: {task_func.__name__}')
        print(f'   Args: {task_arg}')
        
        if args.dry_run:
            print('   ⏭️  [DRY RUN] Would execute task')
            results[step_name] = 'DRY_RUN'
        else:
            try:
                result = task_func(task_arg)
                if isinstance(result, dict):
                    print(f'   ✅ Completed: {result}')
                    results[step_name] = result
                else:
                    print(f'   ✅ Completed: {result}')
                    results[step_name] = result
            except Exception as e:
                print(f'   ❌ Failed: {e}')
                results[step_name] = f'ERROR: {e}'
                # Continue with other steps for partial success
    
    print('\n' + '=' * 50)
    print('📊 Summary:')
    for step_name, result in results.items():
        status = '✅' if not str(result).startswith('ERROR') and result != 'DRY_RUN' else '⏭️' if result == 'DRY_RUN' else '❌'
        print(f'   {status} {step_name}: {result}')
    
    if not args.dry_run:
        error_count = sum(1 for r in results.values() if str(r).startswith('ERROR'))
        if error_count == 0:
            print('\n🎉 All steps completed successfully!')
        else:
            print(f'\n⚠️ {error_count} step(s) failed. Check logs for details.')
            sys.exit(1)


if __name__ == '__main__':
    main()
