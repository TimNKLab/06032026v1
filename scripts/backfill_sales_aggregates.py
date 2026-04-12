#!/usr/bin/env python
"""Backfill sales aggregates for a date range."""
import sys
import os
from datetime import date, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_tasks import update_sales_aggregates


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Backfill sales aggregates')
    parser.add_argument('--start', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    args = parser.parse_args()
    
    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    
    print(f"Backfilling sales aggregates from {start_date} to {end_date}")
    
    current = start_date
    while current <= end_date:
        date_str = current.isoformat()
        print(f"\nProcessing {date_str}...")
        if args.dry_run:
            print(f"  [DRY RUN] Would run update_sales_aggregates({date_str})")
        else:
            result = update_sales_aggregates(date_str)
            if result:
                print(f"  Created: {result}")
            else:
                print(f"  No data or error")
        current += timedelta(days=1)
    
    print("\nBackfill complete!")


if __name__ == '__main__':
    main()
