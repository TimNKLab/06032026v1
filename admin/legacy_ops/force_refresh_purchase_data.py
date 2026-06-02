"""Force-refresh raw, clean, and fact purchase data for a date range.

This script bypasses the Celery queue and invokes the ETL tasks locally to
re-extract purchase invoices from Odoo, write raw parquet files, clean them, and update
fact_purchases. All outputs land in the configured DATA_LAKE_ROOT (e.g. D:\data-lake).

Usage (run from project root):
    docker-compose exec celery-worker python scripts/force_refresh_purchase_data.py \
        --start 2025-05-01 --end 2025-05-31

To refresh a single day, omit --end:
    docker-compose exec celery-worker python scripts/force_refresh_purchase_data.py \
        --start 2025-05-01
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from typing import Optional, Sequence

from etl_tasks import (
    extract_purchase_invoice_lines,
    save_raw_purchase_invoice_lines,
    clean_purchase_invoice_lines,
    update_purchase_star_schema,
)

def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Force refresh purchase data for a date range")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD, inclusive)")
    return parser.parse_args(argv)

def _date_range(start: str, end: str) -> list[str]:
    """Generate a list of dates from start to end (inclusive)."""
    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.strptime(end, "%Y-%m-%d").date() if end else start_date
    delta = end_date - start_date
    return [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") 
            for i in range(delta.days + 1)]

def _run_pipeline_for_date(target_date: str) -> bool:
    """Run the ETL pipeline for a single date."""
    logging.info(f"Processing purchase data for {target_date}")
    
    try:
        # Extract raw purchase invoice lines
        logging.info("Extracting purchase invoice lines...")
        extraction_result = extract_purchase_invoice_lines(target_date)
        
        if not extraction_result:
            logging.warning(f"No purchase data found for {target_date}")
            return False

        # Save raw data
        logging.info("Saving raw purchase data...")
        raw_file = save_raw_purchase_invoice_lines(extraction_result)
        
        if not raw_file:
            logging.error(f"Failed to save raw purchase data for {target_date}")
            return False

        # Clean and process the data
        logging.info("Cleaning purchase data...")
        clean_file = clean_purchase_invoice_lines(raw_file, target_date)
        
        if not clean_file:
            logging.error(f"Failed to clean purchase data for {target_date}")
            return False

        # Update star schema
        logging.info("Updating purchase star schema...")
        update_purchase_star_schema(clean_file, target_date)

        logging.info(f"Successfully processed purchase data for {target_date}")
        return True

    except Exception as e:
        logging.error(f"Error processing purchase data for {target_date}: {str(e)}", exc_info=True)
        return False

def main(argv: Optional[Sequence[str]] = None) -> int:
    """Main entry point."""
    args = _parse_args(argv)
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # Process each date in the range
    success_count = 0
    dates = _date_range(args.start, args.end or args.start)
    
    for date_str in dates:
        if _run_pipeline_for_date(date_str):
            success_count += 1
    
    # Print summary
    total = len(dates)
    logging.info(f"Processed {success_count} of {total} days successfully")
    return 0 if success_count == total else 1

if __name__ == "__main__":
    raise SystemExit(main())
