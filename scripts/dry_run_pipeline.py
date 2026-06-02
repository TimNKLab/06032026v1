"""
Dry Run Pipeline Validator
===========================

This script simulates the full ETL flow without requiring a real Odoo connection.
It mocks the extraction phase and verifies that the transform and load phases
work correctly with the new modular structure.
"""
import os
import logging
from datetime import date
import polars as pl

from etl.tasks import get_task

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DryRun")

# Setup environment for the test
os.environ['RAW_PATH'] = '/home/user/data-lake/raw'
os.environ['CLEAN_PATH'] = '/home/user/data-lake/clean'
os.environ['STAR_SCHEMA_PATH'] = '/home/user/data-lake/star-schema'
os.makedirs('/home/user/data-lake/admin/logs', exist_ok=True)

def mock_extraction_result(dataset: str, target_date: str):
    """Returns a sample dictionary that mimics Odoo extraction results."""
    if dataset == 'pos':
        return {
            'target_date': target_date,
            'lines': [
                {'order_id': 101, 'order_ref': 'POS/001', 'product_id': 1, 'qty': 2, 'price_subtotal_incl': 10000, 'discount_amount': 0, 'order_date': '2026-06-01 10:00:00'},
                {'order_id': 101, 'order_ref': 'POS/001', 'product_id': 2, 'qty': 1, 'price_subtotal_incl': 5000, 'discount_amount': 500, 'order_date': '2026-06-01 10:00:00'},
            ]
        }
    elif dataset == 'sales_invoice':
        return {
            'target_date': target_date,
            'lines': [
                {'move_id': 201, 'move_name': 'INV/001', 'product_id': 1, 'price_unit': 4500, 'quantity': 2, 'tax_id': 5, 'move_date': '2026-06-01'},
            ]
        }
    return {'target_date': target_date, 'lines': []}

def run_test_flow(target_date: str):
    logger.info(f"🚀 Starting Dry Run for {target_date}")
    
    # --- FLOW 1: POS ---
    logger.info("Testing POS Flow...")
    raw_pos = get_task('save_raw_data')(mock_extraction_result('pos', target_date))
    logger.info(f"Raw POS saved to: {raw_pos}")
    
    clean_pos_path = get_task('clean_pos_data')(raw_pos, target_date)
    logger.info(f"Clean POS saved to: {clean_pos_path}")
    
    # Load the cleaned DF before passing to update_star_schema
    clean_pos_df = pl.read_parquet(clean_pos_path)
    fact_pos = get_task('update_star_schema')(clean_pos_df, target_date)
    logger.info(f"Fact POS saved to: {fact_pos}")

    # --- FLOW 2: Invoice Sales ---
    logger.info("Testing Invoice Sales Flow...")
    raw_inv = get_task('save_raw_sales_invoice_lines')(mock_extraction_result('sales_invoice', target_date))
    logger.info(f"Raw Inv saved to: {raw_inv}")
    
    clean_inv_path = get_task('clean_sales_invoice_lines')(raw_inv, target_date)
    logger.info(f"Clean Inv saved to: {clean_inv_path}")
    
    # Load the cleaned DF before passing to update_star_schema
    clean_inv_df = pl.read_parquet(clean_inv_path)
    fact_inv = get_task('update_invoice_sales_star_schema')(clean_inv_df, target_date)
    logger.info(f"Fact Inv saved to: {fact_inv}")

    logger.info("✅ Dry Run Completed Successfully!")

if __name__ == "__main__":
    run_test_flow("2026-06-01")
