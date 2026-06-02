#!/usr/bin/env python3
"""
Standalone script to load beginning costs from CSV file.

Usage:
    python scripts/load_beginning_costs.py /path/to/beginning_costs.csv

The CSV should have columns:
- product_id (required)
- cost_unit (required) 
- purchase_tax_id (required)
- notes (optional)
"""

import sys
import os
import logging
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from etl_tasks import load_beginning_costs_from_csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) != 2:
        print("Usage: python load_beginning_costs.py <csv_file_path>")
        print("\nCSV format:")
        print("product_id,cost_unit,purchase_tax_id,notes")
        print("12345,15000.00,5,Beginning cost from legacy system")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        sys.exit(1)
    
    try:
        logger.info(f"Loading beginning costs from {csv_path}")
        result_path = load_beginning_costs_from_csv(csv_path)
        
        if result_path:
            logger.info(f"✅ Successfully loaded beginning costs to: {result_path}")
            print(f"\n✅ Beginning costs loaded successfully!")
            print(f"📁 Output: {result_path}")
            print(f"\nNext steps:")
            print(f"1. Re-run profit ETL for affected dates:")
            print(f"   python scripts/run_profit_etl.py --date 2025-02-10")
            print(f"   python scripts/run_profit_etl.py --date 2025-02-11")
            print(f"2. Check the homepage for updated profit metrics")
        else:
            logger.error("❌ Failed to load beginning costs")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Error loading beginning costs: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
