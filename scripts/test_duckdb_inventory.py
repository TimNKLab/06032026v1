import sys
from pathlib import Path
from datetime import date

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.duckdb_connector import query_inventory_snapshot, query_sales_by_product_duckdb

# Test with a single date (May 26, 2026 - recent date from logs)
test_date = date(2026, 5, 26)

print(f"Testing query_inventory_snapshot with date: {test_date}")
try:
    result = query_inventory_snapshot(test_date)
    print(f"Success! Returned {len(result)} rows")
    print(f"Columns: {result.columns.tolist()}")
    if not result.empty:
        print(f"Sample data:\n{result.head()}")
    else:
        print("Warning: Empty result")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*50 + "\n")

# Test sales by product with single date range
start_date = date(2026, 5, 26)
end_date = date(2026, 5, 26)

print(f"Testing query_sales_by_product_duckdb with range: {start_date} to {end_date}")
try:
    result = query_sales_by_product_duckdb(start_date, end_date)
    print(f"Success! Returned {len(result)} rows")
    print(f"Columns: {result.columns.tolist()}")
    if not result.empty:
        print(f"Sample data:\n{result.head()}")
    else:
        print("Warning: Empty result")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
