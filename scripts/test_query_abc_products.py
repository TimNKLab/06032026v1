import sys
from pathlib import Path
from datetime import date

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.inventory_metrics import _query_abc_products

# Test with recent date range
start_date = date(2026, 5, 20)
end_date = date(2026, 5, 26)

print(f"Testing _query_abc_products")
print(f"Date range: {start_date} to {end_date}")

try:
    result = _query_abc_products(start_date, end_date)
    print(f"Success! Returned {len(result)} rows")
    if not result.empty:
        print(f"Columns: {result.columns.tolist()}")
        print(f"Sample data:\n{result.head()}")
    else:
        print("Warning: Empty result")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
