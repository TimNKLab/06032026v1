import sys
from pathlib import Path
from datetime import date, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.inventory_metrics import _query_stock_levels

# Test with recent date range
snapshot_date = date(2026, 5, 26)
lookback_start = date(2026, 5, 20)
lookback_end = date(2026, 5, 26)

print(f"Testing _query_stock_levels")
print(f"Snapshot date: {snapshot_date}")
print(f"Lookback range: {lookback_start} to {lookback_end}")

try:
    result = _query_stock_levels(snapshot_date, lookback_start, lookback_end)
    print(f"Success! Returned {len(result)} rows")
    print(f"Columns: {result.columns.tolist()}")
    if not result.empty:
        print(f"Sample data:\n{result.head()}")
        print(f"\nSummary stats:")
        print(f"Total on_hand_qty: {result['on_hand_qty'].sum()}")
        print(f"Total units_sold: {result['units_sold'].sum()}")
    else:
        print("Warning: Empty result")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
