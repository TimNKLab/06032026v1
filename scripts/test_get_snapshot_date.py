import sys
from pathlib import Path
from datetime import date

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.inventory_metrics import _get_snapshot_date

# Test with a date that should exist (May 26, 2026 - recent snapshot)
test_date = date(2026, 5, 26)

print(f"Testing _get_snapshot_date with date: {test_date}")
try:
    result = _get_snapshot_date(test_date)
    print(f"Result: {result}")
    if result:
        print(f"Success! Function returned date: {result}")
    else:
        print("Warning: Function returned None (snapshot may not exist)")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*50 + "\n")

# Test with a date that should not exist (future date)
future_date = date(2026, 12, 31)

print(f"Testing _get_snapshot_date with future date: {future_date}")
try:
    result = _get_snapshot_date(future_date)
    print(f"Result: {result}")
    if result is None:
        print("Success! Function correctly returned None for non-existent date")
    else:
        print(f"Warning: Function returned {result} for future date")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
