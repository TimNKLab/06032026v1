import sys
from pathlib import Path
from datetime import date
import time

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.inventory_metrics import (
    _get_snapshot_date,
    _query_stock_levels,
    query_inventory_summary,
    _query_abc_products,
)

print("=" * 60)
print("Benchmarking Migrated Inventory Queries (DuckDB)")
print("=" * 60)

# Test dates
snapshot_date = date(2026, 5, 26)
start_date = date(2026, 5, 20)
end_date = date(2026, 5, 26)

# Benchmark _get_snapshot_date
print("\n1. Benchmarking _get_snapshot_date")
try:
    start = time.time()
    result = _get_snapshot_date(snapshot_date)
    elapsed = time.time() - start
    print(f"   Result: {result}")
    print(f"   Time: {elapsed:.3f}s")
except Exception as e:
    print(f"   Error: {e}")

# Benchmark _query_stock_levels
print("\n2. Benchmarking _query_stock_levels")
try:
    start = time.time()
    result = _query_stock_levels(snapshot_date, start_date, end_date)
    elapsed = time.time() - start
    print(f"   Rows: {len(result)}")
    print(f"   Time: {elapsed:.3f}s")
    if not result.empty:
        print(f"   Columns: {result.columns.tolist()}")
except Exception as e:
    print(f"   Error: {e}")

# Benchmark query_inventory_summary
print("\n3. Benchmarking query_inventory_summary")
try:
    start = time.time()
    result = query_inventory_summary(snapshot_date, lookback_days=7)
    elapsed = time.time() - start
    print(f"   Result: {result}")
    print(f"   Time: {elapsed:.3f}s")
except Exception as e:
    print(f"   Error: {e}")

# Benchmark _query_abc_products
print("\n4. Benchmarking _query_abc_products")
try:
    start = time.time()
    result = _query_abc_products(start_date, end_date)
    elapsed = time.time() - start
    print(f"   Rows: {len(result)}")
    print(f"   Time: {elapsed:.3f}s")
    if not result.empty:
        print(f"   Columns: {result.columns.tolist()}")
except Exception as e:
    print(f"   Error: {e}")

print("\n" + "=" * 60)
print("Benchmark Complete")
print("=" * 60)
