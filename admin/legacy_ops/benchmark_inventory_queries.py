import time
import sys
from pathlib import Path
from datetime import date, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.inventory_metrics import query_inventory_summary, get_stock_levels_ledger

def benchmark_inventory_summary():
    """Benchmark query_inventory_summary with current SQLite MVs."""
    snapshot_date = date.today()
    
    start = time.time()
    result = query_inventory_summary(snapshot_date)
    elapsed = time.time() - start
    
    print(f"query_inventory_summary: {elapsed:.3f}s")
    print(f"Result keys: {result.keys()}")
    return elapsed

def benchmark_stock_levels():
    """Benchmark get_stock_levels_ledger with current SQLite MVs."""
    as_of_date = date.today()
    
    start = time.time()
    result = get_stock_levels_ledger(as_of_date)
    elapsed = time.time() - start
    
    print(f"get_stock_levels_ledger: {elapsed:.3f}s")
    print(f"Items count: {len(result.get('items', []))}")
    return elapsed

if __name__ == "__main__":
    print("=== Benchmarking Current Inventory Queries ===")
    t1 = benchmark_inventory_summary()
    t2 = benchmark_stock_levels()
    print(f"\nTotal time: {t1 + t2:.3f}s")
