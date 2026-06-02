# Inventory Metrics Migration Rollback Plan

**Workstream:** NK_20260512_mv_refresh_dashboard_integration  
**Migration Date:** 2026-05-29  
**Target:** Migrate inventory metrics from SQLite MVs to DuckDB parquet reads

---

## Summary

This rollback plan documents how to revert the inventory metrics migration from DuckDB parquet reads back to SQLite Materialized Views (MVs). The migration removed SQLiteManager dependencies from `services/inventory_metrics.py` and replaced them with DuckDB parquet queries.

---

## Changes Made

### 1. Migrated Functions

| Function | Previous Implementation | New Implementation |
|----------|------------------------|-------------------|
| `_get_snapshot_date` | SQLite MV (`mv_inventory_daily`) | DuckDB parquet (`fact_stock_on_hand_snapshot`) |
| `_query_stock_levels` | SQLite MVs (`mv_inventory_daily`, `mv_sales_by_product`) | DuckDB parquet (`fact_stock_on_hand_snapshot`, `agg_sales_daily_by_product`) |
| `query_inventory_summary` | SQLite MVs (`mv_inventory_daily`, `mv_sales_by_product`) | DuckDB parquet (`fact_stock_on_hand_snapshot`, `agg_sales_daily_by_product`) |
| `_query_abc_products` | SQLite MV (`mv_sales_by_product`) | DuckDB parquet (`agg_sales_daily_by_product`) |

### 2. Removed Functions

| Function | Reason for Removal |
|----------|-------------------|
| `_query_sell_through` | Deferred due to complexity (requires fact_inventory_moves data) |
| `get_sell_through_analysis` | Depends on `_query_sell_through` |

**Note:** A stub function was added for `get_sell_through_analysis` to prevent import errors. It returns empty data.

### 3. Files Modified

- `services/inventory_metrics.py` - Migrated 4 functions, removed 2 functions, added 1 stub
- `services/duckdb_connector.py` - Added `query_inventory_snapshot` and `query_sales_by_product_duckdb` functions

---

## Rollback Procedure

### Step 1: Git Revert

```bash
# Revert to commit before migration
git log --oneline --grep="Task 6: Remove SQLiteManager"
# Identify the commit hash before the migration
git revert <commit-hash> --no-commit
# OR
git checkout <commit-before-migration> -- services/inventory_metrics.py services/duckdb_connector.py
```

### Step 2: Restore SQLiteManager Imports

The reverted file should automatically restore:
- `from services.sqlite_manager import SQLiteManager` imports
- SQLite MV query patterns
- Original function implementations

### Step 3: Remove DuckDB Query Functions (Optional)

If you want to remove the DuckDB helper functions added during migration:

```bash
# Edit services/duckdb_connector.py
# Remove or comment out:
# - query_inventory_snapshot (lines ~1795-1815)
# - query_sales_by_product_duckdb (lines ~1817-1849)
```

### Step 4: Restore Sell-Through Functions

The reverted file should automatically restore:
- `_query_sell_through` function
- `get_sell_through_analysis` function

Remove the stub function that was added:
```python
# Remove this stub from services/inventory_metrics.py:
def get_sell_through_analysis(start_date: date, end_date: date) -> Dict[str, object]:
    """Stub function - sell-through analysis deferred due to complexity..."""
    # ... stub implementation
```

### Step 5: Verify SQLite MVs Exist

Ensure SQLite MVs are populated before rollback:

```python
# In Python shell or script
from services.sqlite_manager import SQLiteManager
manager = SQLiteManager()
with manager.reader_conn() as conn:
    # Check mv_inventory_daily
    result = conn.execute("SELECT COUNT(*) FROM mv_inventory_daily").fetchone()
    print(f"mv_inventory_daily rows: {result[0]}")
    
    # Check mv_sales_by_product
    result = conn.execute("SELECT COUNT(*) FROM mv_sales_by_product").fetchone()
    print(f"mv_sales_by_product rows: {result[0]}")
```

If MVs are empty, refresh them:
```python
from services.sqlite_manager import SQLiteManager
manager = SQLiteManager()
manager.refresh_materialized_views()
```

### Step 6: Test Rollback

```bash
# Test compilation
python -m py_compile services/inventory_metrics.py

# Test imports
python -c "from services.inventory_metrics import get_abc_analysis, query_inventory_summary, get_stock_levels_ledger"

# Run existing tests
pytest tests/test_inventory_cross_domain_joins.py
```

### Step 7: Deploy Rollback

```bash
git add services/inventory_metrics.py services/duckdb_connector.py
git commit -m "Rollback: Revert inventory metrics to SQLite MVs

- Restored SQLiteManager imports
- Restored _get_snapshot_date, _query_stock_levels, query_inventory_summary to use SQLite MVs
- Restored _query_sell_through and get_sell_through_analysis functions
- Removed DuckDB parquet query functions (optional)
- Reason: <rollback reason>"

# Deploy to production
git push origin main
```

---

## Rollback Triggers

Consider rollback if:

1. **Performance Degradation**
   - DuckDB parquet queries are significantly slower than SQLite MVs (>2x slower)
   - Benchmark shows: DuckDB 16s+ vs SQLite <5s for same queries

2. **Data Inconsistency**
   - DuckDB parquet data doesn't match SQLite MV data
   - Missing or incorrect data in parquet files
   - Parquet partitioning issues causing data gaps

3. **Operational Issues**
   - DuckDB lock conflicts preventing queries
   - Memory exhaustion from DuckDB operations
   - Frequent connection errors to DuckDB

4. **User Impact**
   - Inventory page fails to load
   - Incorrect metrics displayed
   - Sell-through tab completely non-functional (not just deferred)

---

## Verification After Rollback

### 1. Check Page Loads

```bash
# Access inventory page in browser
# Verify all tabs load without errors:
# - Action Items
# - Stock Levels
# - Sell-through (should work after rollback)
# - ABC Analysis
```

### 2. Verify Data Correctness

```python
# Spot-check key metrics
from services.inventory_metrics import (
    get_abc_analysis,
    query_inventory_summary,
    get_stock_levels_ledger,
)
from datetime import date

# Test ABC analysis
result = get_abc_analysis(date(2026, 5, 20), date(2026, 5, 26))
assert result['total_revenue'] > 0, "ABC analysis returned zero revenue"

# Test inventory summary
result = query_inventory_summary(date(2026, 5, 26))
assert result['total_sku_count'] > 0, "Inventory summary returned zero SKUs"

# Test stock levels ledger
result = get_stock_levels_ledger(date(2026, 5, 26))
assert result['summary']['total_on_hand'] > 0, "Stock levels returned zero on-hand"
```

### 3. Performance Check

```python
import time
from services.inventory_metrics import query_inventory_summary
from datetime import date

start = time.time()
result = query_inventory_summary(date(2026, 5, 26))
elapsed = time.time() - start

print(f"Query time: {elapsed:.3f}s")
# Should be <5s for SQLite MVs
assert elapsed < 5.0, f"Query too slow: {elapsed:.3f}s"
```

---

## Known Limitations After Rollback

1. **Sell-Through Functionality**
   - Sell-through analysis will work after rollback
   - However, it was originally deferred due to complexity
   - May still have issues with fact_inventory_moves data

2. **SQLite MV Refresh**
   - Requires MV refresh pipeline to be operational
   - MVs must be populated with current data
   - Refresh may take time for large date ranges

3. **Memory Usage**
   - SQLite MVs use more memory than DuckDB parquet reads
   - May cause OOM issues during MV refresh in celery-worker

---

## Contact

For questions or issues with rollback:
- Check git history: `git log --oneline --all --grep="inventory"`
- Review migration plan: `.windsurf/plans/2026-05-29-remove-all-materialized-views-d0c61f.md`
- Check SSOT: `SSOT.md` (Section on inventory migration)
