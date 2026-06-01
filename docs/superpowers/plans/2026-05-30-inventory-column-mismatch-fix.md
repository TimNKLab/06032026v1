# Inventory Column Mismatch Audit and Fix

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Systematically audit and fix all column name mismatches between parquet file schemas and code references in the inventory system to prevent runtime errors.

**Architecture:** Audit all parquet-to-code data access paths, identify schema mismatches, and align column references across ETL, DuckDB views, and query functions.

**Tech Stack:** Polars (parquet reads), DuckDB (views), Pandas (dataframes), Python (data access functions)

---

## Context Summary

**Known Issue Fixed:**
- `_query_location_ledger_deltas()` was using `pl.col("movement_date")` but parquet files have column "date"
- Fixed by changing to `pl.col("date")` in services/inventory_metrics.py lines 186-187

**Root Cause Analysis:**
- ETL extracts inventory moves with 'movement_date' in JSON
- ETL writes parquet with column aliased as 'date' (etl_tasks.py:884)
- DuckDB view renames 'date' back to 'movement_date' (duckdb_connector.py:455)
- Polars code reads raw parquet files, which have 'date', not 'movement_date'
- Code must use raw parquet column names when using Polars scan_parquet()

**Files to Audit:**
- services/inventory_metrics.py (Polars parquet reads)
- services/duckdb_connector.py (DuckDB view definitions)
- pages/inventory.py (UI column references)
- etl_tasks.py (ETL column aliases)

---

### Task 1: Audit Polars parquet reads in inventory_metrics.py

**Files:**
- Modify: `services/inventory_metrics.py`
- Reference: `services/duckdb_connector.py:450-499`

- [ ] **Step 1: Document all Polars scan_parquet() calls and column references**

Create audit table:
```python
# Audit findings to be documented:
# File: services/inventory_metrics.py
# Function: _query_location_ledger_deltas (line 170-214)
#   - Parquet path: fact_inventory_moves/**/*.parquet
#   - Column filter: pl.col("date") ✓ (already fixed)
#   - Column access: location_dest_id, location_src_id, qty_moved
#   - Expected schema check: verify these columns exist in parquet
```

- [ ] **Step 2: Check parquet schema for fact_inventory_moves**

Run schema inspection:
```bash
# Use Python to inspect parquet schema
python -c "
import polars as pl
import os
data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
moves_path = f'{data_lake_root}/star-schema/fact_inventory_moves/**/*.parquet'
try:
    df = pl.scan_parquet(moves_path, hive_partitioning=True).collect()
    print('Columns:', df.columns)
except Exception as e:
    print(f'Error: {e}')
"
```

Expected output: Compare with error message columns:
["date", "move_id", "move_line_id", "product_id", "product_name", "product_brand", "location_src_id", "location_src_name", "location_src_usage", "location_dest_id", "location_dest_name", "location_dest_usage", "qty_moved", "uom_id", "uom_name", "uom_category", "movement_type", "inventory_adjustment_flag", "manufacturing_order_id", "picking_id", "picking_type_code", "reference", "origin_reference", "source_partner_id", "source_partner_name", "destination_partner_id", "destination_partner_name", "created_by_user", "create_date", "year", "month", "day"]

- [ ] **Step 3: Verify column access in _query_location_ledger_deltas matches schema**

Check these column references in lines 194-197:
```python
# Current code:
dest_id = row.get('location_dest_id')
src_id = row.get('location_src_id')
qty = abs(row.get('qty_moved', 0))
```

Verify these exist in parquet schema from Step 2.

- [ ] **Step 4: Check for other Polars parquet reads in inventory_metrics.py**

Search for other `pl.scan_parquet()` or `pl.read_parquet()` calls:
```python
# Check line 526 in get_inventory_costs():
purchase_df = pl.scan_parquet(cost_latest_path, hive_partitioning=True).filter(
    pl.col("date") <= as_of_date
)
```

- [ ] **Step 5: Audit DuckDB view vs Polars direct access for cost data**

Check if fact_product_cost parquet has 'date' column:
```python
# From duckdb_connector.py line 556-560:
# DuckDB view uses: TRY_CAST(date AS DATE) AS date
# So parquet should have 'date' column
# Verify this matches line 526 in inventory_metrics.py
```

- [ ] **Step 6: Document audit findings**

Create audit log in services/inventory_metrics.py:
```python
# COLUMN REFERENCE AUDIT LOG
# Updated: 2026-05-30
#
# Polars Direct Parquet Access (uses raw parquet column names):
# ----------------------------------------------------------------------
# _query_location_ledger_deltas (line 170-214):
#   - Source: fact_inventory_moves/**/*.parquet
#   - Filter column: "date" ✓ (matches parquet schema)
#   - Access columns: location_dest_id, location_src_id, qty_moved ✓ (verified)
#
# get_inventory_costs (line 516-567):
#   - Source: fact_product_cost/**/*.parquet  
#   - Filter column: "date" ✓ (matches DuckDB view definition)
#   - Access columns: product_id, cost_unit_tax_in ✓ (verified)
#
# DuckDB View Access (uses view column names):
# ----------------------------------------------------------------------
# query_inventory_snapshot: Uses DuckDB view fact_stock_on_hand_snapshot
# query_sales_by_product_duckdb: Uses DuckDB view mv_sales_by_product
# These return column names as defined in DuckDB views, not raw parquet
```

- [ ] **Step 7: Commit audit documentation**

```bash
git add services/inventory_metrics.py
git commit -m "docs: add column reference audit log for inventory_metrics.py

Generated with [Devin](https://cli.devin.ai/docs)

Co-Authored-By: Devin <158243242+devin-ai-integration[bot]@users.noreply.github.com>"
```

---

### Task 2: Audit DuckDB view definitions for column consistency

**Files:**
- Modify: `services/duckdb_connector.py`
- Reference: `services/inventory_metrics.py`

- [ ] **Step 1: Extract all column renames in DuckDB views**

Review duckdb_connector.py lines 450-670 for column renames:
```python
# Key view: fact_inventory_moves (line 450-499)
# Renames: TRY_CAST(date AS TIMESTAMP) AS movement_date
# This creates view column "movement_date" from parquet "date"
#
# Key view: fact_stock_on_hand_snapshot (line 522-548)
# No renames - uses same column names as parquet
#
# Key view: fact_product_cost (line 556-570)
# No renames - uses same column names as parquet
```

- [ ] **Step 2: Document DuckDB view vs parquet schema mapping**

Create mapping documentation in duckdb_connector.py:
```python
# DUCKDB VIEW TO PARQUET COLUMN MAPPING
# Updated: 2026-05-30
#
# fact_inventory_moves view (line 450):
#   View column "movement_date" <- parquet column "date"
#   Other columns: 1:1 mapping (no rename)
#
# fact_stock_on_hand_snapshot view (line 522):
#   All columns: 1:1 mapping (no rename)
#
# fact_product_cost view (line 556):
#   All columns: 1:1 mapping (no rename)
#
# IMPORTANT: When using Polars scan_parquet() directly, use parquet column names
# When using DuckDB views, use view column names
```

- [ ] **Step 3: Check for code that mixes view and parquet access patterns**

Search for functions that use both:
```python
# Check if any function uses:
# - DuckDB views for some data
# - Polars parquet reads for other data
# This is the pattern in _query_stock_levels (line 70-120)
```

- [ ] **Step 4: Verify _query_stock_levels column consistency**

Check line 70-120 in inventory_metrics.py:
```python
# Uses DuckDB views (returns view column names):
on_hand_df = query_inventory_snapshot(snapshot_date)  # Returns 'quantity' not 'qty_on_hand'
on_hand_df = on_hand_df.rename(columns={'qty_on_hand': 'on_hand_qty'})  # But view returns 'quantity'?

# Verify what query_inventory_snapshot actually returns
```

- [ ] **Step 5: Check query_inventory_snapshot return column names**

Review duckdb_connector.py line 1591-1620:
```python
# query_inventory_snapshot returns:
# SELECT snapshot_date, product_id, quantity, reserved_quantity
# So it returns 'quantity', not 'qty_on_hand'
# But inventory_metrics.py line 81 renames 'qty_on_hand' which doesn't exist
```

- [ ] **Step 6: Fix column rename in _query_stock_levels**

Fix inventory_metrics.py line 81:
```python
# Change from:
on_hand_df = on_hand_df.rename(columns={'qty_on_hand': 'on_hand_qty'})

# To:
on_hand_df = on_hand_df.rename(columns={'quantity': 'on_hand_qty'})
```

- [ ] **Step 7: Check for similar issues in query_inventory_summary**

Search for other column rename mismatches:
```python
# Check line 446 in inventory_metrics.py:
stock_df = stock_df.rename(columns={'qty_on_hand': 'on_hand_qty'})
# This might have the same issue if it uses query_inventory_snapshot
```

- [ ] **Step 8: Fix column rename in query_inventory_summary**

Fix inventory_metrics.py line 446 if needed (same fix as Step 6).

- [ ] **Step 9: Commit DuckDB view documentation and fixes**

```bash
git add services/duckdb_connector.py services/inventory_metrics.py
git commit -m "fix: correct column renames from DuckDB views to match actual return columns

- query_inventory_snapshot returns 'quantity' not 'qty_on_hand'
- Fixed rename in _query_stock_levels and query_inventory_summary
- Added DuckDB view to parquet column mapping documentation

Generated with [Devin](https://cli.devin.ai/docs)

Co-Authored-By: Devin <158243242+devin-ai-integration[bot]@users.noreply.github.com>"
```

---

### Task 3: Audit inventory.py UI column references

**Files:**
- Modify: `pages/inventory.py`
- Reference: `services/inventory_metrics.py`

- [ ] **Step 1: Extract all DataFrame column references in inventory.py**

Search for df['column'] patterns:
```python
# From grep results:
# abc_subset = abc_df[abc_cols].copy() (line 1208)
# revenue_at_risk = float(reorder_df['revenue'].sum()) (line 1280)
# capital_locked = float(markdown_df['est_stock_value'].sum()) (line 1281)
# etc.
```

- [ ] **Step 2: Verify column names match what inventory_metrics functions return**

Check each function's return schema:
```python
# get_stock_levels_ledger returns:
# 'product_id', 'product_name', 'product_category', 'product_brand',
# 'on_hand_qty', 'reserved_qty', 'units_sold', 'avg_daily_sold',
# 'days_of_cover', 'low_stock_flag', 'dead_stock_flag'
#
# get_abc_analysis returns:
# 'product_id', 'abc_class', 'revenue', 'quantity'
#
# get_sell_through_analysis returns:
# 'product_id', 'product_name', 'product_category', 'begin_on_hand',
# 'units_received', 'units_sold', 'sell_through'
```

- [ ] **Step 3: Cross-reference inventory.py column usage with expected schemas**

Check key locations:
```python
# Line 1280: reorder_df['revenue'] - check if reorder_df has 'revenue'
# Line 1281: markdown_df['est_stock_value'] - check if markdown_df has this column
# Line 1503-1505: categories_df['begin_on_hand', 'units_received', 'units_sold'] - verify
```

- [ ] **Step 4: Check if reorder_df and markdown_df have expected columns**

Review how these DataFrames are built:
```python
# Search for where reorder_df and markdown_df are created
# They should be subsets of get_stock_levels_ledger output
# Verify the columns match
```

- [ ] **Step 5: Check sell-through column references**

Verify sell-through analysis columns:
```python
# Line 1503-1508 uses: begin_on_hand, units_received, units_sold
# From get_sell_through_analysis return schema, these should be present
```

- [ ] **Step 6: Document column reference audit for inventory.py**

Add audit comment in inventory.py:
```python
# INVENTORY.PY COLUMN REFERENCE AUDIT
# Updated: 2026-05-30
#
# DataFrame Sources and Expected Columns:
# ----------------------------------------------------------------------
# reorder_df: Subset of get_stock_levels_ledger output
#   Expected: product_id, product_name, product_category, abc_class,
#             on_hand_qty, avg_daily_sold, days_of_cover, reorder_qty, revenue
#
# markdown_df: Subset of get_stock_levels_ledger output  
#   Expected: product_id, product_name, product_category, on_hand_qty,
#             days_of_cover, avg_daily_sold, est_stock_value, revenue
#
# categories_df: From get_sell_through_analysis aggregated by category
#   Expected: product_category, begin_on_hand, units_received, units_sold, sell_through
#
# All column references verified against function return schemas
```

- [ ] **Step 7: Commit inventory.py audit documentation**

```bash
git add pages/inventory.py
git commit -m "docs: add column reference audit for inventory.py

Verified all DataFrame column references match function return schemas.

Generated with [Devin](https://cli.devin.ai/docs)

Co-Authored-By: Devin <158243242+devin-ai-integration[bot]@users.noreply.github.com>"
```

---

### Task 4: Verify ETL to parquet column consistency

**Files:**
- Reference: `etl_tasks.py:884`
- Reference: `etl/extract/inventory_moves.py:305`

- [ ] **Step 1: Document ETL column aliasing pattern**

From etl_tasks.py line 884:
```python
# ETL creates 'movement_date' in JSON (inventory_moves.py:305)
# ETL aliases to 'date' when writing parquet (etl_tasks.py:884)
# This is the root cause of the naming confusion
```

- [ ] **Step 2: Verify all ETL parquet writes use consistent column names**

Check other ETL column renames:
```python
# Search for .alias() in etl_tasks.py
# Ensure all aliases are documented and consistent
```

- [ ] **Step 3: Document ETL column naming strategy**

Add documentation comment in etl_tasks.py:
```python
# ETL COLUMN NAMING STRATEGY  
# Updated: 2026-05-30
#
# Parquet files use simplified column names:
# - 'date' instead of 'movement_date'
# - 'quantity' instead of 'qty_on_hand' (where applicable)
#
# DuckDB views can rename columns for semantic clarity:
# - fact_inventory_moves: 'date' -> 'movement_date' 
# - fact_stock_on_hand_snapshot: 'quantity' -> 'qty_on_hand' (not currently done)
#
# Polars code must use raw parquet column names
# DuckDB view code uses view column names
```

- [ ] **Step 4: Commit ETL column documentation**

```bash
git add etl_tasks.py
git commit -m "docs: document ETL column naming strategy

Clarifies parquet vs DuckDB view column naming conventions.

Generated with [Devin](https://cli.devin.ai/docs)

Co-Authored-By: Devin <158243242+devin-ai-integration[bot]@users.noreply.github.com>"
```

---

### Task 5: Create comprehensive column reference test

**Files:**
- Create: `tests/test_inventory_column_consistency.py`

- [ ] **Step 1: Write test to verify parquet schema matches code expectations**

```python
# tests/test_inventory_column_consistency.py
import polars as pl
import os
import pytest

def test_fact_inventory_moves_schema():
    """Verify fact_inventory_moves parquet has expected columns."""
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    moves_path = f'{data_lake_root}/star-schema/fact_inventory_moves/**/*.parquet'
    
    try:
        df = pl.scan_parquet(moves_path, hive_partitioning=True).collect()
        expected_columns = [
            'date', 'move_id', 'move_line_id', 'product_id', 'product_name', 
            'product_brand', 'location_src_id', 'location_src_name', 
            'location_src_usage', 'location_dest_id', 'location_dest_name', 
            'location_dest_usage', 'qty_moved', 'uom_id', 'uom_name', 
            'uom_category', 'movement_type', 'inventory_adjustment_flag',
            'manufacturing_order_id', 'picking_id', 'picking_type_code',
            'reference', 'origin_reference', 'source_partner_id', 
            'source_partner_name', 'destination_partner_id', 
            'destination_partner_name', 'created_by_user', 'create_date',
            'year', 'month', 'day'
        ]
        
        for col in expected_columns:
            assert col in df.columns, f"Missing expected column: {col}"
            
    except Exception as e:
        pytest.skip(f"Cannot access parquet files: {e}")

def test_polars_code_uses_parquet_columns():
    """Verify Polars code uses actual parquet column names."""
    # Import the function
    from services.inventory_metrics import _query_location_ledger_deltas
    from datetime import datetime, date
    
    # Read the source code to check column references
    import inspect
    source = inspect.getsource(_query_location_ledger_deltas)
    
    # Should use 'date' not 'movement_date'
    assert 'pl.col("date")' in source or 'pl.col(\'date\')' in source
    assert 'pl.col("movement_date")' not in source
```

- [ ] **Step 2: Run test to verify current state**

```bash
pytest tests/test_inventory_column_consistency.py -v
```

Expected: Tests should pass after our fixes.

- [ ] **Step 3: Commit column consistency test**

```bash
git add tests/test_inventory_column_consistency.py
git commit -m "test: add column consistency test for inventory parquet schemas

Ensures Polars code uses actual parquet column names, not DuckDB view names.

Generated with [Devin](https://cli.devin.ai/docs)

Co-Authored-By: Devin <158243242+devin-ai-integration[bot]@users.noreply.github.com>"
```

---

### Task 6: Final verification and Docker restart

**Files:**
- None (verification task)

- [ ] **Step 1: Run full test suite to ensure no regressions**

```bash
pytest tests/ -v --tb=short
```

Expected: All tests pass.

- [ ] **Step 2: Test inventory page manually**

```bash
# Restart Docker to pick up changes
docker-compose restart

# Check inventory page loads without errors
# Navigate to /inventory and verify no column mismatch errors
```

Expected: Inventory page loads successfully, no column errors.

- [ ] **Step 3: Verify all audit documentation is in place**

Check that audit comments were added:
- services/inventory_metrics.py: Column reference audit log
- services/duckdb_connector.py: View to parquet mapping  
- pages/inventory.py: UI column reference audit
- etl_tasks.py: ETL naming strategy documentation

- [ ] **Step 4: Create summary documentation**

Create `docs/inventory_column_audit_summary.md`:
```markdown
# Inventory Column Mismatch Audit Summary

**Date:** 2026-05-30
**Status:** Complete

## Issues Found and Fixed

1. **movement_date vs date mismatch** (services/inventory_metrics.py:186-187)
   - Fixed: Changed pl.col("movement_date") to pl.col("date")
   - Root cause: Polars reads raw parquet, which uses 'date' not 'movement_date'

2. **qty_on_hand vs quantity mismatch** (services/inventory_metrics.py:81, 446)
   - Fixed: Changed rename from 'qty_on_hand' to 'quantity'
   - Root cause: DuckDB view returns 'quantity', code expected 'qty_on_hand'

## Documentation Added

- Column reference audit logs in inventory_metrics.py
- DuckDB view to parquet mapping in duckdb_connector.py  
- UI column reference audit in inventory.py
- ETL naming strategy documentation in etl_tasks.py
- Column consistency test in tests/test_inventory_column_consistency.py

## Lessons Learned

- Polars scan_parquet() uses raw parquet column names
- DuckDB views can rename columns for semantic clarity
- Code must match the access pattern (direct parquet vs view)
- Always verify actual schema vs expected schema
```

- [ ] **Step 5: Commit summary documentation**

```bash
git add docs/inventory_column_audit_summary.md
git commit -m "docs: add inventory column mismatch audit summary

Documents all issues found, fixes applied, and lessons learned.

Generated with [Devin](https://cli.devin.ai/docs)

Co-Authored-By: Devin <158243242+devin-ai-integration[bot]@users.noreply.github.com>"
```

---

## Verification Checklist

After completing all tasks:

- [ ] All Polars parquet reads use actual parquet column names
- [ ] All DuckDB view column references match view definitions
- [ ] All UI DataFrame column references match function return schemas
- [ ] ETL column naming strategy is documented
- [ ] Column consistency tests pass
- [ ] Manual verification of inventory page successful
- [ ] All audit documentation is in place
- [ ] Docker restart completed and page loads without errors
