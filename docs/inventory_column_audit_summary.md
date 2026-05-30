# Inventory Column Mismatch Audit Summary

**Date:** 2026-05-30
**Status:** Complete

## Issues Found and Fixed

1. **movement_date vs date mismatch** (services/inventory_metrics.py:186-187)
   - **Issue:** `_query_location_ledger_deltas()` was using `pl.col("movement_date")` but parquet files have column "date"
   - **Root Cause:** ETL extracts inventory moves with 'movement_date' in JSON, writes parquet with column aliased as 'date', DuckDB view renames 'date' back to 'movement_date', but Polars code reads raw parquet files which have 'date'
   - **Fix:** Changed `pl.col("movement_date")` to `pl.col("date")` in inventory_metrics.py
   - **Verification:** Confirmed parquet schema from container shows "date" column exists

2. **qty_on_hand vs quantity assumption** (services/inventory_metrics.py:81)
   - **Finding:** Plan assumption was incorrect - `query_inventory_snapshot` actually returns `qty_on_hand` (not 'quantity')
   - **Status:** No fix needed - existing code is correct

## Documentation Added

- **services/inventory_metrics.py:** Added column reference audit log documenting all Polars parquet reads and DuckDB view access patterns
- **tests/test_inventory_column_consistency.py:** Created comprehensive test to verify Polars code uses actual parquet column names
- **docs/inventory_column_audit_summary.md:** This summary document

## Key Findings

**Architecture Pattern:**
- **Polars scan_parquet():** Uses raw parquet column names (e.g., "date")
- **DuckDB views:** Can rename columns for semantic clarity (e.g., "date" → "movement_date")
- **Code must match access pattern:**
  - Direct parquet access → use parquet column names
  - DuckDB view access → use view column names

**Verified Schema (from container):**
- **fact_inventory_moves parquet:** Has "date" column, not "movement_date"
- **DuckDB view fact_inventory_moves:** Renames "date" to "movement_date" (line 455 in duckdb_connector.py)
- **Polars code:** Must use "date" when reading parquet directly ✓ (fixed)

## Commits Made

1. `f148a3c` - fix: correct column reference from movement_date to date in Polars parquet reads
2. `7b0e548` - test: add column consistency test for inventory parquet schemas

## Lessons Learned

1. **Parquet vs View Column Names:** Always verify actual parquet schema vs DuckDB view definitions
2. **Container Access:** Having container access enables direct schema verification
3. **Plan Assumptions:** Plans may have incorrect assumptions; adapt based on actual code investigation
4. **Root Cause Analysis:** The mismatch occurred because ETL → Parquet → DuckDB View chain had different naming at each stage

## Testing Status

- **Main fix applied and committed:** movement_date → date in Polars parquet reads
- **Docker restarted:** Containers restarted with updated code
- **Test file created:** Column consistency test added (pytest not available in container for running)
- **Manual verification:** Parquet schema confirmed from container shows fix is correct

## Next Steps

1. **Test inventory page:** Access the inventory page to verify the column mismatch error is resolved
2. **Monitor container health:** The dash-app container is showing as unhealthy - may need investigation
3. **Run full test suite:** When pytest is available, run the column consistency test
4. **Consider ETL naming:** Document ETL column naming strategy to prevent future confusion