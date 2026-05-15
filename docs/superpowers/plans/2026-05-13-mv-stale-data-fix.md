# MV Stale Data Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix materialized views not showing data newer than the initial startup load (Feb–Sept 2025 shows, May 2026 doesn't).

**Architecture:** Three compounding bugs cause stale data: (1) `_get_mv_refresh_info` has a broken SQL query that always triggers full refresh but `ensure_materialized_views` skips already-tracked MVs; (2) `mv_profit_daily` always does `CREATE OR REPLACE` (no incremental) but is never re-run after startup; (3) `@lru_cache` on query functions returns stale results even after MV data changes. Fix: correct the refresh info query, add incremental logic to `mv_profit_daily`, add `force_reload` path that bypasses tracking, and ensure `lru_cache` is cleared when MVs are refreshed.

**Tech Stack:** Python 3.9, DuckDB 1.2.0, Polars, Flask-Caching (Redis), `functools.lru_cache`

---

## Root Cause Analysis

### Bug 1: `_get_mv_refresh_info` broken SQL

```python
# services/duckdb_connector.py ~line 152
result = conn.execute(f"""
    SELECT COUNT(*), MAX(date) 
    FROM information_schema.tables   # <-- returns TABLE COUNT, not row count
    WHERE table_name = '{mv_name}'
""").fetchone()

if result[0] == 0:   # result[0] is 0 or 1 (table exists or not)
    return (True, None, 0)
```

`information_schema.tables` returns one row per table. `COUNT(*)` is always 0 (no table) or 1 (table exists). `MAX(date)` on `information_schema.tables` is always NULL. So `max_mv_date` is always NULL → incremental path never runs → always full refresh. But `ensure_materialized_views` skips MVs already in `_materialized_views` set, so after startup the full refresh never runs again.

### Bug 2: `mv_profit_daily` has no incremental path

`mv_profit_daily` always does `CREATE OR REPLACE TABLE` — no `INSERT` incremental path. Even if called again, it rebuilds from all parquet. But it's never called again after startup because of Bug 1 + tracking set.

### Bug 3: `@lru_cache` on query functions

`query_profit_trends` and `query_profit_summary` have `@lru_cache(maxsize=32)`. Even if MV data changes, cached results are returned. `clear_profit_caches()` exists but is only called in `bulk_poll` — not after `refresh_materialized_views` task completes.

### Bug 4: `ensure_materialized_views` tracking set blocks re-runs

After startup loads MVs into `_materialized_views`, subsequent calls to `ensure_materialized_views` skip them. The `force_reload=True` fix was added in a previous session but `_load_materialized_views` still uses the broken `_get_mv_refresh_info` for `mv_sales_daily`.

---

## File Map

| File | Change |
|------|--------|
| `services/duckdb_connector.py` | Fix `_get_mv_refresh_info`, add incremental to `mv_profit_daily`, fix `mv_sales_daily` incremental filter |
| `services/profit_metrics.py` | Clear `lru_cache` after MV refresh |
| `etl_tasks.py` | Call `clear_profit_caches()` + `clear_sales_caches()` after MV refresh completes |
| `tests/test_mv_refresh.py` | New: verify MV contains recent dates after refresh |

---

## Task 1: Fix `_get_mv_refresh_info` SQL

**Files:**
- Modify: `services/duckdb_connector.py` (~line 148–172)

The fix: query the MV table directly for max date, not `information_schema.tables`.

- [ ] **Step 1: Understand current broken code**

Read lines 148–172 of `services/duckdb_connector.py`. The bug is:
```python
result = conn.execute(f"""
    SELECT COUNT(*), MAX(date) 
    FROM information_schema.tables 
    WHERE table_name = '{mv_name}'
""").fetchone()
```
`information_schema.tables` has no `date` column. `MAX(date)` is NULL. `COUNT(*)` is 0 or 1 (table exists check). This means `max_mv_date` is always NULL, so incremental path never runs.

- [ ] **Step 2: Replace `_get_mv_refresh_info` with correct implementation**

In `services/duckdb_connector.py`, replace the entire `_get_mv_refresh_info` method:

```python
def _get_mv_refresh_info(self, conn: duckdb.DuckDBPyConnection, mv_name: str, parquet_path: str) -> tuple:
    """Get refresh info for incremental MV loading.
    Returns: (needs_full_refresh, max_date_in_mv, new_files_count)
    """
    try:
        # Check if MV table exists
        result = conn.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = '{mv_name}' AND table_type = 'BASE TABLE'
        """).fetchone()

        if result[0] == 0:
            return (True, None, 0)  # MV doesn't exist, needs full load

        # Get max date from the actual MV table
        max_mv_date = conn.execute(f"SELECT MAX(date) FROM {mv_name}").fetchone()[0]

        if max_mv_date is None:
            return (True, None, 0)  # MV exists but empty, needs full load

        return (False, max_mv_date, 0)
    except Exception as e:
        print(f"[duckdb] _get_mv_refresh_info error for {mv_name}: {e}")
        return (True, None, 0)  # Error, do full refresh
```

- [ ] **Step 3: Commit**

```bash
git add services/duckdb_connector.py
git commit -m "fix: correct _get_mv_refresh_info to query MV table directly for max date"
```

---

## Task 2: Add Incremental Refresh to `mv_profit_daily`

**Files:**
- Modify: `services/duckdb_connector.py` (~line 310–329)

`mv_profit_daily` always does `CREATE OR REPLACE` — no incremental path. Add same pattern as `mv_sales_daily`.

- [ ] **Step 1: Replace `mv_profit_daily` load block**

In `services/duckdb_connector.py`, replace the `mv_profit_daily` block (lines ~311–329):

```python
# Materialized view: Daily profit aggregates
if "mv_profit_daily" in views:
    agg_profit_path = f"{data_lake}/star-schema/agg_profit_daily"
    needs_full, max_date, _ = self._get_mv_refresh_info(conn, "mv_profit_daily", agg_profit_path)

    if needs_full or max_date is None:
        conn.execute(f"""
            CREATE OR REPLACE TABLE mv_profit_daily AS
            SELECT
                COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
                    CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
                    CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
                    CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
                )) AS date,
                COALESCE(TRY_CAST(revenue_tax_in AS DOUBLE), 0) AS revenue_tax_in,
                COALESCE(TRY_CAST(cogs_tax_in AS DOUBLE), 0) AS cogs_tax_in,
                COALESCE(TRY_CAST(gross_profit AS DOUBLE), 0) AS gross_profit,
                COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                COALESCE(TRY_CAST(transactions AS BIGINT), 0) AS transactions,
                COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
            FROM read_parquet('{agg_profit_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
            WHERE revenue_tax_in IS NOT NULL
        """)
        refresh_type = 'full'
    else:
        # Incremental: load only dates > max_date in MV
        conn.execute(f"""
            INSERT INTO mv_profit_daily
            SELECT
                COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
                    CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
                    CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
                    CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
                )) AS date,
                COALESCE(TRY_CAST(revenue_tax_in AS DOUBLE), 0) AS revenue_tax_in,
                COALESCE(TRY_CAST(cogs_tax_in AS DOUBLE), 0) AS cogs_tax_in,
                COALESCE(TRY_CAST(gross_profit AS DOUBLE), 0) AS gross_profit,
                COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                COALESCE(TRY_CAST(transactions AS BIGINT), 0) AS transactions,
                COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
            FROM read_parquet('{agg_profit_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
            WHERE revenue_tax_in IS NOT NULL
              AND COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
                    CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
                    CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
                    CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
                )) > '{max_date}'
        """)
        refresh_type = 'incremental'

    # Update metadata
    conn.execute(f"""
        INSERT OR REPLACE INTO mv_refresh_metadata 
        SELECT 'mv_profit_daily', NOW(), MAX(date), COUNT(*), '{refresh_type}'
        FROM mv_profit_daily
    """)
    print(f"[duckdb] mv_profit_daily refreshed ({refresh_type})")
```

- [ ] **Step 2: Commit**

```bash
git add services/duckdb_connector.py
git commit -m "fix: add incremental refresh path to mv_profit_daily"
```

---

## Task 3: Clear `lru_cache` After MV Refresh

**Files:**
- Modify: `services/profit_metrics.py`
- Modify: `services/duckdb_connector.py` (add sales cache clear)
- Modify: `etl_tasks.py` (~line 2265, after CLI refresh)

`@lru_cache` on `query_profit_trends`, `query_profit_summary`, `query_sales_trends`, `query_revenue_comparison`, `query_top_products` means stale results survive MV refresh. Must clear after refresh.

- [ ] **Step 1: Add `clear_sales_caches` to `duckdb_connector.py`**

At the bottom of `services/duckdb_connector.py`, before the last function, add:

```python
def clear_sales_caches() -> None:
    """Clear all lru_cache'd sales query functions to force fresh reads after MV refresh."""
    query_sales_trends.cache_clear()
    query_revenue_comparison.cache_clear()
    query_top_products.cache_clear()
    query_overview_summary.cache_clear()
    try:
        query_hourly_sales_heatmap.cache_clear()
    except AttributeError:
        pass
```

- [ ] **Step 2: Update `clear_profit_caches` in `profit_metrics.py` to also clear versioned cache**

In `services/profit_metrics.py`, replace `clear_profit_caches`:

```python
def clear_profit_caches() -> None:
    """Clear all cached profit query functions to force fresh reads after ETL/MV updates."""
    query_profit_summary.cache_clear()
    query_profit_revenue_by_category.cache_clear()
    query_profit_trends.cache_clear()
    query_profit_by_product.cache_clear()
    # Also clear versioned Redis cache entries
    try:
        from .cache import cache
        cache.delete_many([
            k for k in (cache._cache.keys() if hasattr(cache, '_cache') else [])
            if 'profit' in str(k)
        ])
    except Exception:
        pass
```

- [ ] **Step 3: Call both cache clears in `etl_tasks.py` after MV refresh**

In `etl_tasks.py`, find the block after `run_compose_exec_with_output` succeeds (around line 2265). After the existing `force-reload` block, add:

```python
            # Clear query caches so dashboard picks up new MV data immediately
            try:
                from services.profit_metrics import clear_profit_caches
                from services.duckdb_connector import clear_sales_caches
                clear_profit_caches()
                clear_sales_caches()
                logger.info("Cleared profit and sales query caches after MV refresh")
            except Exception as cache_exc:
                logger.warning(f"Could not clear query caches: {cache_exc}")
```

- [ ] **Step 4: Commit**

```bash
git add services/duckdb_connector.py services/profit_metrics.py etl_tasks.py
git commit -m "fix: clear lru_cache and versioned cache after MV refresh"
```

---

## Task 4: Verify `force_reload` Bypasses Tracking Correctly

**Files:**
- Modify: `services/duckdb_connector.py` (`ensure_materialized_views`, ~line 105–145)

The `force_reload=True` path added previously checks `existing_in_db` but then filters `needed = needed & existing_in_db`. If an MV exists in DB but has stale data, this is correct. However, the `_materialized_views` tracking set is only updated with `|=` — it never removes MVs. Verify the force_reload path works end-to-end.

- [ ] **Step 1: Verify `ensure_materialized_views` with `force_reload=True` calls `_load_materialized_views`**

Read `services/duckdb_connector.py` lines 105–145. Confirm:
- `force_reload=True` sets `needed = views` (not `views - self._materialized_views`)
- `needed = needed & existing_in_db` — only loads MVs that exist in DB
- Calls `_load_materialized_views(conn, needed)`
- Updates `self._materialized_views |= needed`

If the logic is correct, no change needed. If `needed & existing_in_db` filters out MVs that should be refreshed (e.g., MV exists but is stale), change to:

```python
if force_reload:
    needed = views  # reload all requested, regardless of tracking
    print(f"[duckdb] force-reloading materialized views: {sorted(needed)}")
    # Don't filter by existing_in_db for force_reload — let _load_materialized_views handle missing tables
else:
    needed = views - self._materialized_views
    if not needed:
        return
    # For normal load, only load MVs that exist in DB
    existing_in_db = set()
    try:
        tables = conn.execute("SHOW TABLES").fetchall()
        existing_in_db = {t[0] for t in tables if t[0].startswith('mv_')}
    except Exception as e:
        print(f"[duckdb] error checking existing tables: {e}")
    needed = needed & existing_in_db
    if not needed:
        print("[duckdb] no requested MVs exist in database")
        return
```

- [ ] **Step 2: Commit if changed**

```bash
git add services/duckdb_connector.py
git commit -m "fix: force_reload bypasses existing_in_db filter to allow stale MV refresh"
```

---

## Task 5: Diagnostic Script — Verify MV Date Ranges

**Files:**
- Create: `scripts/check_mv_dates.py`

Run this inside Docker to confirm what dates are in each MV vs what's in parquet.

- [ ] **Step 1: Create diagnostic script**

```python
#!/usr/bin/env python3
"""
Diagnostic: compare MV date ranges vs parquet date ranges.
Run: docker-compose exec dash-app python scripts/check_mv_dates.py
"""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.duckdb_connector import DuckDBManager
from etl.config import (
    AGG_PROFIT_DAILY_PATH, AGG_SALES_DAILY_PATH,
    AGG_SALES_DAILY_BY_PRODUCT_PATH, AGG_SALES_DAILY_BY_PRINCIPAL_PATH,
)

MV_TO_PATH = {
    "mv_profit_daily": AGG_PROFIT_DAILY_PATH,
    "mv_sales_daily": AGG_SALES_DAILY_PATH,
    "mv_sales_by_product": AGG_SALES_DAILY_BY_PRODUCT_PATH,
    "mv_sales_by_principal": AGG_SALES_DAILY_BY_PRINCIPAL_PATH,
}

def check():
    manager = DuckDBManager()
    conn = manager.get_connection()

    print("=" * 60)
    print("MV DATE RANGE DIAGNOSTIC")
    print("=" * 60)

    for mv_name, parquet_path in MV_TO_PATH.items():
        print(f"\n--- {mv_name} ---")

        # Check MV
        try:
            result = conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{mv_name}' AND table_type = 'BASE TABLE'
            """).fetchone()
            if result[0] == 0:
                print(f"  MV: DOES NOT EXIST")
            else:
                row = conn.execute(f"SELECT COUNT(*), MIN(date), MAX(date) FROM {mv_name}").fetchone()
                print(f"  MV rows: {row[0]}, min_date: {row[1]}, max_date: {row[2]}")
        except Exception as e:
            print(f"  MV error: {e}")

        # Check parquet
        try:
            import glob
            files = glob.glob(f"{parquet_path}/**/*.parquet", recursive=True)
            if not files:
                print(f"  Parquet: NO FILES at {parquet_path}")
            else:
                row = conn.execute(f"""
                    SELECT COUNT(*), MIN(date), MAX(date)
                    FROM read_parquet('{parquet_path}/**/*.parquet', 
                                      union_by_name=True, hive_partitioning=1)
                """).fetchone()
                print(f"  Parquet rows: {row[0]}, min_date: {row[1]}, max_date: {row[2]}")
                print(f"  Parquet files: {len(files)}")
        except Exception as e:
            print(f"  Parquet error: {e}")

    # Check lru_cache state
    print("\n--- CACHE STATE ---")
    try:
        from services.profit_metrics import query_profit_trends, query_profit_summary
        print(f"  query_profit_trends cache: {query_profit_trends.cache_info()}")
        print(f"  query_profit_summary cache: {query_profit_summary.cache_info()}")
    except Exception as e:
        print(f"  Cache check error: {e}")

    try:
        from services.duckdb_connector import query_sales_trends
        print(f"  query_sales_trends cache: {query_sales_trends.cache_info()}")
    except Exception as e:
        print(f"  Sales cache check error: {e}")

    print("\n--- mv_refresh_metadata ---")
    try:
        rows = conn.execute("SELECT * FROM mv_refresh_metadata ORDER BY last_refresh_date DESC").fetchall()
        for row in rows:
            print(f"  {row}")
    except Exception as e:
        print(f"  metadata error: {e}")

if __name__ == "__main__":
    check()
```

- [ ] **Step 2: Run diagnostic inside Docker**

```bash
docker-compose exec dash-app python scripts/check_mv_dates.py
```

Expected output shows:
- MV `max_date` matches parquet `max_date` (if fixed)
- OR MV `max_date` is Sept 2025 while parquet `max_date` is May 2026 (confirms bug)

- [ ] **Step 3: Commit**

```bash
git add scripts/check_mv_dates.py
git commit -m "chore: add MV date range diagnostic script"
```

---

## Task 6: Write Tests

**Files:**
- Create: `tests/test_mv_refresh_fix.py`

- [ ] **Step 1: Write tests**

```python
"""Tests for MV refresh fix — stale data bug."""
import pytest
from unittest.mock import MagicMock, patch
from datetime import date


class TestGetMvRefreshInfo:
    """Test _get_mv_refresh_info returns correct (needs_full, max_date, count)."""

    def test_returns_full_refresh_when_table_missing(self):
        """When MV table doesn't exist, needs_full=True."""
        from services.duckdb_connector import DuckDBManager
        manager = DuckDBManager.__new__(DuckDBManager)

        mock_conn = MagicMock()
        # information_schema.tables returns 0 rows (table doesn't exist)
        mock_conn.execute.return_value.fetchone.return_value = (0,)

        needs_full, max_date, count = manager._get_mv_refresh_info(
            mock_conn, "mv_profit_daily", "/fake/path"
        )

        assert needs_full is True
        assert max_date is None

    def test_returns_max_date_when_table_exists(self):
        """When MV exists, returns max_date from actual table."""
        from services.duckdb_connector import DuckDBManager
        manager = DuckDBManager.__new__(DuckDBManager)

        mock_conn = MagicMock()
        # First call: information_schema check → table exists (count=1)
        # Second call: SELECT MAX(date) → returns a date
        mock_conn.execute.return_value.fetchone.side_effect = [
            (1,),                    # table exists
            (date(2025, 9, 30),),    # max date
        ]

        needs_full, max_date, count = manager._get_mv_refresh_info(
            mock_conn, "mv_profit_daily", "/fake/path"
        )

        assert needs_full is False
        assert max_date == date(2025, 9, 30)

    def test_returns_full_refresh_when_max_date_is_none(self):
        """When MV exists but is empty (max_date=None), needs_full=True."""
        from services.duckdb_connector import DuckDBManager
        manager = DuckDBManager.__new__(DuckDBManager)

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.side_effect = [
            (1,),    # table exists
            (None,), # max date is None (empty table)
        ]

        needs_full, max_date, count = manager._get_mv_refresh_info(
            mock_conn, "mv_profit_daily", "/fake/path"
        )

        assert needs_full is True
        assert max_date is None


class TestClearCaches:
    """Test cache clearing functions exist and work."""

    def test_clear_profit_caches_clears_lru(self):
        """clear_profit_caches() clears lru_cache on all profit query fns."""
        from services.profit_metrics import (
            query_profit_trends, query_profit_summary, clear_profit_caches
        )
        # Populate cache with dummy call (will fail but cache_info tracks calls)
        try:
            query_profit_trends(date(2025, 1, 1), date(2025, 1, 31))
        except Exception:
            pass

        clear_profit_caches()

        # After clear, currsize should be 0
        assert query_profit_trends.cache_info().currsize == 0
        assert query_profit_summary.cache_info().currsize == 0

    def test_clear_sales_caches_exists(self):
        """clear_sales_caches() is importable and callable."""
        from services.duckdb_connector import clear_sales_caches
        # Should not raise
        clear_sales_caches()
```

- [ ] **Step 2: Run tests**

```bash
docker-compose exec dash-app pytest tests/test_mv_refresh_fix.py -v
```

Expected:
```
tests/test_mv_refresh_fix.py::TestGetMvRefreshInfo::test_returns_full_refresh_when_table_missing PASSED
tests/test_mv_refresh_fix.py::TestGetMvRefreshInfo::test_returns_max_date_when_table_exists PASSED
tests/test_mv_refresh_fix.py::TestGetMvRefreshInfo::test_returns_full_refresh_when_max_date_is_none PASSED
tests/test_mv_refresh_fix.py::TestClearCaches::test_clear_profit_caches_clears_lru PASSED
tests/test_mv_refresh_fix.py::TestClearCaches::test_clear_sales_caches_exists PASSED
```

- [ ] **Step 3: Commit**

```bash
git add tests/test_mv_refresh_fix.py
git commit -m "test: add tests for MV refresh fix and cache clearing"
```

---

## Task 7: End-to-End Verification

- [ ] **Step 1: Run diagnostic before fix**

```bash
docker-compose exec dash-app python scripts/check_mv_dates.py
```

Note the `max_date` for each MV. Expect Sept 2025 or earlier.

- [ ] **Step 2: Restart dash-app to apply code changes**

```bash
docker-compose restart dash-app
```

- [ ] **Step 3: Trigger MV refresh from operational page**

In browser: go to `/operational` → click "Refresh MVs" button with today's date range.

OR via CLI:
```bash
docker-compose exec celery-worker python scripts/etl_data_manager_cli.py refresh-mvs-cascading \
  --views mv_profit_daily,mv_sales_daily,mv_sales_by_product,mv_sales_by_principal \
  --start 2026-01-01 --end 2026-05-13 --auto-fetch
```

- [ ] **Step 4: Run diagnostic after fix**

```bash
docker-compose exec dash-app python scripts/check_mv_dates.py
```

Expected: `max_date` for all MVs now shows May 2026.

- [ ] **Step 5: Verify dashboard shows May 2026 data**

Open browser → Overview page → set date range to May 2026 → click Apply. Charts should show data.

- [ ] **Step 6: Final commit**

```bash
git add .
git commit -m "fix: MV stale data — fix refresh info query, add profit incremental, clear caches"
```

---

## Self-Review

**Spec coverage:**
- Bug 1 (`_get_mv_refresh_info` broken SQL) → Task 1 ✓
- Bug 2 (`mv_profit_daily` no incremental) → Task 2 ✓
- Bug 3 (`lru_cache` stale) → Task 3 ✓
- Bug 4 (`force_reload` tracking) → Task 4 ✓
- Diagnostic tooling → Task 5 ✓
- Tests → Task 6 ✓
- E2E verification → Task 7 ✓

**Placeholder scan:** None found. All code blocks complete.

**Type consistency:** `_get_mv_refresh_info` returns `tuple[bool, Optional[date], int]` — consistent across Tasks 1 and 2. `clear_sales_caches` defined in Task 3 Step 1, referenced in Task 3 Step 3 — consistent.
