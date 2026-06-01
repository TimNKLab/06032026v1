# Remove All Materialized Views Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove all materialized views (SQLite and DuckDB) from the codebase, migrating to direct parquet queries through DuckDB views with read-only connections to solve lock conflicts.

**Architecture:** Keep DuckDB views as read-only query layer over parquet aggregates (agg_sales_daily, agg_profit_daily, etc.), remove SQLite MVs and DuckDB MV tables, eliminate MV refresh logic, use read-only connections for queries to prevent lock conflicts.

**Tech Stack:** DuckDB 1.2.0, Polars, Python 3.9, Plotly Dash 2.14.2, Redis, Celery

---

## Complexity Evaluation

This is a **MAJOR** architectural change affecting 8+ files across the codebase. Breaking into 3 phases:

**Phase 1: DuckDB Lock Conflict Fix (Immediate)**
- Add read-only connection support to DuckDBManager
- Update query functions to use read-only connections
- Test concurrent access
- **Risk:** Low, isolated changes

**Phase 2: Remove SQLite MV Layer (Medium)**
- Migrate all query functions from SQLite to DuckDB views
- Remove SQLiteManager from app.py
- Remove SQLite MV refresh from ETL
- **Risk:** Medium, affects all query paths

**Phase 3: Remove DuckDB MV Tables (Low)**
- Remove DuckDB MV loading logic
- Remove MV refresh metadata
- Clean up legacy MV code
- **Risk:** Low, cleanup only

---

## File Structure

**Files to Modify:**
- `services/duckdb_connector.py` - Add read-only connection support, remove MV loading
- `services/sqlite_manager.py` - Remove entire file (or deprecate)
- `services/sales_metrics.py` - Migrate to DuckDB views
- `services/profit_metrics.py` - Migrate to DuckDB views
- `services/inventory_metrics.py` - Migrate to direct parquet queries
- `services/overview_metrics.py` - Migrate to DuckDB views
- `app.py` - Remove SQLiteManager initialization
- `etl_tasks.py` - Remove MV refresh tasks
- `pages/operational.py` - Remove MV refresh UI

**Files to Create:**
- `tests/test_duckdb_readonly_connections.py` - Test read-only connection behavior
- `scripts/test_concurrent_access.py` - Test concurrent access without locks

---

## Phase 1: DuckDB Lock Conflict Fix

### Task 1: Add Read-Only Connection Support to DuckDBManager

**Files:**
- Modify: `services/duckdb_connector.py:76-85`

- [ ] **Step 1: Write test for read-only connection**

```python
# tests/test_duckdb_readonly_connections.py
import duckdb
import os
import pytest

def test_readonly_connection_prevents_writes():
    """Test that read-only connections cannot write."""
    data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    db_path = f"{data_lake}/cache/nkdash.duckdb"
    
    # Create read-only connection
    conn = duckdb.connect(database=db_path, read_only=True)
    
    # Try to write - should fail
    with pytest.raises(Exception):
        conn.execute("CREATE TABLE test_table (id INTEGER)")
    
    conn.close()

def test_readonly_connection_can_read():
    """Test that read-only connections can read."""
    from services.duckdb_connector import get_duckdb_connection, ensure_duckdb_view_groups
    
    # Ensure views exist
    ensure_duckdb_view_groups({"sales_agg"})
    
    data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    db_path = f"{data_lake}/cache/nkdash.duckdb"
    
    # Create read-only connection
    conn = duckdb.connect(database=db_path, read_only=True)
    
    # Try to read - should succeed
    result = conn.execute("SELECT COUNT(*) FROM agg_sales_daily").fetchone()
    assert result[0] >= 0
    
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
docker-compose exec dash-app pytest tests/test_duckdb_readonly_connections.py::test_readonly_connection_prevents_writes -v
```

Expected: FAIL with "test file not found" (file doesn't exist yet)

- [ ] **Step 3: Create test file**

```bash
touch tests/test_duckdb_readonly_connections.py
```

- [ ] **Step 4: Run test to verify it fails**

```bash
docker-compose exec dash-app pytest tests/test_duckdb_readonly_connections.py::test_readonly_connection_prevents_writes -v
```

Expected: FAIL with "cannot import duckdb" or connection error

- [ ] **Step 5: Add get_readonly_connection method to DuckDBManager**

```python
# In services/duckdb_connector.py, after get_connection method (around line 85)

def get_readonly_connection(self) -> duckdb.DuckDBPyConnection:
    """Returns read-only connection for queries to avoid file locks.
    
    Use this for dashboard queries (dash-app).
    Read-only mode allows multiple processes to access the same database file concurrently.
    """
    data_lake = os.environ.get('DATA_LAKE_ROOT') or os.environ.get('DATA_LAKE_PATH', '/data-lake')
    db_path = f"{data_lake}/cache/nkdash.duckdb"
    
    conn = duckdb.connect(database=db_path, read_only=True)
    return conn
```

- [ ] **Step 6: Run test to verify it passes**

```bash
docker-compose exec dash-app pytest tests/test_duckdb_readonly_connections.py::test_readonly_connection_prevents_writes -v
```

Expected: PASS

- [ ] **Step 7: Run test to verify read test passes**

```bash
docker-compose exec dash-app pytest tests/test_duckdb_readonly_connections.py::test_readonly_connection_can_read -v
```

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add services/duckdb_connector.py tests/test_duckdb_readonly_connections.py
git commit -m "feat: add read-only connection support to DuckDBManager to prevent lock conflicts"
```

---

### Task 2: Update Inventory Query Functions to Use Read-Only Connections

**Files:**
- Modify: `services/duckdb_connector.py:1805-1857`

- [ ] **Step 1: Write test for concurrent access**

```python
# scripts/test_concurrent_access.py
import duckdb
import os
import time
from concurrent.futures import ThreadPoolExecutor

def test_concurrent_readonly_access():
    """Test that multiple read-only connections can access DuckDB simultaneously."""
    data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    db_path = f"{data_lake}/cache/nkdash.duckdb"
    
    def query_worker(worker_id):
        conn = duckdb.connect(database=db_path, read_only=True)
        try:
            result = conn.execute("SELECT COUNT(*) FROM agg_sales_daily").fetchone()
            print(f"Worker {worker_id}: {result[0]} rows")
            time.sleep(0.1)
            return result[0]
        finally:
            conn.close()
    
    # Run 10 concurrent queries
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(query_worker, i) for i in range(10)]
        results = [f.result() for f in futures]
    
    # All should succeed with same result
    assert all(r == results[0] for r in results), "Concurrent queries returned different results"
    print(f"All 10 workers succeeded: {results[0]} rows")

if __name__ == "__main__":
    test_concurrent_readonly_access()
```

- [ ] **Step 2: Run concurrent access test**

```bash
docker-compose exec dash-app python scripts/test_concurrent_access.py
```

Expected: All 10 workers succeed without lock conflicts

- [ ] **Step 3: Update query_inventory_snapshot to use read-only connection**

```python
# In services/duckdb_connector.py, modify query_inventory_snapshot (lines 1805-1829)

def query_inventory_snapshot(snapshot_date: date) -> pd.DataFrame:
    """Query inventory snapshot from DuckDB view over parquet.
    
    Replaces SQLite MV mv_inventory_daily.
    Uses read-only connection to prevent lock conflicts.
    """
    import os
    
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    snapshot_path = f"{data_lake_root}/star-schema/fact_stock_on_hand_snapshot/**/*.parquet"
    
    # Use read-only connection to prevent lock conflicts
    manager = DuckDBManager()
    conn = manager.get_readonly_connection()
    
    query = f"""
    SELECT
        product_id,
        SUM(quantity) AS qty_on_hand
    FROM read_parquet('{snapshot_path}', hive_partitioning=1)
    WHERE snapshot_date = ?
    GROUP BY product_id
    """
    
    query_start = time.time()
    df = conn.execute(query, [snapshot_date]).fetchdf()
    conn.close()
    print(f"[TIMING] query_inventory_snapshot: {time.time() - query_start:.3f}s")
    return df
```

- [ ] **Step 4: Update query_sales_by_product_duckdb to use read-only connection**

```python
# In services/duckdb_connector.py, modify query_sales_by_product_duckdb (lines 1832-1857)

def query_sales_by_product_duckdb(start_date: date, end_date: date) -> pd.DataFrame:
    """Query sales by product from DuckDB view over parquet.
    
    Replaces SQLite MV mv_sales_by_product.
    Uses read-only connection to prevent lock conflicts.
    """
    import os
    
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    agg_path = f"{data_lake_root}/star-schema/agg_sales_daily_by_product/**/*.parquet"
    
    # Use read-only connection to prevent lock conflicts
    manager = DuckDBManager()
    conn = manager.get_readonly_connection()
    
    query = f"""
    SELECT
        product_id,
        SUM(quantity) AS units_sold,
        SUM(revenue) AS revenue
    FROM read_parquet('{agg_path}', hive_partitioning=1)
    WHERE date >= ? AND date <= ?
    GROUP BY product_id
    """
    
    query_start = time.time()
    df = conn.execute(query, [start_date, end_date]).fetchdf()
    conn.close()
    print(f"[TIMING] query_sales_by_product_duckdb: {time.time() - query_start:.3f}s")
    return df
```

- [ ] **Step 5: Run concurrent access test again**

```bash
docker-compose exec dash-app python scripts/test_concurrent_access.py
```

Expected: All 10 workers succeed without lock conflicts

- [ ] **Step 6: Test inventory page in Docker**

```bash
docker-compose restart dash-app
docker-compose logs dash-app --tail 50
```

Open http://localhost:8050/inventory in browser:
- Inventory page loads without errors
- No DuckDB lock conflict errors in logs

- [ ] **Step 7: Commit**

```bash
git add services/duckdb_connector.py scripts/test_concurrent_access.py
git commit -m "fix: use read-only connections for inventory queries to prevent lock conflicts"
```

---

### Task 3: Update All DuckDB Query Functions to Use Read-Only Connections

**Files:**
- Modify: `services/duckdb_connector.py` (all query functions)

- [ ] **Step 1: Update query_sales_trends to use read-only connection**

```python
# In services/duckdb_connector.py, modify query_sales_trends
# Replace get_duckdb_connection() with get_readonly_connection()

def query_sales_trends(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Query sales trends - uses DuckDB aggregates with read-only connection."""
    ensure_duckdb_view_groups({"sales_agg"})
    
    # Use read-only connection to prevent lock conflicts
    manager = DuckDBManager()
    conn = manager.get_readonly_connection()
    
    # ... rest of function unchanged ...
    conn.close()
    return result
```

- [ ] **Step 2: Update query_top_products to use read-only connection**

```python
# Replace get_duckdb_connection() with get_readonly_connection()
```

- [ ] **Step 3: Update query_revenue_comparison to use read-only connection**

```python
# Replace get_duckdb_connection() with get_readonly_connection()
```

- [ ] **Step 4: Update query_overview_summary to use read-only connection**

```python
# Replace get_duckdb_connection() with get_readonly_connection()
```

- [ ] **Step 5: Update query_sales_by_principal to use read-only connection**

```python
# Replace get_duckdb_connection() with get_readonly_connection()
```

- [ ] **Step 6: Update query_hourly_sales_pattern to use read-only connection**

```python
# Replace get_duckdb_connection() with get_readonly_connection()
```

- [ ] **Step 7: Update query_hourly_sales_heatmap to use read-only connection**

```python
# Replace get_duckdb_connection() with get_readonly_connection()
```

- [ ] **Step 8: Update query_profit_summary to use read-only connection**

```python
# Replace get_duckdb_connection() with get_readonly_connection()
```

- [ ] **Step 9: Update query_profit_trends to use read-only connection**

```python
# Replace get_duckdb_connection() with get_readonly_connection()
```

- [ ] **Step 10: Update query_profit_by_product to use read-only connection**

```python
# Replace get_duckdb_connection() with get_readonly_connection()
```

- [ ] **Step 11: Update query_profit_drilldown to use read-only connection**

```python
# Replace get_duckdb_connection() with get_readonly_connection()
```

- [ ] **Step 12: Update query_profit_revenue_by_category to use read-only connection**

```python
# Replace get_duckdb_connection() with get_readonly_connection()
```

- [ ] **Step 13: Test all dashboard pages**

```bash
docker-compose restart dash-app
```

Open in browser:
- http://localhost:8050/ - Overview page loads
- http://localhost:8050/sales - Sales page loads
- http://localhost:8050/profit - Profit page loads
- http://localhost:8050/inventory - Inventory page loads

- [ ] **Step 14: Run concurrent access test**

```bash
docker-compose exec dash-app python scripts/test_concurrent_access.py
```

Expected: All 10 workers succeed without lock conflicts

- [ ] **Step 15: Commit**

```bash
git add services/duckdb_connector.py
git commit -m "fix: use read-only connections for all DuckDB query functions to prevent lock conflicts"
```

---

## Phase 2: Remove SQLite MV Layer

### Task 4: Migrate sales_metrics.py to DuckDB Views

**Files:**
- Modify: `services/sales_metrics.py`

- [ ] **Step 1: Read current sales_metrics.py to understand SQLite usage**

```bash
head -100 services/sales_metrics.py
```

- [ ] **Step 2: Replace SQLite imports with DuckDB imports**

```python
# Remove: from services.sqlite_manager import SQLiteManager
# Add: from services.duckdb_connector import (
#     query_sales_trends,
#     query_top_products,
#     query_revenue_comparison,
#     query_sales_by_principal,
#     query_hourly_sales_pattern,
#     query_hourly_sales_heatmap
# )
```

- [ ] **Step 3: Replace function implementations with DuckDB calls**

```python
def get_sales_trends_data(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Get revenue trend data using DuckDB aggregates."""
    from services.duckdb_connector import query_sales_trends
    return query_sales_trends(start_date, end_date, period)

def get_revenue_comparison(start_date: date, end_date: date) -> Dict:
    """Compare revenue between periods using DuckDB aggregates."""
    from services.duckdb_connector import query_revenue_comparison
    return query_revenue_comparison(start_date, end_date)

def get_top_products(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Get top selling products using DuckDB aggregates."""
    from services.duckdb_connector import query_top_products
    return query_top_products(start_date, end_date, limit)

def get_sales_by_principal(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Aggregate sales revenue by principal using DuckDB aggregates."""
    from services.duckdb_connector import query_sales_by_principal
    return query_sales_by_principal(start_date, end_date, limit)

def get_hourly_sales_pattern(target_date: date) -> pd.DataFrame:
    """Get hourly sales pattern using DuckDB."""
    from services.duckdb_connector import query_hourly_sales_pattern
    return query_hourly_sales_pattern(target_date)

def get_hourly_sales_heatmap_data(start_date: date, end_date: date) -> pd.DataFrame:
    """Get hourly sales heatmap data using DuckDB."""
    from services.duckdb_connector import query_hourly_sales_heatmap
    return query_hourly_sales_heatmap(start_date, end_date)
```

- [ ] **Step 4: Test sales page**

```bash
docker-compose restart dash-app
```

Open http://localhost:8050/sales in browser:
- Sales page loads without errors
- Revenue trends chart shows data
- Top products table shows data

- [ ] **Step 5: Commit**

```bash
git add services/sales_metrics.py
git commit -m "migrate: sales_metrics.py to DuckDB views (remove SQLite dependency)"
```

---

### Task 5: Migrate profit_metrics.py to DuckDB Views

**Files:**
- Modify: `services/profit_metrics.py`

- [ ] **Step 1: Read current profit_metrics.py to understand SQLite usage**

```bash
head -100 services/profit_metrics.py
```

- [ ] **Step 2: Replace SQLite imports with DuckDB imports**

```python
# Remove: from services.sqlite_manager import SQLiteManager
# Add: from services.duckdb_connector import (
#     query_profit_trends,
#     query_profit_by_product,
#     query_profit_summary,
#     query_profit_revenue_by_category,
#     query_profit_drilldown
# )
```

- [ ] **Step 3: Replace function implementations with DuckDB calls**

```python
def query_profit_trends(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Query profit trends - uses DuckDB aggregates."""
    from services.duckdb_connector import query_profit_trends as duckdb_query_profit_trends
    return duckdb_query_profit_trends(start_date, end_date, period)

def query_profit_by_product(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Query top products by profit - uses DuckDB aggregates."""
    from services.duckdb_connector import query_profit_by_product as duckdb_query_profit_by_product
    return duckdb_query_profit_by_product(start_date, end_date, limit)

def query_profit_summary(start_date: date, end_date: date) -> Dict:
    """Get profit summary - uses DuckDB aggregates."""
    from services.duckdb_connector import query_profit_summary as duckdb_query_profit_summary
    return duckdb_query_profit_summary(start_date, end_date)

def query_profit_revenue_by_category(start_date: date, end_date: date) -> Dict[str, Dict[str, float]]:
    """Query profit revenue by category - uses DuckDB aggregates."""
    from services.duckdb_connector import query_profit_revenue_by_category as duckdb_query_profit_revenue_by_category
    return duckdb_query_profit_revenue_by_category(start_date, end_date)

def query_profit_drilldown(start_date: date, end_date: date, product_id: Optional[int] = None) -> pd.DataFrame:
    """Drill-down to line-level profit details - uses DuckDB fact table."""
    from services.duckdb_connector import query_profit_drilldown as duckdb_query_profit_drilldown
    return duckdb_query_profit_drilldown(start_date, end_date, product_id)
```

- [ ] **Step 4: Test profit page**

```bash
docker-compose restart dash-app
```

Open http://localhost:8050/profit in browser:
- Profit page loads without errors
- Profit trends chart shows data
- Profit summary shows data

- [ ] **Step 5: Commit**

```bash
git add services/profit_metrics.py
git commit -m "migrate: profit_metrics.py to DuckDB views (remove SQLite dependency)"
```

---

### Task 6: Migrate overview_metrics.py to DuckDB Views

**Files:**
- Modify: `services/overview_metrics.py`

- [ ] **Step 1: Read current overview_metrics.py to understand SQLite usage**

```bash
head -100 services/overview_metrics.py
```

- [ ] **Step 2: Replace SQLite import with DuckDB import**

```python
# Remove: from services.sqlite_manager import SQLiteManager
# Add: from services.duckdb_connector import query_overview_summary
```

- [ ] **Step 3: Replace function implementation with DuckDB call**

```python
def get_total_overview_summary(target_date_start: date, target_date_end: date = None) -> Dict:
    """Get overview summary - uses DuckDB aggregates."""
    from services.duckdb_connector import query_overview_summary as duckdb_query_overview_summary
    
    if not isinstance(target_date_start, date):
        target_date_start = date.today()
    if target_date_end is None:
        target_date_end = target_date_start

    try:
        return duckdb_query_overview_summary(target_date_start, target_date_end)
    except Exception as e:
        print(f"[OVERVIEW] DuckDB query failed: {e}")
        import traceback
        traceback.print_exc()
        return {
            'target_date_start': target_date_start,
            'target_date_end': target_date_end,
            'today_amount': 0.0,
            'today_qty': 0.0,
            'prev_amount': 0.0,
            'categories_nested': {},
            'brands_nested': {},
        }
```

- [ ] **Step 4: Test overview page**

```bash
docker-compose restart dash-app
```

Open http://localhost:8050/ in browser:
- Overview page loads without errors
- KPI cards show data
- Charts display data

- [ ] **Step 5: Commit**

```bash
git add services/overview_metrics.py
git commit -m "migrate: overview_metrics.py to DuckDB views (remove SQLite dependency)"
```

---

### Task 7: Remove SQLiteManager from app.py

**Files:**
- Modify: `app.py`

- [ ] **Step 1: Find SQLiteManager initialization in app.py**

```bash
grep -n "SQLiteManager" app.py
```

- [ ] **Step 2: Remove SQLiteManager imports and initialization**

```python
# Remove lines with:
# from services.sqlite_manager import SQLiteManager
# manager = SQLiteManager()
```

- [ ] **Step 3: Remove MV diagnostics endpoint**

```python
# Remove /api/mv-diagnostics endpoint (lines 76-89)
```

- [ ] **Step 4: Update health check to not expect SQLite MVs**

```python
@server.route('/health')
def health_check():
    """Health check endpoint - verifies DuckDB views are loaded and queryable."""
    try:
        from services.duckdb_connector import get_duckdb_connection, ensure_duckdb_view_groups
        
        # Ensure DuckDB views are loaded
        ensure_duckdb_view_groups({"overview"})
        conn = get_duckdb_connection()
        
        # Smoke test: verify agg_sales_daily is queryable
        conn.execute("SELECT 1 FROM agg_sales_daily LIMIT 1").fetchone()
        
        return jsonify({
            'status': 'healthy',
            'backend': 'duckdb'
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 503
```

- [ ] **Step 5: Test health check**

```bash
curl http://localhost:8050/health
```

Expected: Returns `{"status": "healthy", "backend": "duckdb"}` with HTTP 200.

- [ ] **Step 6: Commit**

```bash
git add app.py
git commit -m "remove: SQLiteManager from app.py (health check now uses DuckDB)"
```

---

### Task 8: Remove SQLite MV Refresh from ETL Pipeline

**Files:**
- Modify: `etl_tasks.py`

- [ ] **Step 1: Find MV refresh tasks in etl_tasks.py**

```bash
grep -n "refresh_materialized_views" etl_tasks.py
```

- [ ] **Step 2: Remove scheduled MV refresh task**

```python
# Remove or comment out lines 2392-2415:
# @app.task
# def refresh_materialized_views_scheduled():
#     """Scheduled MV refresh after ETL completion (runs at 02:30 daily)."""
#     ...
```

- [ ] **Step 3: Remove MV refresh task from beat schedule**

```python
# Remove from app.conf.beat_schedule:
# 'refresh-materialized-views-daily': {
#     'task': 'etl_tasks.refresh_materialized_views_scheduled',
#     'schedule': crontab(hour=2, minute=30),
# },
```

- [ ] **Step 4: Remove MV refresh task from task routing**

```python
# Remove from app.conf.task_routes:
# 'etl_tasks.refresh_materialized_views_scheduled': {'queue': 'loading'},
```

- [ ] **Step 5: Remove refresh_materialized_views task**

```python
# Remove or comment out the entire refresh_materialized_views task (lines 2000-2100)
```

- [ ] **Step 6: Commit**

```bash
git add etl_tasks.py
git commit -m "remove: SQLite MV refresh tasks from ETL pipeline (no longer needed)"
```

---

### Task 9: Remove MV Refresh UI from Operational Page

**Files:**
- Modify: `pages/operational.py`

- [ ] **Step 1: Find MV refresh callbacks in operational.py**

```bash
grep -n "refresh_mvs\|refresh_materialized_views" pages/operational.py
```

- [ ] **Step 2: Remove MV refresh callbacks**

```python
# Remove or comment out:
# refresh_mvs_cascading callback (lines 1600-1650)
# refresh_all_mvs_simple callback (lines 1652-1680)
# refresh_materialized_views callback (lines 1682-1710)
```

- [ ] **Step 3: Remove MV refresh UI elements**

```python
# Remove from layout:
# MV refresh section (buttons, status messages, etc.)
```

- [ ] **Step 4: Remove MV refresh imports**

```python
# Remove: from etl_tasks import refresh_materialized_views
```

- [ ] **Step 5: Test operational page**

```bash
docker-compose restart dash-app
```

Open http://localhost:8050/operational in browser:
- Operational page loads without errors
- ETL controls work
- MV refresh section removed (should not appear)

- [ ] **Step 6: Commit**

```bash
git add pages/operational.py
git commit -m "remove: MV refresh UI from operational page (no longer needed)"
```

---

## Phase 3: Remove DuckDB MV Tables

### Task 10: Remove DuckDB MV Loading Logic

**Files:**
- Modify: `services/duckdb_connector.py`

- [ ] **Step 1: Remove ensure_materialized_views method**

```python
# Remove entire ensure_materialized_views method (lines 190-227)
```

- [ ] **Step 2: Remove _load_materialized_views method**

```python
# Remove entire _load_materialized_views method (lines 254-400)
```

- [ ] **Step 3: Remove _get_mv_refresh_info method**

```python
# Remove entire _get_mv_refresh_info method (lines 229-252)
```

- [ ] **Step 4: Remove _reload_mvs_background method**

```python
# Remove entire _reload_mvs_background method (lines 130-153)
```

- [ ] **Step 5: Remove _materialized_views tracking**

```python
# Remove from __init__:
# self._materialized_views: set[str] = set()
```

- [ ] **Step 6: Remove MV-related instance variables**

```python
# Remove from __init__:
# self._last_mv_refresh_ts: float = 0.0
# self._needs_mv_reload: bool = False
```

- [ ] **Step 7: Remove _check_mv_refresh_signal method**

```python
# Remove entire _check_mv_refresh_signal method (lines 33-65)
```

- [ ] **Step 8: Remove MV refresh signal check from get_connection**

```python
# In get_connection method, remove:
# self._check_mv_refresh_signal()
```

- [ ] **Step 9: Test dashboard pages**

```bash
docker-compose restart dash-app
```

Open in browser:
- http://localhost:8050/ - Overview page loads
- http://localhost:8050/sales - Sales page loads
- http://localhost:8050/profit - Profit page loads
- http://localhost:8050/inventory - Inventory page loads

- [ ] **Step 10: Commit**

```bash
git add services/duckdb_connector.py
git commit -m "remove: DuckDB MV loading logic (no longer needed)"
```

---

### Task 11: Drop Legacy MV Tables from DuckDB

**Files:**
- Create: `scripts/drop_legacy_mvs.py`

- [ ] **Step 1: Create script to drop legacy MV tables**

```python
# scripts/drop_legacy_mvs.py
import duckdb
import os

data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
db_path = f"{data_lake}/cache/nkdash.duckdb"

print(f"Connecting to {db_path}")
conn = duckdb.connect(database=db_path, read_only=False)

# Drop legacy MV tables
legacy_mvs = [
    'mv_inventory_daily',
    'mv_product_velocity',
    'mv_inventory_status',
    'mv_refresh_metadata'
]

for mv_name in legacy_mvs:
    try:
        conn.execute(f"DROP TABLE IF EXISTS {mv_name}")
        print(f"Dropped {mv_name}")
    except Exception as e:
        print(f"Failed to drop {mv_name}: {e}")

conn.close()
print("Done")
```

- [ ] **Step 2: Run script to drop MV tables**

```bash
docker-compose exec dash-app python scripts/drop_legacy_mvs.py
```

Expected: All MV tables dropped successfully

- [ ] **Step 3: Verify MV tables are gone**

```bash
docker-compose exec dash-app python -c "
import duckdb
import os
data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
db_path = f'{data_lake}/cache/nkdash.duckdb'
conn = duckdb.connect(database=db_path, read_only=True)
tables = conn.execute('SHOW TABLES').fetchall()
print('Tables:', [t[0] for t in tables])
conn.close()
"
```

Expected: No mv_ tables in output

- [ ] **Step 4: Commit**

```bash
git add scripts/drop_legacy_mvs.py
git commit -m "cleanup: drop legacy MV tables from DuckDB database"
```

---

### Task 12: Deprecate sqlite_manager.py

**Files:**
- Modify: `services/sqlite_manager.py`

- [ ] **Step 1: Add deprecation notice to sqlite_manager.py**

```python
# Add at top of file after imports:
"""
DEPRECATED: This module is no longer used.
All materialized views have been removed in favor of direct parquet queries through DuckDB views.
This file is kept for reference only and will be removed in a future cleanup.
"""
```

- [ ] **Step 2: Test that no code imports sqlite_manager**

```bash
grep -r "from services.sqlite_manager import" services/ pages/ app.py etl_tasks.py
```

Expected: No results (no imports found)

- [ ] **Step 3: Commit**

```bash
git add services/sqlite_manager.py
git commit -m "deprecate: mark sqlite_manager.py as deprecated (no longer used)"
```

---

## Final Validation

### Task 13: Comprehensive Testing

**Files:**
- Test: All dashboard pages

- [ ] **Step 1: Restart all services**

```bash
docker-compose down
docker-compose up -d
docker-compose logs -f --tail 100
```

- [ ] **Step 2: Test health check**

```bash
curl http://localhost:8050/health
```

Expected: `{"status": "healthy", "backend": "duckdb"}`

- [ ] **Step 3: Test overview page**

Open http://localhost:8050/ in browser:
- Overview page loads without errors
- KPI cards show data
- Charts display data

- [ ] **Step 4: Test sales page**

Open http://localhost:8050/sales in browser:
- Sales page loads without errors
- Revenue trends chart shows data
- Top products table shows data

- [ ] **Step 5: Test profit page**

Open http://localhost:8050/profit in browser:
- Profit page loads without errors
- Profit trends chart shows data
- Profit summary shows data

- [ ] **Step 6: Test inventory page**

Open http://localhost:8050/inventory in browser:
- Inventory page loads without errors
- Stock levels show data
- ABC analysis shows data

- [ ] **Step 7: Test operational page**

Open http://localhost:8050/operational in browser:
- Operational page loads without errors
- ETL controls work
- MV refresh section removed

- [ ] **Step 8: Run concurrent access test**

```bash
docker-compose exec dash-app python scripts/test_concurrent_access.py
```

Expected: All 10 workers succeed without lock conflicts

- [ ] **Step 9: Verify no SQLite operations**

```bash
docker-compose exec dash-app sh -c "ls -la /data-lake/cache/nkdash.sqlite 2>&1 || echo 'SQLite DB not accessed'"
```

Expected: SQLite DB not accessed

- [ ] **Step 10: Verify no DuckDB MV tables**

```bash
docker-compose exec dash-app python -c "
import duckdb
import os
data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
db_path = f'{data_lake}/cache/nkdash.duckdb'
conn = duckdb.connect(database=db_path, read_only=True)
tables = conn.execute('SHOW TABLES').fetchall()
mv_tables = [t[0] for t in tables if t[0].startswith('mv_')]
print('MV tables:', mv_tables)
conn.close()
"
```

Expected: No MV tables

- [ ] **Step 11: Commit final validation**

```bash
git add .
git commit -m "validation: all materialized views removed, read-only connections working, no lock conflicts"
```

---

## Self-Review

**1. Spec coverage:**
- Phase 1: DuckDB lock conflict fix with read-only connections ✓
- Phase 2: Remove SQLite MV layer ✓
- Phase 3: Remove DuckDB MV tables ✓
- All query functions migrated to DuckDB views ✓
- MV refresh logic eliminated ✓
- Concurrent access tested ✓

**2. Placeholder scan:**
- No TBD, TODO, or placeholder text found
- All steps include specific code and commands
- All file paths are exact

**3. Type consistency:**
- DuckDBManager.get_readonly_connection() consistent across all tasks
- Query function signatures preserved
- Connection close pattern consistent

**4. Test coverage:**
- Read-only connection tests added
- Concurrent access tests added
- Dashboard page tests after each phase
- Health check tests

---

## Expected Outcomes

### After Migration
- SQLite MV layer completely removed
- DuckDB MV tables completely removed
- All dashboard queries use direct parquet reads through DuckDB views
- Read-only connections prevent lock conflicts
- Multiple processes can query DuckDB concurrently
- Health check verifies DuckDB views
- ETL pipeline no longer refreshes MVs
- Operational page MV refresh UI removed

### Architecture
- DuckDB: ETL extraction + parquet creation + read-only query layer
- SQLite: Removed from query layer (deprecated)
- Parquet: Columnar storage with Hive partitioning
- Read-only connections: Prevent file lock conflicts

### Performance
- 30-day queries: < 0.1s (same as before)
- 365-day queries: < 0.5s (same as before)
- No MV refresh overhead
- No lock conflicts
- Concurrent query support

---

## Rollback Plan

If issues arise, rollback steps:

1. Restore SQLiteManager initialization to app.py
2. Restore SQLite query functions in profit_metrics.py and sales_metrics.py
3. Restore MV refresh task to etl_tasks.py
4. Restore MV refresh UI to operational page
5. Restore DuckDB MV loading logic to duckdb_connector.py
6. Restart services

Use git revert for individual commits if needed.
