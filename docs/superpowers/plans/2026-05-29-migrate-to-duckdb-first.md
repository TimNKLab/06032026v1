# Migrate to DuckDB-First Architecture - Reverse SQLite Migration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove SQLite MV layer entirely and migrate all dashboard queries to DuckDB aggregates.

**Architecture:** DuckDB for ETL + DuckDB for queries (parquet aggregates). SQLite removed from query layer.

**Tech Stack:** DuckDB, Parquet with Hive partitioning, Redis caching, Python

---

## Current State Analysis

### DuckDB Query Functions (Already Exist)
From `services/duckdb_connector.py`:
- `query_sales_trends` - Sales trends data
- `query_top_products` - Top products by revenue
- `query_revenue_comparison` - Revenue comparison between periods
- `query_overview_summary` - Overview summary (categories, brands)
- `query_sales_by_principal` - Sales by principal/brand
- `query_hourly_sales_pattern` - Hourly sales pattern
- `query_hourly_sales_heatmap` - Hourly sales heatmap

### Missing DuckDB Query Functions (Need to Add)
- `query_profit_trends` - Profit trends data
- `query_profit_summary` - Profit summary
- `query_profit_by_product` - Profit by product
- `query_profit_revenue_by_category` - Profit by category
- `query_profit_drilldown` - Profit drilldown (line-level)

### Current SQLite Dependencies
- `services/profit_metrics.py` - ALL functions use SQLiteManager
- `services/sales_metrics.py` - ALL functions use SQLiteManager
- `services/overview_metrics.py` - Uses SQLiteManager
- `app.py` - Initializes SQLiteManager twice
- `etl_tasks.py` - Has scheduled MV refresh task
- `pages/operational.py` - Has MV refresh UI

---

## Implementation Plan

### Task 1: Add Missing Profit Query Functions to DuckDB

**Files:**
- Modify: `services/duckdb_connector.py`

- [ ] **Step 1: Add query_profit_summary function**

```python
def query_profit_summary(start_date: date, end_date: date) -> Dict:
    """Get profit summary - uses DuckDB aggregates."""
    ensure_duckdb_view_groups({"profit_agg"})
    conn = get_duckdb_connection()
    
    query = """
    SELECT 
        SUM(revenue_tax_in) as revenue,
        SUM(cogs_tax_in) as cogs,
        SUM(gross_profit) as gross_profit,
        SUM(quantity) as quantity,
        SUM(transactions) as transactions,
        SUM(lines) as lines,
        CASE 
            WHEN SUM(transactions) > 0 
            THEN SUM(revenue_tax_in) / SUM(transactions) 
            ELSE 0 
        END as avg_transaction_value,
        CASE 
            WHEN SUM(revenue_tax_in) > 0 
            THEN SUM(gross_profit) / SUM(revenue_tax_in) * 100 
            ELSE 0 
        END as gross_margin_pct
    FROM agg_profit_daily
    WHERE date >= ? AND date < ? + INTERVAL 1 DAY
    """
    
    query_start = time.time()
    row = conn.execute(query, [start_date, end_date]).fetchone()
    print(f"[TIMING] query_profit_summary: {time.time() - query_start:.3f}s")
    
    revenue, cogs, gross_profit, quantity, transactions, lines, atv, margin_pct = [
        v or 0 for v in row
    ]

    return {
        'revenue': float(revenue),
        'cogs': float(cogs),
        'gross_profit': float(gross_profit),
        'quantity': float(quantity),
        'transactions': int(transactions),
        'lines': int(lines),
        'avg_transaction_value': float(atv),
        'gross_margin_pct': float(margin_pct)
    }
```

- [ ] **Step 2: Add query_profit_trends function**

```python
def query_profit_trends(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Query profit trends - uses DuckDB aggregates."""
    ensure_duckdb_view_groups({"profit_agg"})
    conn = get_duckdb_connection()
    
    # Generate date series in SQL
    trunc_expr = 'day' if period == 'daily' else 'week' if period == 'weekly' else 'month'
    
    query = f"""
    WITH date_series AS (
        SELECT 
            date_trunc('{trunc_expr}', date) + interval '1 {trunc_expr}' as period_end
        FROM generate_series(
            date_trunc('{trunc_expr}', ?::date)::timestamp,
            date_trunc('{trunc_expr}', ?::date)::timestamp,
            interval '1 {trunc_expr}'
        ) as t(date)
    )
    SELECT 
        ds.period_end as date,
        COALESCE(SUM(a.revenue_tax_in), 0) as revenue,
        COALESCE(SUM(a.cogs_tax_in), 0) as cogs,
        COALESCE(SUM(a.gross_profit), 0) as gross_profit,
        COALESCE(SUM(a.quantity), 0) as items_sold,
        COALESCE(SUM(a.transactions), 0) as transactions,
        COALESCE(SUM(a.lines), 0) as lines,
        CASE 
            WHEN SUM(a.transactions) > 0 
            THEN SUM(a.revenue_tax_in) / SUM(a.transactions) 
            ELSE 0 
        END as avg_transaction_value,
        CASE 
            WHEN SUM(a.revenue_tax_in) > 0 
            THEN SUM(a.gross_profit) / SUM(a.revenue_tax_in) * 100 
            ELSE 0 
        END as gross_margin_pct
    FROM date_series ds
    LEFT JOIN agg_profit_daily a ON 
        a.date >= ds.period_end - INTERVAL '1 {trunc_expr}' AND 
        a.date < ds.period_end
    GROUP BY ds.period_end
    ORDER BY ds.period_end
    """
    
    query_start = time.time()
    result = conn.execute(query, [start_date, end_date]).fetchdf()
    print(f"[TIMING] query_profit_trends: {time.time() - query_start:.3f}s")
    return result
```

- [ ] **Step 3: Add query_profit_by_product function**

```python
def query_profit_by_product(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Query top products by profit - uses DuckDB aggregates."""
    ensure_duckdb_view_groups({"profit_agg", "dims"})
    conn = get_duckdb_connection()
    
    query = """
    WITH product_agg AS (
        SELECT 
            product_id,
            SUM(revenue_tax_in) as total_revenue,
            SUM(cogs_tax_in) as total_cogs,
            SUM(gross_profit) as total_profit,
            SUM(quantity) as total_quantity,
            SUM(lines) as total_lines
        FROM agg_profit_daily_by_product
        WHERE date >= ? AND date < ? + INTERVAL 1 DAY
        GROUP BY product_id
        ORDER BY total_profit DESC
        LIMIT ?
    )
    SELECT 
        s.product_id,
        COALESCE(p.product_name, 'Product ' || s.product_id::VARCHAR) as product_name,
        COALESCE(p.product_category, 'Unknown Category') as category,
        s.total_revenue,
        s.total_cogs,
        s.total_profit,
        s.total_quantity,
        s.total_lines,
        CASE 
            WHEN s.total_revenue > 0 
            THEN s.total_profit / s.total_revenue * 100 
            ELSE 0 
        END as profit_margin_pct
    FROM product_agg s
    LEFT JOIN dim_products p ON s.product_id = p.product_id
    ORDER BY s.total_profit DESC
    """
    
    query_start = time.time()
    result = conn.execute(query, [start_date, end_date, limit]).fetchdf()
    print(f"[TIMING] query_profit_by_product: {time.time() - query_start:.3f}s")
    return result
```

- [ ] **Step 4: Add query_profit_drilldown function**

```python
def query_profit_drilldown(start_date: date, end_date: date, product_id: Optional[int] = None) -> pd.DataFrame:
    """Drill-down to line-level profit details - uses DuckDB fact table."""
    ensure_duckdb_view_groups({"profit_detail"})
    conn = get_duckdb_connection()
    
    if product_id:
        where_clause = "WHERE date >= ? AND date < date(?, '+1 day') AND product_id = ?"
        params = [start_date, end_date, product_id]
    else:
        where_clause = "WHERE date >= ? AND date < date(?, '+1 day')"
        params = [start_date, end_date]

    query = f"""
    SELECT 
        date,
        txn_id,
        line_id,
        product_id,
        quantity,
        revenue_tax_in,
        cost_unit_tax_in,
        cogs_tax_in,
        gross_profit,
        CASE 
            WHEN revenue_tax_in > 0 
            THEN gross_profit * 100.0 / revenue_tax_in
            ELSE 0 
        END as profit_margin_pct
    FROM fact_sales_lines_profit
    {where_clause}
    ORDER BY date, gross_profit DESC
    """
    
    query_start = time.time()
    result = conn.execute(query, params).fetchdf()
    print(f"[TIMING] query_profit_drilldown: {time.time() - query_start:.3f}s")
    return result
```

- [ ] **Step 5: Add query_profit_revenue_by_category function**

```python
def query_profit_revenue_by_category(start_date: date, end_date: date) -> Dict[str, Dict[str, float]]:
    """Query profit revenue by category - uses DuckDB aggregates."""
    ensure_duckdb_view_groups({"profit_agg", "dims"})
    conn = get_duckdb_connection()
    
    query = """
    WITH product_agg AS (
        SELECT 
            a.product_id,
            SUM(a.revenue_tax_in) as revenue_tax_in
        FROM agg_profit_daily_by_product a
        WHERE a.date >= ? AND a.date < ? + INTERVAL 1 DAY
        GROUP BY a.product_id
    )
    SELECT 
        COALESCE(p.product_parent_category, 'Unknown') as parent_cat,
        COALESCE(p.product_category, 'Unknown') as cat,
        SUM(s.revenue_tax_in) as revenue
    FROM product_agg s
    LEFT JOIN dim_products p ON s.product_id = p.product_id
    GROUP BY parent_cat, cat
    ORDER BY revenue DESC
    """
    
    query_start = time.time()
    rows = conn.execute(query, [start_date, end_date]).fetchall()
    print(f"[TIMING] query_profit_revenue_by_category: {time.time() - query_start:.3f}s")

    result = {}
    for parent_cat, cat, revenue in rows:
        result.setdefault(str(parent_cat), {})[str(cat)] = float(revenue or 0)

    return result
```

---

### Task 2: Migrate profit_metrics.py to Use DuckDB

**Files:**
- Modify: `services/profit_metrics.py`

- [ ] **Step 1: Replace SQLite imports with DuckDB imports**

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

- [ ] **Step 2: Replace function implementations with DuckDB calls**

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

---

### Task 3: Migrate sales_metrics.py to Use DuckDB

**Files:**
- Modify: `services/sales_metrics.py`

- [ ] **Step 1: Replace SQLite imports with DuckDB imports**

```python
# Remove: from services.sqlite_manager import SQLiteManager
# Add: from services.duckdb_connector import (
#     query_sales_trends,
#     query_top_products,
#     query_revenue_comparison,
#     query_sales_by_principal
# )
```

- [ ] **Step 2: Replace function implementations with DuckDB calls**

```python
def get_sales_trends_data(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Get revenue trend data using DuckDB aggregates."""
    from services.duckdb_connector import query_sales_trends as duckdb_query_sales_trends
    return duckdb_query_sales_trends(start_date, end_date, period)

def get_revenue_comparison(start_date: date, end_date: date) -> Dict:
    """Compare revenue between periods using DuckDB aggregates."""
    from services.duckdb_connector import query_revenue_comparison as duckdb_query_revenue_comparison
    return duckdb_query_revenue_comparison(start_date, end_date)

def get_top_products(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Get top selling products using DuckDB aggregates."""
    from services.duckdb_connector import query_top_products as duckdb_query_top_products
    return duckdb_query_top_products(start_date, end_date, limit)

def get_sales_by_principal(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Aggregate sales revenue by principal using DuckDB aggregates."""
    from services.duckdb_connector import query_sales_by_principal as duckdb_query_sales_by_principal
    return duckdb_query_sales_by_principal(start_date, end_date, limit)

def get_hourly_sales_pattern(target_date: date) -> pd.DataFrame:
    """Get hourly sales pattern using DuckDB."""
    from services.duckdb_connector import query_hourly_sales_pattern as duckdb_query_hourly_sales_pattern
    return duckdb_query_hourly_sales_pattern(target_date)

def get_hourly_sales_heatmap_data(start_date: date, end_date: date) -> pd.DataFrame:
    """Get hourly sales heatmap data using DuckDB."""
    from services.duckdb_connector import query_hourly_sales_heatmap as duckdb_query_hourly_sales_heatmap
    return duckdb_query_hourly_sales_heatmap(start_date, end_date)
```

---

### Task 4: Migrate overview_metrics.py to Use DuckDB

**Files:**
- Modify: `services/overview_metrics.py`

- [ ] **Step 1: Replace SQLite import with DuckDB import**

```python
# Remove: from services.sqlite_manager import SQLiteManager
# Add: from services.duckdb_connector import query_overview_summary
```

- [ ] **Step 2: Replace function implementation with DuckDB call**

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

---

### Task 5: Remove SQLiteManager from app.py

**Files:**
- Modify: `app.py`

- [ ] **Step 1: Remove SQLiteManager imports and initialization**

```python
# Remove lines 69-70:
# from services.sqlite_manager import SQLiteManager
# manager = SQLiteManager()

# Remove lines 98-99:
# from services.sqlite_manager import SQLiteManager
# manager = SQLiteManager()
```

- [ ] **Step 2: Remove MV diagnostics endpoint**

```python
# Remove lines 76-89 (mv_diagnostics endpoint)
```

- [ ] **Step 3: Update health check to not expect SQLite MVs**

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

---

### Task 6: Remove SQLite MV Refresh from ETL Pipeline

**Files:**
- Modify: `etl_tasks.py`

- [ ] **Step 1: Remove scheduled MV refresh task**

```python
# Remove or comment out lines 2392-2415:
# @app.task
# def refresh_materialized_views_scheduled():
#     """Scheduled MV refresh after ETL completion (runs at 02:30 daily)."""
#     ...
```

- [ ] **Step 2: Remove MV refresh task from beat schedule**

```python
# Remove from app.conf.beat_schedule:
# 'refresh-materialized-views-daily': {
#     'task': 'etl_tasks.refresh_materialized_views_scheduled',
#     'schedule': crontab(hour=2, minute=30),
# },
```

- [ ] **Step 3: Remove MV refresh task from task routing**

```python
# Remove from app.conf.task_routes:
# 'etl_tasks.refresh_materialized_views_scheduled': {'queue': 'loading'},
```

- [ ] **Step 4: Remove refresh_materialized_views task**

```python
# Remove or comment out the entire refresh_materialized_views task (lines 2000-2100)
```

---

### Task 7: Remove MV Refresh UI from Operational Page

**Files:**
- Modify: `pages/operational.py`

- [ ] **Step 1: Remove MV refresh callbacks**

```python
# Remove or comment out:
# refresh_mvs_cascading callback (lines 1600-1650)
# refresh_all_mvs_simple callback (lines 1652-1680)
# refresh_materialized_views callback (lines 1682-1710)
```

- [ ] **Step 2: Remove MV refresh UI elements**

```python
# Remove from layout:
# MV refresh section (buttons, status messages, etc.)
```

- [ ] **Step 3: Remove MV refresh imports**

```python
# Remove: from etl_tasks import refresh_materialized_views
```

---

### Task 8: Test All Dashboard Pages

**Files:**
- Test: Browser testing

- [ ] **Step 1: Restart dash-app**

```bash
docker-compose restart dash-app
docker-compose logs dash-app --tail 50
```

- [ ] **Step 2: Test health check**

```bash
curl http://localhost:8050/health
```

Expected: Returns `{"status": "healthy", "backend": "duckdb"}` with HTTP 200.

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

Open http://localhost:8050/profit in browser (if exists):
- Profit page loads without errors
- Profit trends chart shows data
- Profit summary shows data

- [ ] **Step 6: Test operational page**

Open http://localhost:8050/operational in browser:
- Operational page loads without errors
- ETL controls work
- MV refresh section removed (should not appear)

---

### Task 9: Verify Celery Worker No Longer Refreshes MVs

**Files:**
- Test: Celery logs

- [ ] **Step 1: Check Celery worker logs**

```bash
docker-compose logs celery-worker --tail 100
```

Expected: No MV refresh task execution logs.

- [ ] **Step 2: Verify no SQLite database operations**

```bash
docker-compose exec celery-worker sh -c "ls -la /data-lake/cache/nkdash.sqlite 2>&1 || echo 'SQLite DB not accessed'"
```

Expected: SQLite DB not accessed or doesn't exist.

---

### Task 10: Performance Validation

**Files:**
- Create: `scripts/test_duckdb_performance.py`

- [ ] **Step 1: Create performance test script**

```python
import time
from datetime import date, timedelta
from services.duckdb_connector import (
    query_sales_trends,
    query_profit_summary,
    query_overview_summary,
    query_top_products
)

def test_duckdb_performance():
    """Test DuckDB query performance for yearly date ranges."""
    start_date = date.today() - timedelta(days=365)
    end_date = date.today()
    
    print("=== DuckDB Performance Test ===")
    
    # Test sales trends
    start = time.time()
    result = query_sales_trends(start_date, end_date)
    elapsed = time.time() - start
    print(f"query_sales_trends (365 days): {elapsed:.3f}s, {len(result)} rows")
    
    # Test profit summary
    start = time.time()
    result = query_profit_summary(start_date, end_date)
    elapsed = time.time() - start
    print(f"query_profit_summary (365 days): {elapsed:.3f}s")
    
    # Test overview summary
    start = time.time()
    result = query_overview_summary(start_date, end_date)
    elapsed = time.time() - start
    print(f"query_overview_summary (365 days): {elapsed:.3f}s")
    
    # Test top products
    start = time.time()
    result = query_top_products(start_date, end_date)
    elapsed = time.time() - start
    print(f"query_top_products (365 days): {elapsed:.3f}s, {len(result)} rows")
    
    print("=== Test Complete ===")

if __name__ == "__main__":
    test_duckdb_performance()
```

- [ ] **Step 2: Run performance test**

```bash
docker-compose exec dash-app python scripts/test_duckdb_performance.py
```

Expected: All queries complete in < 1 second.

---

## Self-Review

**1. Spec coverage:**
- Add missing profit query functions to DuckDB ✓
- Migrate profit_metrics.py to DuckDB ✓
- Migrate sales_metrics.py to DuckDB ✓
- Migrate overview_metrics.py to DuckDB ✓
- Remove SQLiteManager from app.py ✓
- Remove SQLite MV refresh from ETL ✓
- Remove MV refresh UI from operational page ✓
- Update health check ✓
- Test all pages ✓
- Performance validation ✓

**2. Placeholder scan:**
- No TBD, TODO, or placeholder text found
- All steps include specific code and commands

**3. Architecture validation:**
- DuckDB for ETL (unchanged) ✓
- DuckDB for queries (restored) ✓
- SQLite removed from query layer ✓
- Clear boundaries: DuckDB=ETL+queries ✓

---

## Expected Outcomes

### After Migration
- SQLite MV layer completely removed
- All dashboard queries use DuckDB aggregates
- Health check verifies DuckDB views
- ETL pipeline no longer refreshes SQLite MVs
- Operational page MV refresh UI removed
- Sub-second query performance maintained

### Architecture
- DuckDB: ETL extraction + parquet creation + query layer
- SQLite: Removed from query layer
- Parquet: Columnar storage with Hive partitioning
- Redis: Caching layer (optional enhancement)

### Performance
- 30-day queries: < 0.1s (same as before)
- 365-day queries: < 0.5s (same as before)
- No SQLite overhead
- Direct parquet access

---

## Rollback Plan

If issues arise, rollback steps:

1. Restore SQLiteManager initialization to app.py
2. Restore SQLite query functions in profit_metrics.py and sales_metrics.py
3. Restore MV refresh task to etl_tasks.py
4. Restore MV refresh UI to operational page
5. Restart services
