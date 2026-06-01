# SQLite MV Fix - Corrected Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix SQLite MV initialization and refresh so dashboard queries work correctly.

**Architecture:** DuckDB for ETL (extraction, parquet creation) + SQLite for user-facing queries (MV storage). This is the CORRECT architecture - query functions are already migrated to SQLite.

**Tech Stack:** SQLite, Polars (parquet reads), Python, Celery

---

## Critical Finding: System is Already SQLite-First

### Audit Results
- **profit_metrics.py**: ALL 5 functions use SQLiteManager (mv_profit_daily, mv_fact_sales_lines_profit)
- **sales_metrics.py**: ALL 7 functions use SQLiteManager (mv_sales_daily, mv_sales_by_product, mv_sales_by_principal)
- **app.py**: Initializes SQLiteManager twice on startup, health check expects 5+ SQLite MVs
- **etl_tasks.py**: Scheduled MV refresh task uses SQLiteManager
- **DuckDB aggregates exist but NOTHING uses them** for dashboard queries

### Previous Plan Was Wrong
Removing SQLite MVs would:
1. Break app startup (SQLiteManager import fails)
2. Break health check (Docker marks container unhealthy)
3. Break all dashboard pages (every query function fails)
4. Break ETL pipeline (MV refresh task crashes)
5. Break diagnostics endpoint

### Correct Approach
The architecture is already correct (DuckDB=ETL, SQLite=queries). The problem is:
- **SQLite MV tables don't exist in the database**
- MV refresh logic needs to be fixed to create/populate MVs

---

## Root Cause Analysis

### Current State
- SQLite database at `/data-lake/nkdash.db` has NO MV tables (MV Tables: [])
- Parquet aggregates exist (agg_sales_daily, agg_profit_daily, etc.)
- Query functions expect SQLite MVs to exist
- MV refresh task exists but MVs not being created

### Why MVs Don't Exist
Possible causes:
1. SQLiteManager.initialize_db() not called on app startup
2. MV refresh logic failing silently
3. Database path mismatch between components
4. MV tables dropped during failed refresh
5. initialize_db() doesn't create MV tables (only creates schema)

---

## Implementation Plan

### Task 1: Fix SQLite MV Initialization on App Startup

**Files:**
- Modify: `app.py`
- Modify: `services/sqlite_manager.py`

- [ ] **Step 1: Add MV initialization to app startup**

Modify `app.py` to ensure MVs are initialized:

```python
# After line 99 (after second SQLiteManager initialization)
from services.sqlite_manager import SQLiteManager

# Initialize SQLite database and MVs on startup
try:
    manager = SQLiteManager()
    manager.initialize_db()
    print("[APP] SQLite database and MVs initialized successfully")
except Exception as e:
    print(f"[APP] ERROR: Failed to initialize SQLite database: {e}")
    import traceback
    traceback.print_exc()
```

- [ ] **Step 2: Verify initialize_db() creates MV tables**

Check `services/sqlite_manager.py` initialize_db() method:

```python
def initialize_db(self) -> None:
    """Initialize SQLite database and create MV tables if they don't exist."""
    with self.get_writer_conn() as conn:
        # Create MV tables if they don't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mv_sales_daily (
                date TEXT PRIMARY KEY,
                revenue REAL,
                transactions INTEGER,
                items_sold REAL,
                lines INTEGER
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mv_sales_by_product (
                date TEXT,
                product_id INTEGER,
                revenue REAL,
                quantity REAL,
                lines INTEGER,
                PRIMARY KEY (date, product_id)
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mv_sales_by_principal (
                date TEXT,
                principal TEXT,
                revenue REAL,
                quantity REAL,
                lines INTEGER,
                PRIMARY KEY (date, principal)
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mv_profit_daily (
                date TEXT PRIMARY KEY,
                revenue_tax_in REAL,
                cogs_tax_in REAL,
                gross_profit REAL,
                quantity REAL,
                transactions INTEGER,
                lines INTEGER
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mv_fact_sales_lines_profit (
                date TEXT,
                txn_id INTEGER,
                line_id INTEGER,
                product_id INTEGER,
                quantity REAL,
                revenue_tax_in REAL,
                cost_unit_tax_in REAL,
                cogs_tax_in REAL,
                gross_profit REAL,
                profit_margin_pct REAL,
                PRIMARY KEY (date, txn_id, line_id)
            )
        """)
        
        print("[SQLITE] MV tables created/verified")
```

If initialize_db() doesn't create MV tables, add the CREATE TABLE statements.

- [ ] **Step 3: Restart dash-app and verify MV tables exist**

```bash
docker-compose restart dash-app
docker-compose logs dash-app --tail 50
```

Check if MV tables are created:

```bash
docker-compose exec dash-app python -c "import sqlite3; conn = sqlite3.connect('/data-lake/nkdash.db'); cursor = conn.cursor(); cursor.execute('SELECT name FROM sqlite_master WHERE type=\"table\" AND name LIKE \"mv_%\"'); print('MV Tables:', cursor.fetchall()); conn.close()"
```

Expected: Should show 5 MV tables (mv_sales_daily, mv_sales_by_product, mv_sales_by_principal, mv_profit_daily, mv_fact_sales_lines_profit).

---

### Task 2: Debug MV Refresh Logic

**Files:**
- Modify: `services/sqlite_manager.py`
- Test: Manual MV refresh

- [ ] **Step 1: Check if refresh_mv method is working**

Create test script `scripts/test_mv_refresh_manual.py`:

```python
import sys
sys.path.insert(0, '/app')

from services.sqlite_manager import SQLiteManager
from datetime import date

manager = SQLiteManager()
manager.initialize_db()

# Test refresh for a single date
test_date = date(2026, 5, 28)
conn = manager.get_writer_conn()

print(f"Testing MV refresh for {test_date}")

# Test sales daily refresh
result = manager.refresh_mv('mv_sales_daily', 'sales', conn, date_range=(str(test_date), str(test_date)))
print(f"mv_sales_daily refresh: success={result.success}, rows={result.rows_affected}, error={result.error_message}")

# Test profit daily refresh
result = manager.refresh_mv('mv_profit_daily', 'profit', conn, date_range=(str(test_date), str(test_date)))
print(f"mv_profit_daily refresh: success={result.success}, rows={result.rows_affected}, error={result.error_message}")

conn.close()

# Check if data was inserted
with manager.reader_conn() as conn:
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM mv_sales_daily')
    count = cursor.fetchone()[0]
    print(f"mv_sales_daily row count: {count}")
    
    cursor.execute('SELECT COUNT(*) FROM mv_profit_daily')
    count = cursor.fetchone()[0]
    print(f"mv_profit_daily row count: {count}")
```

- [ ] **Step 2: Run test script**

```bash
docker-compose exec dash-app python scripts/test_mv_refresh_manual.py
```

Expected: MV refresh succeeds and data is inserted.

- [ ] **Step 3: If refresh fails, check parquet files exist**

```bash
docker-compose exec dash-app sh -c "ls -la /data-lake/star-schema/agg_sales_daily/year=2026/month=05/day=28/"
docker-compose exec dash-app sh -c "ls -la /data-lake/star-schema/agg_profit_daily/year=2026/month=05/day=28/"
```

Expected: Parquet files should exist for the test date.

---

### Task 3: Fix MV Refresh Logic Issues

**Files:**
- Modify: `services/sqlite_manager.py`

- [ ] **Step 1: Check if _incremental_refresh is working correctly**

Review the `_incremental_refresh` method in `services/sqlite_manager.py`:

```python
def _incremental_refresh(self, table_name: str, df: pd.DataFrame, conn: sqlite3.Connection) -> RefreshResult:
    """Incremental refresh - insert or update rows."""
    try:
        # Convert date column to string format for SQLite
        if 'date' in df.columns:
            df['date'] = df['date'].astype(str)
        
        # Use INSERT OR REPLACE for upsert
        df.to_sql(
            table_name,
            conn,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        return RefreshResult(success=True, rows_affected=len(df))
    except Exception as e:
        return RefreshResult(success=False, error_message=str(e))
```

- [ ] **Step 2: Check if domain refresh functions are calling the right strategy**

Review domain-specific refresh functions (_refresh_sales_daily, _refresh_profit_daily, etc.):

```python
def _refresh_sales_daily(self, conn: sqlite3.Connection, date_range: tuple = None) -> RefreshResult:
    """Refresh mv_sales_daily from parquet aggregates."""
    try:
        # Read parquet data
        data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
        agg_path = f"{data_lake}/star-schema/agg_sales_daily"
        
        # Filter by date range if provided
        if date_range:
            start_date, end_date = date_range
            df = pl.scan_parquet(f"{agg_path}/**/*.parquet").filter(
                (pl.col('date') >= pl.lit(start_date).cast(pl.Date)) &
                (pl.col('date') <= pl.lit(end_date).cast(pl.Date))
            ).collect()
        else:
            df = pl.scan_parquet(f"{agg_path}/**/*.parquet").collect()
        
        # Convert to pandas
        df_pandas = df.to_pandas()
        
        # Use incremental refresh
        return self._incremental_refresh('mv_sales_daily', df_pandas, conn)
    except Exception as e:
        return RefreshResult(success=False, error_message=str(e))
```

- [ ] **Step 3: Fix any issues found**

Common issues to fix:
- Date format mismatches (ensure dates are strings for SQLite)
- Column name mismatches (ensure parquet columns match MV table schema)
- Missing columns (add default values if needed)
- Primary key conflicts (use INSERT OR REPLACE)

---

### Task 4: Trigger Full MV Refresh for Existing Data

**Files:**
- Modify: None
- Test: Manual MV refresh

- [ ] **Step 1: Trigger MV refresh for available date range**

Create script `scripts/trigger_full_mv_refresh.py`:

```python
import sys
sys.path.insert(0, '/app')

from services.sqlite_manager import SQLiteManager
from datetime import date

manager = SQLiteManager()
manager.initialize_db()

# Refresh for available data range (e.g., May 2026)
start_date = '2026-05-01'
end_date = '2026-05-28'

print(f"Triggering full MV refresh for {start_date} to {end_date}")

conn = manager.get_writer_conn()

# Refresh all sales MVs
sales_views = [
    ('mv_sales_daily', 'sales'),
    ('mv_sales_by_product', 'sales'),
    ('mv_sales_by_principal', 'sales')
]

for view_name, domain in sales_views:
    print(f"Refreshing {view_name}...")
    result = manager.refresh_mv(view_name, domain, conn, date_range=(start_date, end_date))
    print(f"  Success: {result.success}, Rows: {result.rows_affected}, Error: {result.error_message}")

# Refresh profit MVs
profit_views = [
    ('mv_profit_daily', 'profit')
]

for view_name, domain in profit_views:
    print(f"Refreshing {view_name}...")
    result = manager.refresh_mv(view_name, domain, conn, date_range=(start_date, end_date))
    print(f"  Success: {result.success}, Rows: {result.rows_affected}, Error: {result.error_message}")

conn.close()

# Verify data
with manager.reader_conn() as conn:
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*), MIN(date), MAX(date) FROM mv_sales_daily')
    result = cursor.fetchone()
    print(f"mv_sales_daily: {result[0]} rows, {result[1]} to {result[2]}")
    
    cursor.execute('SELECT COUNT(*), MIN(date), MAX(date) FROM mv_profit_daily')
    result = cursor.fetchone()
    print(f"mv_profit_daily: {result[0]} rows, {result[1]} to {result[2]}")
```

- [ ] **Step 2: Run full refresh script**

```bash
docker-compose exec dash-app python scripts/trigger_full_mv_refresh.py
```

Expected: All MVs populated with data for the date range.

---

### Task 5: Verify Health Check and Dashboard Pages

**Files:**
- Test: Health check endpoint
- Test: Dashboard pages

- [ ] **Step 1: Test health check endpoint**

```bash
curl http://localhost:8050/health
```

Expected: Returns `{"status": "healthy", "mvs_loaded": 5}` with HTTP 200.

- [ ] **Step 2: Test MV diagnostics endpoint**

```bash
curl http://localhost:8050/api/mv-diagnostics
```

Expected: Returns MV table metadata (row counts, date ranges).

- [ ] **Step 3: Test overview page in browser**

Open http://localhost:8050/ in browser and check:
- Overview page loads without errors
- KPI cards show data
- Charts display data

- [ ] **Step 4: Test sales page in browser**

Open http://localhost:8050/sales in browser and check:
- Sales page loads without errors
- Revenue trends chart shows data
- Top products table shows data

- [ ] **Step 5: Check browser console for errors**

Open browser developer tools and check console for JavaScript errors.

Expected: No errors related to data fetching or queries.

---

### Task 6: Ensure Scheduled MV Refresh Works

**Files:**
- Modify: `etl_tasks.py` (if needed)
- Test: Celery task execution

- [ ] **Step 1: Verify scheduled MV refresh task exists**

Check `etl_tasks.py` for `refresh_materialized_views_scheduled` task:

```python
@app.task
def refresh_materialized_views_scheduled():
    """Scheduled MV refresh after ETL completion (runs at 02:30 daily)."""
    from datetime import date, timedelta

    yesterday = date.today() - timedelta(days=1)
    start_date = yesterday.isoformat()
    end_date = yesterday.isoformat()

    logger.info(f"Starting scheduled MV refresh for {start_date}")

    try:
        result = refresh_materialized_views.delay(start_date, end_date)
        logger.info(f"Scheduled MV refresh task queued: {result.id}")
        return {
            'status': 'queued',
            'task_id': result.id,
            'date': start_date
        }
    except Exception as exc:
        logger.error(f"Scheduled MV refresh failed: {exc}", exc_info=True)
        raise
```

- [ ] **Step 2: Check Celery beat schedule**

Verify the task is scheduled in Celery beat configuration:

```python
app.conf.beat_schedule = {
    'refresh-materialized-views-daily': {
        'task': 'etl_tasks.refresh_materialized_views_scheduled',
        'schedule': crontab(hour=2, minute=30),
    },
}
```

- [ ] **Step 3: Test manual MV refresh trigger**

From operational page, trigger MV refresh for a specific date range and verify it succeeds.

---

### Task 7: Document Fix and Add Monitoring

**Files:**
- Create: `docs/troubleshooting/sqlite-mv-initialization-fix.md`
- Modify: `README.md` (update architecture section)

- [ ] **Step 1: Document the fix**

Create documentation explaining:
- Why MVs didn't exist
- How they were fixed
- How to prevent recurrence
- Architecture boundaries (DuckDB=ETL, SQLite=queries)

- [ ] **Step 2: Add MV health monitoring**

Add monitoring to check MV health regularly:

```python
# Add to app.py or separate monitoring script
def check_mv_health():
    """Check MV tables exist and have recent data."""
    from services.sqlite_manager import SQLiteManager
    from datetime import date, timedelta
    
    manager = SQLiteManager()
    
    with manager.reader_conn() as conn:
        cursor = conn.cursor()
        
        # Check MV tables exist
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'mv_%'")
        mv_count = cursor.fetchone()[0]
        
        if mv_count < 5:
            print(f"[HEALTH] ERROR: Only {mv_count} MV tables found (expected 5+)")
            return False
        
        # Check MVs have recent data
        yesterday = date.today() - timedelta(days=1)
        cursor.execute(f"SELECT COUNT(*) FROM mv_sales_daily WHERE date >= '{yesterday}'")
        recent_count = cursor.fetchone()[0]
        
        if recent_count == 0:
            print(f"[HEALTH] WARNING: No recent data in mv_sales_daily")
            return False
        
        print(f"[HEALTH] MVs healthy: {mv_count} tables, {recent_count} recent rows")
        return True
```

---

## Self-Review

**1. Spec coverage:**
- Fix SQLite MV initialization ✓
- Debug MV refresh logic ✓
- Trigger full MV refresh ✓
- Verify health check and dashboard ✓
- Ensure scheduled MV refresh works ✓
- Document fix and add monitoring ✓

**2. Placeholder scan:**
- No TBD, TODO, or placeholder text found
- All steps include specific code and commands

**3. Architecture validation:**
- Maintains correct architecture (DuckDB=ETL, SQLite=queries) ✓
- Doesn't break existing query functions ✓
- Fixes the actual problem (MVs don't exist) ✓
- Adds monitoring to prevent recurrence ✓

---

## Expected Outcomes

### After Fix
- SQLite MV tables exist in database
- Health check passes (HTTP 200, 5+ MVs loaded)
- Dashboard pages display data correctly
- Scheduled MV refresh works automatically
- MVs populated with existing parquet data

### Architecture
- DuckDB: ETL extraction and parquet creation (unchanged)
- SQLite: User-facing queries via MVs (unchanged)
- Clear boundaries: DuckDB=ETL, SQLite=queries (clarified)

### Monitoring
- Health check endpoint validates MV existence
- Diagnostics endpoint provides MV metadata
- MV health monitoring script added
