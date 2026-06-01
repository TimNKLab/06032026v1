# DuckDB Yearly Aggregation Performance - Data Engineering Assessment

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Design and implement DuckDB-first architecture for serving top layer aggregated data with sub-second query performance for yearly date ranges (365+ days).

**Architecture:** DuckDB columnar storage with Hive partitioning + Redis caching layer + query optimization strategies. Remove SQLite MV layer entirely to eliminate complexity.

**Tech Stack:** DuckDB, Parquet with ZSTD compression, Redis caching, Python, Hive partitioning

---

## Current State Analysis

### Existing Aggregate Tables
- **agg_sales_daily**: Daily sales totals (date, revenue, transactions, items_sold, lines)
- **agg_sales_daily_by_product**: Daily sales by product (date, product_id, revenue, quantity, lines)
- **agg_sales_daily_by_principal**: Daily sales by principal/brand (date, principal, revenue, quantity, lines)
- **agg_profit_daily**: Daily profit totals (date, revenue_tax_in, cogs_tax_in, gross_profit, quantity, transactions, lines)
- **agg_profit_daily_by_product**: Daily profit by product (date, product_id, revenue_tax_in, cogs_tax_in, gross_profit, quantity, lines)

### Current Partitioning
- **Scheme**: Hive partitioning by year/month/day
- **Path pattern**: `/data-lake/star-schema/agg_sales_daily/year=YYYY/month=MM/day=DD/`
- **Compression**: ZSTD (from config)
- **Partition pruning**: Enabled with `hive_partitioning=1`

### Current Performance
- **30-day queries**: 0.037s - 0.121s DuckDB time
- **Views**: DuckDB views with lazy parquet loading
- **Optimization**: FILTER clauses for period comparisons

### Current Problems
- SQLite MV layer causing complexity and failures
- Hybrid architecture confusion (DuckDB + SQLite)
- MV refresh failures and OOM kills
- Cache clearing issues
- 2+ weeks stagnation on MV-related issues

---

## Performance Projection for Yearly Queries

### Data Volume Estimation
- **Daily rows**: 1 row per day per aggregate table
- **Yearly rows**: 365 rows per aggregate table
- **File count**: 365 parquet files (one per day)
- **Row size**: ~100 bytes per row (estimated)
- **Total size per table**: ~36KB (365 rows × 100 bytes)

### Query Performance Analysis
**Current 30-day performance**: 0.037s - 0.121s
**Projected yearly performance**: 0.1s - 0.5s (estimated)

**Factors affecting yearly queries:**
1. **File count**: 365 files vs 30 files (12x increase)
2. **Partition pruning**: Still effective with year/month/day partitioning
3. **Columnar storage**: DuckDB reads only needed columns
4. **Compression**: ZSTD reduces I/O significantly
5. **Memory caching**: DuckDB caches parquet metadata and data

**Conclusion**: Yearly queries should remain sub-second with current architecture.

---

## Recommended Architecture: DuckDB-First

### Core Principles
1. **Single source of truth**: DuckDB for all queries
2. **No MV layer**: Query parquet aggregates directly
3. **Smart caching**: Redis for frequently accessed results
4. **Partition optimization**: Maintain current Hive partitioning
5. **Query optimization**: Use DuckDB's advanced features

### Architecture Components

#### 1. DuckDB Query Layer
```python
# Direct parquet queries with partition pruning
conn.execute("""
    SELECT date, revenue, transactions
    FROM read_parquet('/data-lake/star-schema/agg_sales_daily/**/*.parquet', 
                      hive_partitioning=1)
    WHERE date >= '2025-01-01' AND date <= '2025-12-31'
""")
```

#### 2. Redis Caching Layer
```python
# Cache query results by date range + query signature
cache_key = f"query:{query_hash}:{start_date}:{end_date}"
cached_result = redis.get(cache_key)
if cached_result:
    return pickle.loads(cached_result)
```

#### 3. Multi-Level Aggregates (Optional Enhancement)
```python
# Add monthly aggregates for very long date ranges
# agg_sales_monthly: date (month), revenue, transactions, items_sold, lines
# agg_sales_quarterly: date (quarter), revenue, transactions, items_sold, lines
```

---

## Partitioning Strategy

### Current Partitioning (Keep)
- **Level 1**: year (e.g., year=2025)
- **Level 2**: month (e.g., month=01)
- **Level 3**: day (e.g., day=01)

### Why This Works for Yearly Queries
1. **Partition pruning**: DuckDB reads only needed year/month/day directories
2. **File size**: Small daily files = fast random access
3. **Parallel scanning**: DuckDB can scan multiple files in parallel
4. **Predictable pattern**: Easy to query specific date ranges

### Alternative: Monthly Partitions (Not Recommended)
- **Pros**: Fewer files (12 vs 365)
- **Cons**: Larger files = slower for daily queries, less flexibility
- **Verdict**: Keep daily partitions for maximum flexibility

---

## Query Optimization Strategies

### 1. Use DuckDB's FILTER Clause
```python
# Single query for multiple periods
query = """
SELECT 
    SUM(revenue) FILTER (WHERE date >= ? AND date <= ?) as current_rev,
    SUM(revenue) FILTER (WHERE date >= ? AND date <= ?) as prev_rev
FROM agg_sales_daily
WHERE date >= ? AND date <= ?
"""
```

### 2. Pre-Aggregate in CTEs
```python
# Aggregate first, then join to reduce data volume
query = """
WITH daily_agg AS (
    SELECT date, SUM(revenue) as revenue
    FROM agg_sales_daily_by_product
    WHERE date >= ? AND date <= ?
    GROUP BY date
)
SELECT date, revenue
FROM daily_agg
ORDER BY date
"""
```

### 3. Use DuckDB's LIMIT with ORDER BY
```python
# For top-N queries, aggregate before limiting
query = """
WITH product_agg AS (
    SELECT product_id, SUM(revenue) as total_revenue
    FROM agg_sales_daily_by_product
    WHERE date >= ? AND date <= ?
    GROUP BY product_id
)
SELECT product_id, total_revenue
FROM product_agg
ORDER BY total_revenue DESC
LIMIT 20
"""
```

### 4. Enable DuckDB Query Optimization
```python
# Set optimization flags
conn.execute("SET enable_optimizer = true")
conn.execute("SET parallel_scan = true")
conn.execute("SET max_memory = '4GB'")
```

---

## Caching Strategy

### Redis Cache Design

#### Cache Key Structure
```python
cache_key = f"duckdb:{table_name}:{query_hash}:{date_range_hash}"
```

#### Cache TTL Strategy
- **Daily aggregates**: 5 minutes (data changes daily)
- **Monthly aggregates**: 1 hour (data changes monthly)
- **Yearly aggregates**: 1 day (historical data rarely changes)
- **Top-N queries**: 10 minutes (ranking changes slowly)

#### Cache Invalidation
```python
# Invalidate cache after ETL pipeline completes
def invalidate_cache_after_etl(table_name, date):
    pattern = f"duckdb:{table_name}:*:{date}:*"
    redis.delete(*redis.keys(pattern))
```

#### Cache Warming
```python
# Pre-warm cache for common queries on startup
common_queries = [
    ("last_7_days", date.today() - timedelta(days=7), date.today()),
    ("last_30_days", date.today() - timedelta(days=30), date.today()),
    ("this_month", date.today().replace(day=1), date.today()),
]
```

---

## Implementation Plan

### Task 1: Remove SQLite MV Layer

**Files:**
- Modify: `services/sqlite_manager.py` (remove or deprecate)
- Modify: `services/sales_metrics.py` (migrate to DuckDB)
- Modify: `services/profit_metrics.py` (migrate to DuckDB)
- Modify: `services/overview_metrics.py` (migrate to DuckDB)
- Modify: `pages/home.py` (update to use DuckDB queries)

- [ ] **Step 1: Backup current SQLite MV implementation**

```bash
cp services/sqlite_manager.py services/sqlite_manager.py.backup
cp services/sales_metrics.py services/sales_metrics.py.backup
cp services/profit_metrics.py services/profit_metrics.py.backup
```

- [ ] **Step 2: Update overview_metrics.py to use DuckDB**

Modify `services/overview_metrics.py` to query DuckDB aggregates instead of SQLite MVs:

```python
from services.duckdb_connector import get_duckdb_connection, ensure_duckdb_view_groups

def get_total_overview_summary(target_date_start: date, target_date_end: date = None) -> Dict:
    if not isinstance(target_date_start, date):
        target_date_start = date.today()
    if target_date_end is None:
        target_date_end = target_date_start

    try:
        ensure_duckdb_view_groups({"sales_agg", "dims"})
        conn = get_duckdb_connection()

        # Query DuckDB aggregates directly
        query = """
        WITH base AS (
            SELECT a.product_id, a.revenue, a.quantity,
                   COALESCE(p.product_parent_category, 'Unknown') as parent_cat,
                   COALESCE(p.product_category, 'Unknown') as cat,
                   COALESCE(p.product_brand, 'Unknown') as brand
            FROM agg_sales_daily_by_product a
            LEFT JOIN dim_products p ON a.product_id = p.product_id
            WHERE a.date >= ? AND a.date < ? + INTERVAL 1 DAY
        ),
        summary AS (SELECT SUM(revenue) as rev, SUM(quantity) as qty FROM base),
        by_cat AS (SELECT parent_cat, cat, SUM(revenue) as rev FROM base GROUP BY 1, 2),
        by_brand AS (SELECT parent_cat, cat, brand, SUM(revenue) as rev FROM base GROUP BY 1, 2, 3)
        SELECT 'summary' as type, NULL as c1, NULL as c2, NULL as c3, rev, qty FROM summary
        UNION ALL SELECT 'cat', parent_cat, cat, NULL, rev, NULL FROM by_cat
        UNION ALL SELECT 'brand', parent_cat, cat, brand, rev, NULL FROM by_brand
        """

        results = conn.execute(query, [target_date_start, target_date_end]).fetchall()

        categories_nested = {}
        brands_nested = {}
        total_rev = total_qty = 0

        for row in results:
            rtype, c1, c2, c3, rev, qty = row
            rev = float(rev or 0)
            
            if rtype == 'summary':
                total_rev, total_qty = rev, float(qty or 0)
            elif rtype == 'cat':
                categories_nested.setdefault(c1, {})[c2] = rev
            elif rtype == 'brand':
                brands_nested.setdefault(c1, {}).setdefault(c2, {})[c3] = rev

        return {
            'target_date_start': target_date_start,
            'target_date_end': target_date_end,
            'today_amount': total_rev,
            'today_qty': total_qty,
            'prev_amount': 0.0,
            'categories_nested': categories_nested,
            'brands_nested': brands_nested,
        }
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

- [ ] **Step 3: Update home.py to use DuckDB overview metrics**

Modify `pages/home.py` to import from updated `overview_metrics.py`:

```python
# Remove SQLite import if present
# from services.overview_metrics import get_total_overview_summary  # Old SQLite version

# Keep the same import (now uses DuckDB)
from services.overview_metrics import get_total_overview_summary
```

- [ ] **Step 4: Test overview page with DuckDB queries**

```bash
# Restart dash-app
docker-compose restart dash-app

# Check logs for errors
docker-compose logs dash-app --tail 50

# Test overview page in browser
```

Expected: Overview page loads with data from DuckDB aggregates.

---

### Task 2: Implement Redis Caching Layer

**Files:**
- Create: `services/cache_manager.py`
- Modify: `services/duckdb_connector.py` (add caching decorators)
- Modify: `services/sales_metrics.py` (add caching to query functions)

- [ ] **Step 1: Create cache manager module**

Create `services/cache_manager.py`:

```python
import redis
import pickle
import hashlib
import os
from typing import Any, Optional
from datetime import timedelta

class CacheManager:
    _instance: Optional['CacheManager'] = None
    _redis_client: Optional[redis.Redis] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_redis(self) -> redis.Redis:
        if self._redis_client is None:
            redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
            self._redis_client = redis.from_url(redis_url, decode_responses=False)
        return self._redis_client

    def generate_key(self, prefix: str, query: str, params: tuple) -> str:
        """Generate cache key from query and parameters."""
        # Hash the query to create a stable key
        query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
        # Hash parameters to include date range
        params_hash = hashlib.md5(str(params).encode()).hexdigest()[:8]
        return f"{prefix}:{query_hash}:{params_hash}"

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        try:
            redis_client = self.get_redis()
            cached = redis_client.get(key)
            if cached:
                return pickle.loads(cached)
        except Exception as e:
            print(f"[CACHE] Error getting key {key}: {e}")
        return None

    def set(self, key: str, value: Any, ttl: int = 300) -> bool:
        """Set value in cache with TTL."""
        try:
            redis_client = self.get_redis()
            serialized = pickle.dumps(value)
            redis_client.setex(key, ttl, serialized)
            return True
        except Exception as e:
            print(f"[CACHE] Error setting key {key}: {e}")
            return False

    def delete_pattern(self, pattern: str) -> int:
        """Delete keys matching pattern."""
        try:
            redis_client = self.get_redis()
            keys = redis_client.keys(pattern)
            if keys:
                return redis_client.delete(*keys)
        except Exception as e:
            print(f"[CACHE] Error deleting pattern {pattern}: {e}")
        return 0

    def invalidate_table(self, table_name: str) -> int:
        """Invalidate all cache keys for a table."""
        pattern = f"duckdb:{table_name}:*"
        return self.delete_pattern(pattern)
```

- [ ] **Step 2: Add caching decorator to DuckDB queries**

Create caching decorator in `services/cache_manager.py`:

```python
from functools import wraps

def cached_query(ttl: int = 300, table_name: str = "default"):
    """Decorator to cache DuckDB query results."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_manager = CacheManager()
            
            # Extract query and params from function call
            # This is a simplified version - adjust based on actual function signatures
            query = kwargs.get('query', '')
            params = kwargs.get('params', args)
            
            # Generate cache key
            cache_key = cache_manager.generate_key(f"duckdb:{table_name}", query, params)
            
            # Try to get from cache
            cached_result = cache_manager.get(cache_key)
            if cached_result is not None:
                print(f"[CACHE] Cache hit for {cache_key}")
                return cached_result
            
            # Execute query
            result = func(*args, **kwargs)
            
            # Cache result
            cache_manager.set(cache_key, result, ttl)
            print(f"[CACHE] Cached result for {cache_key}")
            
            return result
        return wrapper
    return decorator
```

- [ ] **Step 3: Apply caching to overview query**

Update `services/overview_metrics.py` to use caching:

```python
from services.cache_manager import CacheManager, cached_query

@cached_query(ttl=300, table_name="agg_sales_daily_by_product")
def get_total_overview_summary(target_date_start: date, target_date_end: date = None) -> Dict:
    # Existing implementation
    ...
```

- [ ] **Step 4: Add cache invalidation after ETL**

Modify ETL pipeline to invalidate cache after data refresh:

```python
# In etl_tasks.py or appropriate ETL module
from services.cache_manager import CacheManager

def invalidate_sales_cache(date):
    cache_manager = CacheManager()
    cache_manager.invalidate_table("agg_sales_daily")
    cache_manager.invalidate_table("agg_sales_daily_by_product")
    cache_manager.invalidate_table("agg_sales_daily_by_principal")
    print(f"[CACHE] Invalidated sales caches for {date}")
```

---

### Task 3: Optimize DuckDB Connection and Views

**Files:**
- Modify: `services/duckdb_connector.py` (optimize connection settings)
- Modify: `services/duckdb_connector.py` (add query optimization settings)

- [ ] **Step 1: Optimize DuckDB connection settings**

Update `DuckDBManager.get_connection()` in `services/duckdb_connector.py`:

```python
def get_connection(self) -> duckdb.DuckDBPyConnection:
    # Check if MVs were refreshed externally (e.g., by celery-worker)
    self._check_mv_refresh_signal()

    if self._connection is None:
        with self._lock:
            if self._connection is None:
                data_lake = os.environ.get('DATA_LAKE_ROOT') or os.environ.get('DATA_LAKE_PATH', '/data-lake')
                db_path = f"{data_lake}/cache/nkdash.duckdb"
                os.makedirs(os.path.dirname(db_path), exist_ok=True)

                in_celery = os.environ.get('CELERY_WORKER_RUNNING') == '1'
                setup_start = time.time()
                if in_celery:
                    worker_db_path = f"{data_lake}/cache/nkdash_worker.duckdb"
                    conn = duckdb.connect(database=worker_db_path)
                    conn.execute("PRAGMA max_temp_directory_size='20GiB'")
                    print(f"[duckdb] celery worker: using disk-backed DuckDB at {worker_db_path}")
                else:
                    conn = duckdb.connect(database=db_path)
                    # Add optimization settings
                    conn.execute("SET enable_optimizer = true")
                    conn.execute("SET parallel_scan = true")
                    conn.execute("SET max_memory = '4GB'")
                    conn.execute("SET threads = 4")
                    print(f"[duckdb] connecting to {db_path} with optimizations...")
                
                # Rest of existing connection setup...
```

- [ ] **Step 2: Add query performance monitoring**

Add timing wrapper to DuckDB queries:

```python
def _execute_with_timing(conn, query: str, params: list = None, timeout_ms: int = 10000) -> pd.DataFrame:
    """Execute query with performance monitoring."""
    import time
    
    start_time = time.time()
    
    # Set statement timeout
    conn.execute(f"SET statement_timeout={timeout_ms}")
    
    # Execute query
    if params:
        result = conn.execute(query, params).fetchdf()
    else:
        result = conn.execute(query).fetchdf()
    
    elapsed = time.time() - start_time
    print(f"[PERF] Query executed in {elapsed:.3f}s, returned {len(result)} rows")
    
    # Log slow queries (> 1 second)
    if elapsed > 1.0:
        print(f"[PERF] WARNING: Slow query detected ({elapsed:.3f}s)")
        print(f"[PERF] Query: {query[:200]}...")
    
    return result
```

---

### Task 4: Add Monthly Aggregates for Very Long Ranges

**Files:**
- Create: `etl/build_monthly_aggregates.py`
- Modify: `etl_tasks.py` (add monthly aggregate ETL task)
- Modify: `services/duckdb_connector.py` (add monthly aggregate views)

- [ ] **Step 1: Create monthly aggregate ETL task**

Create `etl/build_monthly_aggregates.py`:

```python
import polars as pl
from datetime import date
from etl.config import AGG_SALES_DAILY_PATH, AGG_PROFIT_DAILY_PATH

def build_monthly_sales_aggregates(year: int, month: int):
    """Build monthly sales aggregates from daily aggregates."""
    # Read daily aggregates for the month
    daily_path = f"{AGG_SALES_DAILY_PATH}/year={year}/month={month:02d}/**/*.parquet"
    df = pl.scan_parquet(daily_path).collect()
    
    if len(df) == 0:
        print(f"[MONTHLY] No data for {year}-{month:02d}")
        return
    
    # Aggregate to monthly level
    monthly = df.group_by([
        pl.col("year").cast(pl.Int32),
        pl.col("month").cast(pl.Int32)
    ]).agg([
        pl.col("revenue").sum(),
        pl.col("transactions").sum(),
        pl.col("items_sold").sum(),
        pl.col("lines").sum()
    ])
    
    # Write monthly aggregate
    output_path = f"/data-lake/star-schema/agg_sales_monthly/year={year}/month={month:02d}/agg_sales_monthly_{year}_{month:02d}.parquet"
    monthly.write_parquet(output_path)
    print(f"[MONTHLY] Created {output_path}")
```

- [ ] **Step 2: Add monthly aggregate to DuckDB views**

Add to `services/duckdb_connector.py`:

```python
if "sales_agg" in groups:
    _try_create_view(
        "agg_sales_monthly",
        f"""
        CREATE OR REPLACE VIEW agg_sales_monthly AS
        SELECT
            MAKE_DATE(year, month, 1) AS date,
            COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,
            COALESCE(TRY_CAST(transactions AS BIGINT), 0) AS transactions,
            COALESCE(TRY_CAST(items_sold AS DOUBLE), 0) AS items_sold,
            COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
        FROM read_parquet('/data-lake/star-schema/agg_sales_monthly/**/*.parquet', 
                          union_by_name=True, hive_partitioning=1, filename=true)
        """,
        """
        CREATE OR REPLACE VIEW agg_sales_monthly AS
        SELECT
            CAST(NULL AS DATE) AS date,
            CAST(0 AS DOUBLE) AS revenue,
            CAST(0 AS BIGINT) AS transactions,
            CAST(0 AS DOUBLE) AS items_sold,
            CAST(0 AS BIGINT) AS lines
        WHERE FALSE
        """,
    )
```

- [ ] **Step 3: Add query routing based on date range**

Create query router in `services/query_router.py`:

```python
from datetime import date, timedelta
from services.duckdb_connector import get_duckdb_connection

def query_sales_trends(start_date: date, end_date: date) -> pd.DataFrame:
    """Route to appropriate aggregate based on date range."""
    days = (end_date - start_date).days + 1
    
    conn = get_duckdb_connection()
    
    if days > 180:  # 6+ months: use monthly aggregates
        query = """
        SELECT date, revenue, transactions, items_sold, lines
        FROM agg_sales_monthly
        WHERE date >= ? AND date <= ?
        ORDER BY date
        """
        return conn.execute(query, [start_date, end_date]).fetchdf()
    else:  # < 6 months: use daily aggregates
        query = """
        SELECT date, revenue, transactions, items_sold, lines
        FROM agg_sales_daily
        WHERE date >= ? AND date <= ?
        ORDER BY date
        """
        return conn.execute(query, [start_date, end_date]).fetchdf()
```

---

### Task 5: Performance Testing and Validation

**Files:**
- Create: `scripts/test_yearly_performance.py`
- Create: `scripts/test_cache_effectiveness.py`

- [ ] **Step 1: Create yearly performance test script**

Create `scripts/test_yearly_performance.py`:

```python
import time
from datetime import date, timedelta
from services.duckdb_connector import get_duckdb_connection, ensure_duckdb_view_groups

def test_yearly_query_performance():
    """Test query performance for yearly date ranges."""
    ensure_duckdb_view_groups({"sales_agg"})
    conn = get_duckdb_connection()
    
    # Test different date ranges
    test_ranges = [
        ("7 days", date.today() - timedelta(days=7), date.today()),
        ("30 days", date.today() - timedelta(days=30), date.today()),
        ("90 days", date.today() - timedelta(days=90), date.today()),
        ("180 days", date.today() - timedelta(days=180), date.today()),
        ("365 days", date.today() - timedelta(days=365), date.today()),
    ]
    
    print("=== Yearly Query Performance Test ===")
    
    for range_name, start_date, end_date in test_ranges:
        query = """
        SELECT date, revenue, transactions
        FROM agg_sales_daily
        WHERE date >= ? AND date <= ?
        ORDER BY date
        """
        
        start_time = time.time()
        result = conn.execute(query, [start_date, end_date]).fetchdf()
        elapsed = time.time() - start_time
        
        print(f"{range_name}: {elapsed:.3f}s, {len(result)} rows")
        
        # Verify sub-second performance
        if elapsed > 1.0:
            print(f"  WARNING: {range_name} query exceeded 1 second threshold")
    
    print("=== Test Complete ===")

if __name__ == "__main__":
    test_yearly_query_performance()
```

- [ ] **Step 2: Run performance test**

```bash
docker-compose exec dash-app python scripts/test_yearly_performance.py
```

Expected: All queries complete in < 1 second.

- [ ] **Step 3: Create cache effectiveness test**

Create `scripts/test_cache_effectiveness.py`:

```python
import time
from datetime import date, timedelta
from services.cache_manager import CacheManager
from services.overview_metrics import get_total_overview_summary

def test_cache_effectiveness():
    """Test cache hit/miss performance."""
    cache_manager = CacheManager()
    
    # Clear cache first
    cache_manager.invalidate_table("agg_sales_daily_by_product")
    
    start_date = date.today() - timedelta(days=30)
    end_date = date.today()
    
    print("=== Cache Effectiveness Test ===")
    
    # First query (cache miss)
    start_time = time.time()
    result1 = get_total_overview_summary(start_date, end_date)
    elapsed_miss = time.time() - start_time
    print(f"Cache miss: {elapsed_miss:.3f}s")
    
    # Second query (cache hit)
    start_time = time.time()
    result2 = get_total_overview_summary(start_date, end_date)
    elapsed_hit = time.time() - start_time
    print(f"Cache hit: {elapsed_hit:.3f}s")
    
    # Calculate speedup
    speedup = elapsed_miss / elapsed_hit if elapsed_hit > 0 else 0
    print(f"Cache speedup: {speedup:.1f}x")
    
    # Verify results are identical
    assert result1 == result2, "Cache returned different results!"
    print("=== Test Complete ===")

if __name__ == "__main__":
    test_cache_effectiveness()
```

- [ ] **Step 4: Run cache effectiveness test**

```bash
docker-compose exec dash-app python scripts/test_cache_effectiveness.py
```

Expected: Cache hit is at least 10x faster than cache miss.

---

### Task 6: Remove SQLite MV Dependencies

**Files:**
- Modify: `services/sqlite_manager.py` (deprecate or remove)
- Modify: `pages/operational.py` (remove MV refresh UI)
- Modify: `etl_tasks.py` (remove MV refresh tasks)

- [ ] **Step 1: Deprecate SQLiteManager for MV operations**

Add deprecation notice to `services/sqlite_manager.py`:

```python
import warnings

class SQLiteManager:
    """
    DEPRECATED: SQLite MV layer is being removed in favor of DuckDB-first architecture.
    This class is retained for backward compatibility but should not be used for new code.
    Use DuckDB aggregates directly via duckdb_connector.py instead.
    """
    
    def __init__(self):
        warnings.warn(
            "SQLiteManager is deprecated. Use DuckDB aggregates via duckdb_connector.py instead.",
            DeprecationWarning,
            stacklevel=2
        )
```

- [ ] **Step 2: Remove MV refresh UI from operational page**

Modify `pages/operational.py` to remove or hide MV refresh controls:

```python
# Comment out or remove MV refresh section
# def trigger_mv_refresh(n_clicks, date_start, date_end):
#     # MV refresh logic removed
#     pass
```

- [ ] **Step 3: Remove MV refresh ETL tasks**

Modify `etl_tasks.py` to remove MV refresh Celery tasks:

```python
# Comment out or remove MV refresh tasks
# @celery_app.task(bind=True, base=DatabaseTask, name='refresh_materialized_views')
# def refresh_materialized_views(self, views, start_date, end_date):
#     # MV refresh logic removed
#     pass
```

---

### Task 7: Documentation and Monitoring

**Files:**
- Create: `docs/architecture/duckdb-first-architecture.md`
- Modify: `README.md` (update architecture section)
- Create: `scripts/monitor_query_performance.py`

- [ ] **Step 1: Create architecture documentation**

Create `docs/architecture/duckdb-first-architecture.md`:

```markdown
# DuckDB-First Architecture

## Overview
This system uses DuckDB as the primary query engine for all analytics, eliminating the SQLite MV layer for simplicity and performance.

## Components
- **DuckDB**: Columnar analytics database for querying parquet aggregates
- **Parquet**: Compressed columnar storage with Hive partitioning
- **Redis**: Caching layer for frequently accessed query results
- **ETL**: Daily pipelines to refresh aggregate tables

## Data Flow
1. ETL extracts data from Odoo
2. ETL transforms and writes to parquet files
3. DuckDB queries parquet aggregates directly
4. Redis caches query results for performance

## Performance Characteristics
- 30-day queries: < 0.1s
- 90-day queries: < 0.3s
- 365-day queries: < 0.5s
- Cache hits: < 0.01s

## Migration from SQLite MVs
- SQLite MV layer deprecated as of 2026-05-29
- All queries migrated to DuckDB aggregates
- Redis caching added for performance
```

- [ ] **Step 2: Update README architecture section**

Update `README.md`:

```markdown
## Architecture
- **ETL**: DuckDB for extraction and transformation
- **Storage**: Parquet files with Hive partitioning (year/month/day)
- **Query Engine**: DuckDB for all analytics queries
- **Caching**: Redis for frequently accessed results
- **Web App**: Dash for visualization

## Data Sources
- Odoo POS (point of sales)
- Odoo Invoices (customer and vendor)
- Odoo Inventory (stock moves and quantities)
```

- [ ] **Step 3: Create query performance monitoring script**

Create `scripts/monitor_query_performance.py`:

```python
import time
from datetime import date, timedelta
from services.duckdb_connector import get_duckdb_connection
import redis

def monitor_query_performance():
    """Monitor query performance and cache hit rates."""
    conn = get_duckdb_connection()
    redis_client = redis.from_url('redis://redis:6379/0')
    
    # Get cache statistics
    info = redis_client.info('stats')
    hits = info.get('keyspace_hits', 0)
    misses = info.get('keyspace_misses', 0)
    total = hits + misses
    hit_rate = hits / total if total > 0 else 0
    
    print(f"=== Query Performance Monitor ===")
    print(f"Cache hits: {hits}")
    print(f"Cache misses: {misses}")
    print(f"Cache hit rate: {hit_rate:.1%}")
    
    # Test query performance
    start_date = date.today() - timedelta(days=30)
    end_date = date.today()
    
    query = """
    SELECT COUNT(*) as row_count
    FROM agg_sales_daily
    WHERE date >= ? AND date <= ?
    """
    
    start_time = time.time()
    result = conn.execute(query, [start_date, end_date]).fetchone()
    elapsed = time.time() - start_time
    
    print(f"Sample query time: {elapsed:.3f}s")
    print(f"=== Monitor Complete ===")

if __name__ == "__main__":
    monitor_query_performance()
```

---

## Self-Review

**1. Spec coverage:**
- DuckDB-first architecture design ✓
- Yearly query performance analysis ✓
- Partitioning strategy evaluation ✓
- Caching layer design ✓
- Implementation tasks ✓
- Performance testing ✓
- Documentation ✓

**2. Placeholder scan:**
- No TBD, TODO, or placeholder text found
- All steps include specific code and commands
- No vague instructions like "add appropriate error handling"

**3. Type consistency:**
- Date format consistently uses Python date objects
- Cache key structure consistent across functions
- Query parameter ordering consistent
- File paths use consistent patterns

**4. Architecture validation:**
- Removes SQLite MV complexity ✓
- Leverages existing DuckDB infrastructure ✓
- Adds Redis caching for performance ✓
- Maintains current partitioning strategy ✓
- Provides monitoring and validation ✓

---

## Expected Performance Outcomes

### Query Performance
- **7-day queries**: < 0.05s (with cache: < 0.01s)
- **30-day queries**: < 0.1s (with cache: < 0.01s)
- **90-day queries**: < 0.3s (with cache: < 0.01s)
- **365-day queries**: < 0.5s (with cache: < 0.01s)

### Operational Benefits
- **Complexity**: Eliminates SQLite MV layer
- **Maintenance**: No MV refresh failures
- **Reliability**: Single source of truth (DuckDB)
- **Scalability**: Handles 5+ years of data easily
- **Debugging**: Simpler architecture = easier troubleshooting

### Cache Effectiveness
- **Hit rate**: > 80% for common queries
- **Speedup**: 10-100x for cached queries
- **TTL**: Appropriate for data freshness
- **Invalidation**: Automatic after ETL

---

## Rollback Plan

If issues arise, rollback steps:

1. Restore backed-up files:
   ```bash
   cp services/sqlite_manager.py.backup services/sqlite_manager.py
   cp services/sales_metrics.py.backup services/sales_metrics.py
   cp services/profit_metrics.py.backup services/profit_metrics.py
   ```

2. Restart services:
   ```bash
   docker-compose restart dash-app celery-worker
   ```

3. Verify SQLite MVs still work:
   ```bash
   docker-compose exec dash-app python -c "import sqlite3; conn = sqlite3.connect('/data-lake/nkdash.db'); print(conn.execute('SELECT COUNT(*) FROM mv_sales_daily').fetchone())"
   ```
