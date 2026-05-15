# DuckDB Connector Optimization Bugfix Design

## Overview

This design addresses five performance bugs in the DuckDB connector (`services/duckdb_connector.py`) that cause slow dashboard load times and potential query hangs. The bugs affect all dashboard pages that query DuckDB, degrading user experience across the application. The fix strategy focuses on caching, query optimization, timeout protection, and proper cache invalidation to improve performance by 3-10x for common operations.

**Impact:** Dashboard initialization time reduced from ~150ms to ~50ms (3x faster), queries protected from indefinite hangs, and stale cache issues eliminated.

## Glossary

- **Bug_Condition (C)**: The condition that triggers performance degradation - repeated DESCRIBE queries, inefficient date parsing, missing timeouts, incomplete cache clearing, or missing indexes
- **Property (P)**: The desired behavior - cached column metadata, optimized date parsing, query timeouts, complete cache clearing, and indexed metadata lookups
- **Preservation**: Existing query results, date parsing correctness, incremental MV refresh, hive partitioning, connection reload, and concurrent access safety that must remain unchanged
- **_parquet_columns()**: Helper function in `duckdb_connector.py` that executes DESCRIBE queries to get parquet column names
- **_column_cache**: Module-level dictionary that caches parquet column metadata to avoid repeated DESCRIBE queries
- **_load_materialized_views()**: Method that loads data from parquet into DuckDB tables for fast queries
- **clear_sales_caches()**: Function that clears @lru_cache decorated query functions after MV refresh
- **mv_refresh_metadata**: DuckDB table that tracks materialized view refresh timestamps and row counts

## Bug Details

### Bug Condition

The bugs manifest when the DuckDB connector performs common operations without proper optimization. The `_setup_views()` method calls `_parquet_columns()` multiple times on the same files, `_load_materialized_views()` uses complex filename parsing instead of direct column access, queries execute without timeout protection, `clear_sales_caches()` doesn't clear the column cache, and `mv_refresh_metadata` table lacks a primary key index.

**Formal Specification:**
```
FUNCTION isBugCondition(input)
  INPUT: input of type DuckDBOperation
  OUTPUT: boolean
  
  RETURN (
    (input.operation = "describe_parquet" AND input.path IN previously_described_paths) OR
    (input.operation = "load_mv" AND input.uses_filename_parsing = true) OR
    (input.operation = "execute_query" AND input.has_timeout = false) OR
    (input.operation = "clear_cache" AND input.clears_column_cache = false) OR
    (input.operation = "create_mv_metadata" AND input.has_primary_key = false)
  )
END FUNCTION
```

### Examples

**Bug 1: Column Cache Inefficiency**
- Input: Initialize DuckDB connection with `_setup_views(conn, groups={"overview"})`
- Current behavior: `_parquet_columns()` called 3 times for `agg_profit_daily` during view setup, each executing DESCRIBE query (~50ms each = 150ms total)
- Expected behavior: First call executes DESCRIBE and caches result in `_column_cache`, subsequent calls return cached result (~0ms), total time ~50ms (3x faster)

**Bug 2: Inefficient Date Parsing**
- Input: Load `mv_sales_daily` with `ensure_materialized_views({"mv_sales_daily"})`
- Current behavior: SQL uses `MAKE_DATE(CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER), ...)` - complex parsing for every row
- Expected behavior: SQL uses `TRY_CAST(date AS DATE)` - direct column access, no parsing needed

**Bug 3: No Query Timeout**
- Input: Execute slow query `query_sales_trends(date(2020, 1, 1), date(2025, 12, 31))`
- Current behavior: Query runs indefinitely if data is large or query is inefficient, dashboard hangs, user cannot interact
- Expected behavior: Query times out after 10 seconds, exception raised, empty DataFrame returned, user sees "No data" message instead of hang

**Bug 4: Cache Not Cleared**
- Input: Refresh materialized views with `ensure_materialized_views({"mv_sales_daily"}, force_reload=True)` then call `clear_sales_caches()`
- Current behavior: `_column_cache` not cleared, old column metadata may be stale
- Expected behavior: All caches cleared including `_column_cache`, fresh data loaded on next query

**Bug 5: Missing MV Metadata Index**
- Input: Query MV refresh metadata with `conn.execute("SELECT * FROM mv_refresh_metadata WHERE view_name = 'mv_sales_daily'")`
- Current behavior: Full table scan on every lookup, slow for large metadata tables
- Expected behavior: Index lookup using PRIMARY KEY, fast O(1) access

## Expected Behavior

### Preservation Requirements

**Unchanged Behaviors:**
- Existing cached query results must continue to be returned without re-executing queries
- Parquet files with the `date` column must continue to read date values correctly
- Queries that complete within the timeout period must continue to return results normally
- Materialized views refreshed incrementally must continue to load only new/changed partitions
- DuckDB views must continue to use hive partitioning for efficient date-based filtering
- Connection close and reopen must continue to reload materialized views correctly
- Multiple dashboard pages querying DuckDB concurrently must continue to handle concurrent access safely with the existing lock mechanism

**Scope:**
All operations that do NOT involve the five bug conditions should be completely unaffected by this fix. This includes:
- Normal query execution that completes quickly
- Parquet files without date columns (fallback to filename parsing)
- Cache hits on already-cached query results
- View creation for non-aggregate tables
- DuckDB operations outside the connector (direct SQL queries)

## Hypothesized Root Cause

Based on the bug description and code analysis, the most likely issues are:

1. **Missing Column Cache**: The `_parquet_columns()` function executes DESCRIBE queries every time it's called, even for the same file path. The module-level `_column_cache` dictionary exists but is never populated or checked.

2. **Suboptimal SQL Generation**: The `_load_materialized_views()` method generates SQL that uses `MAKE_DATE(CAST(SPLIT_PART(...)))` to parse dates from filenames, even though the parquet files have a direct `date` column available. This is a fallback pattern that should only be used when the date column is missing.

3. **No Timeout Configuration**: Query execution uses `conn.execute()` without setting DuckDB's `statement_timeout` configuration. DuckDB supports timeout via `SET statement_timeout=<milliseconds>`, but this is never configured.

4. **Incomplete Cache Clearing**: The `clear_sales_caches()` function calls `.cache_clear()` on all `@lru_cache` decorated functions but doesn't clear the `_column_cache` dictionary, leaving stale column metadata.

5. **Missing Primary Key**: The `mv_refresh_metadata` table is created with `CREATE TABLE IF NOT EXISTS mv_refresh_metadata (view_name VARCHAR PRIMARY KEY, ...)` but the PRIMARY KEY constraint is in the column definition, not as a separate constraint. DuckDB may not be recognizing this as a primary key index.

## Correctness Properties

Property 1: Bug Condition - Column Cache Efficiency

_For any_ parquet file path that has been previously described, the fixed `_parquet_columns()` function SHALL return cached column metadata from `_column_cache` without executing a DESCRIBE query, reducing repeated DESCRIBE overhead from ~150ms to ~0ms.

**Validates: Requirements 2.1**

Property 2: Bug Condition - Optimized Date Parsing

_For any_ materialized view load operation, the fixed `_load_materialized_views()` function SHALL use direct `TRY_CAST(date AS DATE)` column access instead of `MAKE_DATE(CAST(SPLIT_PART(...)))` filename parsing, reducing query complexity and improving load performance.

**Validates: Requirements 2.2**

Property 3: Bug Condition - Query Timeout Protection

_For any_ query execution, the fixed query functions SHALL set DuckDB's `statement_timeout` to 10 seconds (10000ms) before execution, preventing indefinite query hangs and ensuring the dashboard remains responsive.

**Validates: Requirements 2.3**

Property 4: Bug Condition - Complete Cache Clearing

_For any_ cache clear operation after MV refresh, the fixed `clear_sales_caches()` function SHALL clear both `@lru_cache` decorated functions AND the `_column_cache` dictionary, ensuring no stale metadata persists.

**Validates: Requirements 2.4**

Property 5: Bug Condition - MV Metadata Index

_For any_ MV metadata table creation, the fixed `_load_materialized_views()` function SHALL define `view_name` as a PRIMARY KEY with proper index creation, enabling O(1) lookup performance instead of full table scans.

**Validates: Requirements 2.5**

Property 6: Preservation - Existing Query Behavior

_For any_ query operation that does NOT involve the five bug conditions (repeated DESCRIBE, filename parsing, missing timeout, incomplete cache clear, missing index), the fixed code SHALL produce exactly the same results as the original code, preserving all existing query functionality.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7**

## Fix Implementation

### Changes Required

Assuming our root cause analysis is correct:

**File**: `services/duckdb_connector.py`

**Function**: `_parquet_columns()`

**Specific Changes**:
1. **Add Cache Check**: Before executing DESCRIBE query, check if `parquet_path` exists in `_column_cache` dictionary
   - If found, return cached value immediately
   - If not found, execute DESCRIBE query and store result in `_column_cache[parquet_path]`

2. **Optimize Date Parsing in MVs**: In `_load_materialized_views()`, replace all instances of:
   ```sql
   COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
       CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
       CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
       CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
   )) AS date
   ```
   With:
   ```sql
   TRY_CAST(date AS DATE) AS date
   ```
   - Remove `filename=true` from `read_parquet()` calls since filename parsing is no longer needed
   - Keep COALESCE fallback only if date column might be NULL

3. **Add Query Timeout Helper**: Create new function `_execute_with_timeout()`:
   ```python
   def _execute_with_timeout(conn, query: str, params: list = None, timeout_ms: int = 10000) -> pd.DataFrame:
       """Execute query with configurable timeout."""
       conn.execute(f"SET statement_timeout={timeout_ms}")
       if params:
           result = conn.execute(query, params).fetchdf()
       else:
           result = conn.execute(query).fetchdf()
       return result
   ```

4. **Update Query Functions**: Replace all `conn.execute(query, params).fetchdf()` calls in query functions with `_execute_with_timeout(conn, query, params)`
   - Functions to update: `query_sales_trends()`, `query_hourly_sales_pattern()`, `query_top_products()`, `query_revenue_comparison()`, `query_hourly_sales_heatmap()`, `query_overview_summary()`, `query_sales_by_principal()`

5. **Fix Cache Clearing**: In `clear_sales_caches()`, add line to clear column cache:
   ```python
   _column_cache.clear()
   ```

6. **Fix MV Metadata Index**: In `_load_materialized_views()`, change table creation from:
   ```sql
   CREATE TABLE IF NOT EXISTS mv_refresh_metadata (
       view_name VARCHAR PRIMARY KEY,
       ...
   )
   ```
   To:
   ```sql
   CREATE TABLE IF NOT EXISTS mv_refresh_metadata (
       view_name VARCHAR,
       last_refresh_date TIMESTAMP,
       max_data_date DATE,
       row_count BIGINT,
       refresh_type VARCHAR,
       PRIMARY KEY (view_name)
   )
   ```

## Testing Strategy

### Validation Approach

The testing strategy follows a two-phase approach: first, surface counterexamples that demonstrate the bugs on unfixed code, then verify the fixes work correctly and preserve existing behavior.

### Exploratory Bug Condition Checking

**Goal**: Surface counterexamples that demonstrate the bugs BEFORE implementing the fix. Confirm or refute the root cause analysis. If we refute, we will need to re-hypothesize.

**Test Plan**: Write tests that measure performance and behavior of the unfixed code. Run these tests to observe failures and understand the root causes.

**Test Cases**:
1. **Column Cache Test**: Call `_parquet_columns()` 3 times on same file, measure execution time (will show ~150ms on unfixed code)
2. **Date Parsing Test**: Inspect SQL generated by `_load_materialized_views()` for `mv_sales_daily` (will show MAKE_DATE+SPLIT_PART on unfixed code)
3. **Query Timeout Test**: Execute slow query with large date range, observe if it hangs indefinitely (will hang on unfixed code)
4. **Cache Clear Test**: Call `clear_sales_caches()`, check if `_column_cache` is empty (will not be empty on unfixed code)
5. **MV Metadata Index Test**: Query `SHOW TABLES` and check if PRIMARY KEY constraint exists on `mv_refresh_metadata` (may not exist on unfixed code)

**Expected Counterexamples**:
- `_parquet_columns()` executes DESCRIBE query every time, no cache hits
- SQL contains `MAKE_DATE(CAST(SPLIT_PART(...)))` instead of direct date column
- Queries run indefinitely without timeout
- `_column_cache` not cleared after `clear_sales_caches()`
- `mv_refresh_metadata` table has no PRIMARY KEY index

### Fix Checking

**Goal**: Verify that for all inputs where the bug condition holds, the fixed function produces the expected behavior.

**Pseudocode:**
```
FOR ALL input WHERE isBugCondition(input) DO
  result := fixedFunction(input)
  ASSERT expectedBehavior(result)
END FOR
```

**Specific Tests:**

1. **Column Cache Efficiency**:
```python
# Clear cache first
_column_cache.clear()

# First call - should execute DESCRIBE
start = time.time()
cols1 = _parquet_columns(conn, "/data-lake/star-schema/agg_sales_daily/**/*.parquet")
time1 = time.time() - start

# Second call - should use cache
start = time.time()
cols2 = _parquet_columns(conn, "/data-lake/star-schema/agg_sales_daily/**/*.parquet")
time2 = time.time() - start

# Assertions
assert cols1 == cols2  # Same columns returned
assert time2 < time1 / 10  # Cache hit is 10x faster
assert "/data-lake/star-schema/agg_sales_daily/**/*.parquet" in _column_cache
```

2. **Optimized Date Parsing**:
```python
# Inspect SQL generated for mv_sales_daily
conn = DuckDBManager().get_connection()
# Trigger MV load
ensure_materialized_views({"mv_sales_daily"})

# Check that date column is used directly
result = conn.execute("SELECT date FROM mv_sales_daily LIMIT 1").fetchone()
assert result[0] is not None  # Date parsed correctly

# Verify SQL doesn't contain filename parsing (check logs or SQL plan)
# Expected: No "SPLIT_PART" or "filename" in SQL
```

3. **Query Timeout Protection**:
```python
# Execute query with timeout
conn = get_duckdb_connection()
start = time.time()
try:
    # This should timeout after 10 seconds
    result = _execute_with_timeout(conn, "SELECT * FROM generate_series(1, 1000000000)", timeout_ms=1000)
    assert False, "Query should have timed out"
except Exception as e:
    elapsed = time.time() - start
    assert elapsed < 2.0  # Should timeout within 2 seconds (1s timeout + overhead)
    assert "timeout" in str(e).lower() or "interrupt" in str(e).lower()
```

4. **Complete Cache Clearing**:
```python
# Populate caches
_column_cache["/test/path"] = {"col1", "col2"}
query_sales_trends(date(2025, 1, 1), date(2025, 1, 31))  # Populate lru_cache

# Clear caches
clear_sales_caches()

# Assertions
assert len(_column_cache) == 0  # Column cache cleared
assert query_sales_trends.cache_info().currsize == 0  # LRU cache cleared
```

5. **MV Metadata Index**:
```python
# Create metadata table
conn = get_duckdb_connection()
ensure_materialized_views({"mv_sales_daily"})

# Check PRIMARY KEY exists
schema = conn.execute("""
    SELECT * FROM information_schema.table_constraints 
    WHERE table_name = 'mv_refresh_metadata' AND constraint_type = 'PRIMARY KEY'
""").fetchall()

assert len(schema) > 0  # PRIMARY KEY constraint exists
assert schema[0][2] == 'view_name'  # Primary key is on view_name column
```

### Preservation Checking

**Goal**: Verify that for all inputs where the bug condition does NOT hold, the fixed function produces the same result as the original function.

**Pseudocode:**
```
FOR ALL input WHERE NOT isBugCondition(input) DO
  ASSERT originalFunction(input) = fixedFunction(input)
END FOR
```

**Testing Approach**: Property-based testing is recommended for preservation checking because:
- It generates many test cases automatically across the input domain
- It catches edge cases that manual unit tests might miss
- It provides strong guarantees that behavior is unchanged for all non-buggy inputs

**Test Plan**: Observe behavior on UNFIXED code first for non-bug inputs, then write property-based tests capturing that behavior.

**Test Cases**:
1. **Query Results Preservation**: Execute various queries on both unfixed and fixed code, verify results are identical
   - Test queries: `query_sales_trends()`, `query_top_products()`, `query_revenue_comparison()`
   - Compare DataFrames using `pd.testing.assert_frame_equal()`

2. **Date Parsing Preservation**: Verify date values are parsed correctly in all MVs
   - Load MVs on both unfixed and fixed code
   - Compare date ranges and row counts

3. **Incremental MV Refresh Preservation**: Verify incremental refresh still works
   - Load MV with initial data
   - Add new data to parquet files
   - Reload MV incrementally
   - Verify only new data is loaded

4. **Concurrent Access Preservation**: Verify multiple threads can query DuckDB safely
   - Spawn multiple threads executing queries concurrently
   - Verify no deadlocks or race conditions

### Unit Tests

- Test `_parquet_columns()` caching logic with various file paths
- Test `_execute_with_timeout()` with fast and slow queries
- Test `clear_sales_caches()` clears all caches
- Test MV metadata table creation with PRIMARY KEY
- Test date parsing in `_load_materialized_views()` with and without date column

### Property-Based Tests

- Generate random date ranges and verify query results are consistent
- Generate random parquet file paths and verify caching works correctly
- Generate random query patterns and verify timeout protection works
- Test cache clearing across many scenarios

### Integration Tests

- Test full dashboard initialization with optimized connector
- Test MV refresh workflow with cache clearing
- Test concurrent dashboard page loads
- Test query timeout behavior under load
