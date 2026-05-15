# Bugfix Requirements Document

## Introduction

The DuckDB connector (`services/duckdb_connector.py`) has multiple performance issues causing slow dashboard load times and potential query hangs. These issues affect all dashboard pages that query DuckDB, degrading user experience across the application. The bugs include inefficient column caching, suboptimal date parsing, missing query timeouts, unverified cache clearing, and missing database indexes.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN the DuckDB connection is initialized THEN the system calls `_parquet_columns()` 3 times on the same parquet files without caching results

1.2 WHEN materialized views are loaded THEN the system uses complex `MAKE_DATE` + `SPLIT_PART` operations to parse dates from filenames instead of using the direct `date` column

1.3 WHEN a query is executed THEN the system allows queries to run indefinitely without timeout protection

1.4 WHEN `clear_sales_caches()` is called after MV refresh THEN the system may not properly clear the `@lru_cache` decorated query functions

1.5 WHEN the materialized view metadata table is queried THEN the system performs full table scans without a primary key index

### Expected Behavior (Correct)

2.1 WHEN the DuckDB connection is initialized THEN the system SHALL cache `_parquet_columns()` results in a module-level dictionary to avoid repeated DESCRIBE queries

2.2 WHEN materialized views are loaded THEN the system SHALL use the direct `date` column from parquet files instead of parsing filenames

2.3 WHEN a query is executed THEN the system SHALL enforce a configurable timeout (default 10 seconds) to prevent long-running queries from blocking the application

2.4 WHEN `clear_sales_caches()` is called after MV refresh THEN the system SHALL properly clear all `@lru_cache` decorated query functions AND clear the column cache

2.5 WHEN the materialized view metadata table is created THEN the system SHALL define `view_name` as the PRIMARY KEY for efficient lookups

### Unchanged Behavior (Regression Prevention)

3.1 WHEN existing cached query results are valid THEN the system SHALL CONTINUE TO return cached results without re-executing queries

3.2 WHEN parquet files have the `date` column available THEN the system SHALL CONTINUE TO read date values correctly

3.3 WHEN queries complete within the timeout period THEN the system SHALL CONTINUE TO return results normally

3.4 WHEN materialized views are refreshed incrementally THEN the system SHALL CONTINUE TO load only new/changed partitions

3.5 WHEN DuckDB views are queried THEN the system SHALL CONTINUE TO use hive partitioning for efficient date-based filtering

3.6 WHEN the connection is closed and reopened THEN the system SHALL CONTINUE TO reload materialized views correctly

3.7 WHEN multiple dashboard pages query DuckDB concurrently THEN the system SHALL CONTINUE TO handle concurrent access safely with the existing lock mechanism

## Bug Condition and Property Specification

### Bug Condition Function

```pascal
FUNCTION isBugCondition(X)
  INPUT: X of type DuckDBOperation
  OUTPUT: boolean
  
  // Returns true when performance bug conditions are met
  RETURN (
    (X.operation = "describe_parquet" AND X.path IN previously_described_paths) OR
    (X.operation = "load_mv" AND X.uses_filename_parsing = true) OR
    (X.operation = "execute_query" AND X.has_timeout = false) OR
    (X.operation = "clear_cache" AND X.clears_column_cache = false) OR
    (X.operation = "create_mv_metadata" AND X.has_primary_key = false)
  )
END FUNCTION
```

### Property Specification - Fix Checking

```pascal
// Property: Fix Checking - Column Cache Efficiency
FOR ALL X WHERE X.operation = "describe_parquet" AND X.path IN previously_described_paths DO
  result ← _parquet_columns'(X.connection, X.path)
  ASSERT result = cached_value(X.path) AND X.connection.execute.call_count = 0
END FOR

// Property: Fix Checking - Optimized Date Parsing
FOR ALL X WHERE X.operation = "load_mv" DO
  sql ← generate_mv_sql'(X.mv_name)
  ASSERT "date," IN sql OR "date AS date" IN sql
  ASSERT "SPLIT_PART" NOT IN sql OR "filename" NOT IN sql
END FOR

// Property: Fix Checking - Query Timeout
FOR ALL X WHERE X.operation = "execute_query" DO
  result ← execute_with_timeout'(X.connection, X.query, X.params)
  ASSERT X.connection.timeout_was_set = true
  ASSERT X.connection.timeout_value <= 10000  // 10 seconds in milliseconds
END FOR

// Property: Fix Checking - Cache Clearing
FOR ALL X WHERE X.operation = "clear_cache" DO
  clear_sales_caches'()
  ASSERT query_sales_trends.cache_info().currsize = 0
  ASSERT query_top_products.cache_info().currsize = 0
  ASSERT len(_column_cache) = 0
END FOR

// Property: Fix Checking - MV Metadata Index
FOR ALL X WHERE X.operation = "create_mv_metadata" DO
  schema ← get_table_schema'("mv_refresh_metadata")
  ASSERT "PRIMARY KEY" IN schema.constraints
  ASSERT schema.primary_key_column = "view_name"
END FOR
```

### Property Specification - Preservation Checking

```pascal
// Property: Preservation Checking
FOR ALL X WHERE NOT isBugCondition(X) DO
  ASSERT F(X) = F'(X)
END FOR

// Specific preservation checks:
// - Cached query results still returned correctly
// - Date values read correctly from parquet
// - Queries completing within timeout return normally
// - Incremental MV refresh still works
// - Hive partitioning still used for filtering
// - Connection reload still works
// - Concurrent access still safe
```

## Counterexamples

### Example 1: Column Cache Inefficiency
```python
# Input: Initialize DuckDB connection
manager = DuckDBManager()
conn = manager.get_connection()

# Current behavior (buggy):
# _parquet_columns() called 3 times for same file during _setup_views()
# Each call executes DESCRIBE query (~50ms each = 150ms total)

# Expected behavior (fixed):
# First call executes DESCRIBE and caches result
# Subsequent calls return cached result (~0ms)
# Total time: ~50ms (3x faster)
```

### Example 2: Inefficient Date Parsing
```python
# Input: Load mv_sales_daily
manager.ensure_materialized_views({"mv_sales_daily"})

# Current behavior (buggy):
# SQL uses: MAKE_DATE(CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER), ...)
# Complex parsing for every row

# Expected behavior (fixed):
# SQL uses: date
# Direct column access, no parsing needed
```

### Example 3: No Query Timeout
```python
# Input: Execute slow query
df = query_sales_trends(date(2020, 1, 1), date(2025, 12, 31))

# Current behavior (buggy):
# Query runs indefinitely if data is large or query is inefficient
# Dashboard hangs, user cannot interact

# Expected behavior (fixed):
# Query times out after 10 seconds
# Exception raised, empty DataFrame returned
# User sees "No data" message instead of hang
```

### Example 4: Cache Not Cleared
```python
# Input: Refresh materialized views
manager.ensure_materialized_views({"mv_sales_daily"}, force_reload=True)
clear_sales_caches()

# Current behavior (buggy):
# _column_cache not cleared
# Old column metadata may be stale

# Expected behavior (fixed):
# All caches cleared including _column_cache
# Fresh data loaded on next query
```

### Example 5: Missing MV Metadata Index
```python
# Input: Query MV refresh metadata
conn.execute("SELECT * FROM mv_refresh_metadata WHERE view_name = 'mv_sales_daily'")

# Current behavior (buggy):
# Full table scan on every lookup
# Slow for large metadata tables

# Expected behavior (fixed):
# Index lookup using PRIMARY KEY
# Fast O(1) access
```
