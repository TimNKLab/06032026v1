"""Bug Condition Exploration Tests for DuckDB Connector Optimization.

CRITICAL: These tests are EXPECTED TO FAIL on unfixed code.
Failures confirm bugs exist and surface counterexamples.

DO NOT fix tests or code when they fail - document failures and move on.

Bug Conditions Being Explored:
1. Column Cache Inefficiency - _parquet_columns() called 3x without caching
2. Inefficient Date Parsing - Uses MAKE_DATE+SPLIT_PART instead of direct date column
3. No Query Timeout - Queries can hang indefinitely
4. Incomplete Cache Clearing - _column_cache not cleared by clear_sales_caches()
5. Missing MV Metadata Index - No PRIMARY KEY on mv_refresh_metadata table
"""

import pytest
import time
import os
from datetime import date
from unittest.mock import MagicMock, patch
from services.duckdb_connector import DuckDBManager, _column_cache


class TestBug1ColumnCacheInefficiency:
    """Bug 1: Column Cache - _parquet_columns() executes DESCRIBE 3x on same file.
    
    Expected behavior on UNFIXED code: Test FAILS - no caching, ~150ms total time.
    Expected behavior on FIXED code: Test PASSES - caching works, ~50ms total time.
    """

    def test_column_cache_repeated_calls_performance(self):
        """Call _parquet_columns() 3x on same file, measure time.
        
        UNFIXED: Expect ~150ms (3 x 50ms DESCRIBE queries, no cache).
        FIXED: Expect ~50ms (1 DESCRIBE + 2 cache hits).
        """
        # Clear any existing cache
        _column_cache.clear()
        
        manager = DuckDBManager()
        conn = manager.get_connection()
        
        # Use a real parquet path from the data lake
        data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
        parquet_path = f"{data_lake}/star-schema/agg_sales_daily/**/*.parquet"
        
        # Measure time for 3 consecutive calls
        times = []
        for i in range(3):
            start = time.time()
            try:
                cols = manager._parquet_columns(conn, parquet_path)
                elapsed = time.time() - start
                times.append(elapsed)
                print(f"[Bug1] Call {i+1}: {elapsed*1000:.1f}ms, columns: {len(cols)}")
            except Exception as e:
                print(f"[Bug1] Call {i+1} failed: {e}")
                times.append(0)
        
        total_time = sum(times) * 1000  # Convert to ms
        print(f"[Bug1] Total time for 3 calls: {total_time:.1f}ms")
        
        # On UNFIXED code: expect ~150ms (no caching)
        # On FIXED code: expect ~50ms (caching works)
        # This assertion will FAIL on unfixed code, confirming the bug
        assert total_time < 100, (
            f"Bug 1 DETECTED: Column cache inefficiency. "
            f"Expected <100ms with caching, got {total_time:.1f}ms. "
            f"This indicates _parquet_columns() is not caching results."
        )


class TestBug2InefficientDateParsing:
    """Bug 2: Date Parsing - Uses MAKE_DATE+SPLIT_PART instead of direct date column.
    
    Expected behavior on UNFIXED code: Test FAILS - SQL contains SPLIT_PART.
    Expected behavior on FIXED code: Test PASSES - SQL uses direct date column.
    """

    def test_mv_sql_uses_filename_parsing(self):
        """Inspect SQL from _load_materialized_views() for mv_sales_daily.
        
        UNFIXED: Expect MAKE_DATE(CAST(SPLIT_PART(...))) in SQL.
        FIXED: Expect direct date column usage, no SPLIT_PART.
        """
        manager = DuckDBManager()
        conn = manager.get_connection()
        
        # Capture SQL executed during MV load
        captured_sql = []
        original_execute = conn.execute
        
        def capture_execute(sql, *args, **kwargs):
            if isinstance(sql, str):
                captured_sql.append(sql)
            return original_execute(sql, *args, **kwargs)
        
        conn.execute = capture_execute
        
        # Trigger MV load for mv_sales_daily
        try:
            manager._load_materialized_views(conn, {"mv_sales_daily"})
        except Exception as e:
            print(f"[Bug2] MV load failed: {e}")
        
        # Find the CREATE TABLE SQL for mv_sales_daily
        mv_sql = None
        for sql in captured_sql:
            if "mv_sales_daily" in sql and ("CREATE" in sql or "INSERT" in sql):
                mv_sql = sql
                break
        
        if mv_sql:
            print(f"[Bug2] SQL snippet: {mv_sql[:500]}...")
            
            # Check for inefficient filename parsing
            has_split_part = "SPLIT_PART" in mv_sql
            has_filename = "filename" in mv_sql.lower()
            uses_make_date = "MAKE_DATE" in mv_sql
            
            print(f"[Bug2] Has SPLIT_PART: {has_split_part}")
            print(f"[Bug2] Has filename: {has_filename}")
            print(f"[Bug2] Uses MAKE_DATE: {uses_make_date}")
            
            # On UNFIXED code: expect SPLIT_PART + filename parsing
            # This assertion will FAIL on unfixed code, confirming the bug
            assert not (has_split_part and has_filename), (
                f"Bug 2 DETECTED: Inefficient date parsing. "
                f"SQL uses SPLIT_PART + filename parsing instead of direct date column. "
                f"This causes unnecessary parsing overhead for every row."
            )
        else:
            pytest.skip("Could not capture mv_sales_daily SQL")


class TestBug3NoQueryTimeout:
    """Bug 3: Query Timeout - Queries can run indefinitely without timeout.
    
    Expected behavior on UNFIXED code: Test FAILS - query hangs or takes >10s.
    Expected behavior on FIXED code: Test PASSES - query times out within 10s.
    """

    def test_slow_query_hangs_without_timeout(self):
        """Execute slow query with 2020-2025 date range.
        
        UNFIXED: Expect query to hang or take very long (>10s).
        FIXED: Expect query to timeout within 10s.
        """
        manager = DuckDBManager()
        conn = manager.get_connection()
        
        # Execute a potentially slow query (large date range)
        start = time.time()
        try:
            # This query might be slow if there's a lot of data
            result = conn.execute("""
                SELECT COUNT(*) as cnt
                FROM mv_sales_daily
                WHERE date BETWEEN '2020-01-01' AND '2025-12-31'
            """).fetchone()
            elapsed = time.time() - start
            print(f"[Bug3] Query completed in {elapsed:.2f}s, result: {result}")
            
            # On UNFIXED code: query might take very long or hang
            # On FIXED code: query should timeout within 10s
            # This assertion will FAIL on unfixed code if query takes >10s
            assert elapsed < 10, (
                f"Bug 3 DETECTED: No query timeout protection. "
                f"Query took {elapsed:.2f}s without timing out. "
                f"Expected timeout within 10s to prevent dashboard hangs."
            )
        except Exception as e:
            elapsed = time.time() - start
            print(f"[Bug3] Query failed after {elapsed:.2f}s: {e}")
            
            # If query timed out, that's actually the FIXED behavior
            if "timeout" in str(e).lower() or "interrupt" in str(e).lower():
                print(f"[Bug3] Query timed out correctly (FIXED behavior)")
                # This is the expected behavior on FIXED code
                assert elapsed < 12, "Timeout should occur within 10s + overhead"
            else:
                # Other error - re-raise
                raise


class TestBug4CacheNotCleared:
    """Bug 4: Cache Clearing - _column_cache not cleared by clear_sales_caches().
    
    Expected behavior on UNFIXED code: Test FAILS - _column_cache not empty.
    Expected behavior on FIXED code: Test PASSES - _column_cache cleared.
    """

    def test_clear_sales_caches_does_not_clear_column_cache(self):
        """Call clear_sales_caches(), check if _column_cache is empty.
        
        UNFIXED: Expect _column_cache NOT empty after clear.
        FIXED: Expect _column_cache empty after clear.
        """
        from services.sales_metrics import clear_sales_caches
        
        # Populate _column_cache
        _column_cache["/test/path1"] = {"col1", "col2"}
        _column_cache["/test/path2"] = {"col3", "col4"}
        
        print(f"[Bug4] Before clear: _column_cache has {len(_column_cache)} entries")
        
        # Call clear_sales_caches()
        clear_sales_caches()
        
        print(f"[Bug4] After clear: _column_cache has {len(_column_cache)} entries")
        
        # On UNFIXED code: expect _column_cache NOT cleared
        # This assertion will FAIL on unfixed code, confirming the bug
        assert len(_column_cache) == 0, (
            f"Bug 4 DETECTED: Incomplete cache clearing. "
            f"_column_cache still has {len(_column_cache)} entries after clear_sales_caches(). "
            f"Expected 0 entries. Stale column metadata may persist."
        )


class TestBug5MissingMVMetadataIndex:
    """Bug 5: MV Metadata Index - No PRIMARY KEY on mv_refresh_metadata table.
    
    Expected behavior on UNFIXED code: Test FAILS - no PRIMARY KEY constraint.
    Expected behavior on FIXED code: Test PASSES - PRIMARY KEY exists.
    """

    def test_mv_metadata_table_has_no_primary_key(self):
        """Query information_schema.table_constraints for PRIMARY KEY.
        
        UNFIXED: Expect no PRIMARY KEY constraint on mv_refresh_metadata.
        FIXED: Expect PRIMARY KEY constraint on view_name column.
        """
        manager = DuckDBManager()
        conn = manager.get_connection()
        
        # Ensure mv_refresh_metadata table exists
        try:
            manager._load_materialized_views(conn, {"mv_sales_daily"})
        except Exception as e:
            print(f"[Bug5] MV load failed: {e}")
        
        # Query for PRIMARY KEY constraint
        try:
            result = conn.execute("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_name = 'mv_refresh_metadata'
                  AND constraint_type = 'PRIMARY KEY'
            """).fetchall()
            
            print(f"[Bug5] PRIMARY KEY constraints found: {len(result)}")
            if result:
                print(f"[Bug5] Constraints: {result}")
            
            # On UNFIXED code: expect no PRIMARY KEY constraint
            # This assertion will FAIL on unfixed code, confirming the bug
            assert len(result) > 0, (
                f"Bug 5 DETECTED: Missing MV metadata index. "
                f"No PRIMARY KEY constraint found on mv_refresh_metadata table. "
                f"This causes full table scans on every lookup, degrading performance."
            )
        except Exception as e:
            print(f"[Bug5] Query failed: {e}")
            # If information_schema query fails, try alternative check
            try:
                # Check if we can query the table at all
                conn.execute("SELECT * FROM mv_refresh_metadata LIMIT 1").fetchone()
                print(f"[Bug5] Table exists but PRIMARY KEY check failed")
                pytest.fail(
                    f"Bug 5 DETECTED: Could not verify PRIMARY KEY on mv_refresh_metadata. "
                    f"Error: {e}"
                )
            except Exception as e2:
                pytest.skip(f"mv_refresh_metadata table does not exist: {e2}")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "-s"])
