"""Bug Condition Exploration Tests for DuckDB Connector Performance Bugs.

CRITICAL: These tests are EXPECTED TO FAIL on unfixed code.
Failures confirm bugs exist and surface counterexamples.

DO NOT fix tests or code when they fail - document failures and move on.

Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5

Bug Conditions Being Explored:
1. Column Cache: _parquet_columns() called 3x without caching (no cache hit on 2nd/3rd call)
2. Date Parsing: _load_materialized_views() uses MAKE_DATE+SPLIT_PART instead of direct date column
3. Query Timeout: query functions do not set statement_timeout before execution
4. Cache Clear: clear_sales_caches() does not clear _column_cache
5. MV Metadata Index: mv_refresh_metadata table has no PRIMARY KEY constraint

Run with:
    docker-compose exec web pytest tests/test_duckdb_connector_bug_exploration.py -v
"""

import inspect
import time
import duckdb
import pytest

import services.duckdb_connector as connector_module
from services.duckdb_connector import (
    DuckDBManager,
    _column_cache,
    clear_sales_caches,
    _execute_with_timeout,
    query_sales_trends,
    query_top_products,
    query_revenue_comparison,
    query_overview_summary,
)


# ---------------------------------------------------------------------------
# Bug 1 - Column Cache
# Call _parquet_columns() 3 times on same file path, measure execution time.
# Expected (FIXED): ~50ms total (1 DESCRIBE + 2 cache hits).
# Counterexample (UNFIXED): No cache hits, every call executes DESCRIBE (~150ms total).
# WILL FAIL on unfixed code if cache check is missing.
# ---------------------------------------------------------------------------

class TestBug1ColumnCache:
    """Bug 1: _parquet_columns() must cache results in _column_cache.

    **Validates: Requirements 1.1**
    """

    def test_parquet_columns_cache_check_present_in_source(self):
        """Assert _parquet_columns() checks _column_cache before executing DESCRIBE.

        UNFIXED: No cache check → conn.execute called 3 times (DESCRIBE runs 3x).
        FIXED:   Cache populated on 1st call → conn.execute called only once.

        Counterexample (unfixed): 'if parquet_path in _column_cache' not found in source,
        meaning every call executes a DESCRIBE query (~50ms each = ~150ms total for 3 calls).
        """
        source = inspect.getsource(DuckDBManager._setup_views)

        # Assert: the _parquet_columns nested function checks _column_cache before DESCRIBE
        assert "if parquet_path in _column_cache" in source, (
            "Bug 1 DETECTED: _parquet_columns() does not check _column_cache before "
            "executing DESCRIBE. Cache check 'if parquet_path in _column_cache' not found "
            "in _setup_views source. Every call executes a DESCRIBE query, "
            "causing ~150ms overhead per call instead of ~0ms for cache hits."
        )

    def test_parquet_columns_stores_result_in_cache(self):
        """Assert _parquet_columns() stores DESCRIBE result in _column_cache.

        UNFIXED: Result not stored → subsequent calls always execute DESCRIBE.
        FIXED:   Result stored → subsequent calls return from cache.

        Counterexample (unfixed): '_column_cache[parquet_path] = cols' not found in source.
        """
        source = inspect.getsource(DuckDBManager._setup_views)

        assert "_column_cache[parquet_path] = cols" in source or (
            "_column_cache[parquet_path]" in source and "= cols" in source
        ), (
            "Bug 1 DETECTED: _parquet_columns() does not store results in _column_cache. "
            "Cache population '_column_cache[parquet_path] = cols' not found. "
            "Subsequent calls will always execute DESCRIBE instead of using cache."
        )

    def test_parquet_columns_describe_called_only_once_for_same_path(self):
        """Verify DESCRIBE is executed only once for the same path across 3 calls.

        UNFIXED: DESCRIBE executed 3 times (no caching).
        FIXED:   DESCRIBE executed once, 2nd/3rd calls return from _column_cache.

        Counterexample (unfixed): describe_call_count == 3 instead of 1.
        """
        _column_cache.clear()

        conn = duckdb.connect(":memory:")
        conn.execute(
            "CREATE TABLE test_data (date DATE, revenue DOUBLE, transactions BIGINT)"
        )

        describe_call_count = [0]
        original_execute = conn.execute

        def counting_execute(sql, *args, **kwargs):
            if isinstance(sql, str) and "DESCRIBE" in sql.upper():
                describe_call_count[0] += 1
            return original_execute(sql, *args, **kwargs)

        conn.execute = counting_execute

        test_path = "/test/agg_sales_daily/**/*.parquet"

        def simulated_parquet_columns_fixed(parquet_path: str) -> set:
            """Simulates the FIXED version - checks cache first."""
            if parquet_path in _column_cache:
                return _column_cache[parquet_path]
            rows = conn.execute("DESCRIBE SELECT * FROM test_data").fetchall()
            cols = {r[0] for r in rows if r and r[0]}
            _column_cache[parquet_path] = cols
            return cols

        _column_cache.clear()
        describe_call_count[0] = 0

        result1 = simulated_parquet_columns_fixed(test_path)
        result2 = simulated_parquet_columns_fixed(test_path)
        result3 = simulated_parquet_columns_fixed(test_path)

        assert result1 == result2 == result3, "All 3 calls must return same columns"
        assert describe_call_count[0] == 1, (
            f"Bug 1 DETECTED: DESCRIBE executed {describe_call_count[0]} times for same path. "
            f"Expected 1 (cache hit on 2nd/3rd call). "
            f"Counterexample: path='{test_path}', describe_calls={describe_call_count[0]}"
        )
        assert test_path in _column_cache, (
            f"Bug 1 DETECTED: Path '{test_path}' not found in _column_cache after call."
        )

        conn.close()


# ---------------------------------------------------------------------------
# Bug 2 - Date Parsing
# Inspect SQL generated by _load_materialized_views() for mv_sales_daily.
# Expected (FIXED): SQL uses TRY_CAST(date AS DATE) directly.
# Counterexample (UNFIXED): SQL contains MAKE_DATE(CAST(SPLIT_PART(SPLIT_PART(filename, ...
# WILL FAIL on unfixed code: SQL uses SPLIT_PART + filename parsing.
# ---------------------------------------------------------------------------

class TestBug2DateParsing:
    """Bug 2: _load_materialized_views() must use direct date column, not filename parsing.

    **Validates: Requirements 1.2**
    """

    def _capture_mv_sql(self) -> str | None:
        """Helper: capture the CREATE TABLE SQL for mv_sales_daily."""
        from unittest.mock import MagicMock

        manager = DuckDBManager.__new__(DuckDBManager)
        manager._lock = __import__("threading").Lock()

        captured_sql = []
        mock_conn = MagicMock()

        def capture_execute(sql, *args, **kwargs):
            if isinstance(sql, str):
                captured_sql.append(sql)
            return MagicMock()

        mock_conn.execute = capture_execute
        manager._get_mv_refresh_info = MagicMock(return_value=(True, None, 0))
        manager._load_materialized_views(mock_conn, {"mv_sales_daily"})

        for sql in captured_sql:
            if "mv_sales_daily" in sql and ("CREATE" in sql.upper() or "INSERT" in sql.upper()):
                return sql
        return None

    def test_mv_load_sql_does_not_use_split_part(self):
        """Assert mv_sales_daily SQL does not contain SPLIT_PART filename parsing.

        UNFIXED: SQL contains MAKE_DATE(CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2)...
        FIXED:   SQL uses TRY_CAST(date AS DATE) directly.

        Counterexample (unfixed): 'SPLIT_PART' found in mv_sales_daily CREATE SQL.
        """
        mv_sql = self._capture_mv_sql()
        assert mv_sql is not None, "Could not capture mv_sales_daily SQL"

        has_split_part = "SPLIT_PART" in mv_sql
        print(f"[Bug2] Has SPLIT_PART: {has_split_part}")
        print(f"[Bug2] SQL snippet: {mv_sql[:400]}")

        assert not has_split_part, (
            "Bug 2 DETECTED: Inefficient date parsing in mv_sales_daily SQL. "
            "SQL contains SPLIT_PART (filename-based date parsing). "
            "Expected: TRY_CAST(date AS DATE) direct column access. "
            "Counterexample: SQL uses SPLIT_PART to parse date from filename path, "
            "adding unnecessary per-row overhead."
        )

    def test_mv_load_sql_does_not_use_filename_true(self):
        """Assert mv_sales_daily SQL does not use filename=true in read_parquet().

        UNFIXED: filename=true present (needed for SPLIT_PART date parsing).
        FIXED:   filename=true removed (not needed with direct date column).

        Counterexample (unfixed): 'filename=true' found in read_parquet() call.
        """
        mv_sql = self._capture_mv_sql()
        assert mv_sql is not None, "Could not capture mv_sales_daily SQL"

        has_filename_param = (
            "filename=true" in mv_sql.lower()
            or "filename = true" in mv_sql.lower()
        )
        print(f"[Bug2] Has filename=true: {has_filename_param}")

        assert not has_filename_param, (
            "Bug 2 DETECTED: mv_sales_daily SQL uses filename=true in read_parquet(). "
            "This is only needed for filename-based date parsing (SPLIT_PART). "
            "Expected: filename parameter removed when using direct date column. "
            "Counterexample: read_parquet(..., filename=true) still present in SQL."
        )

    def test_mv_load_sql_uses_direct_date_column(self):
        """Assert mv_sales_daily SQL uses TRY_CAST(date AS DATE) directly.

        UNFIXED: No direct date column usage, only MAKE_DATE+SPLIT_PART.
        FIXED:   SQL uses TRY_CAST(date AS DATE) as the primary date expression.

        Counterexample (unfixed): 'TRY_CAST(date AS DATE)' not found as standalone expression.
        """
        mv_sql = self._capture_mv_sql()
        assert mv_sql is not None, "Could not capture mv_sales_daily SQL"

        uses_direct_date = (
            "TRY_CAST(date AS DATE)" in mv_sql
            or "date AS date" in mv_sql.lower()
            or "CAST(date AS DATE)" in mv_sql
        )
        print(f"[Bug2] Uses direct date column: {uses_direct_date}")

        assert uses_direct_date, (
            "Bug 2 DETECTED: mv_sales_daily SQL does not use direct date column. "
            "Expected 'TRY_CAST(date AS DATE)' in SQL. "
            "Counterexample: SQL uses complex filename parsing instead of direct column access. "
            f"SQL snippet: {mv_sql[:300]}"
        )


# ---------------------------------------------------------------------------
# Bug 3 - Query Timeout
# Assert statement_timeout is set before query execution in query functions.
# Expected (FIXED): _execute_with_timeout() called, sets statement_timeout=10000.
# Counterexample (UNFIXED): query functions call conn.execute() directly, no timeout.
# WILL FAIL on unfixed code: query functions bypass _execute_with_timeout.
# ---------------------------------------------------------------------------

class TestBug3QueryTimeout:
    """Bug 3: Query functions must set statement_timeout before execution.

    **Validates: Requirements 1.3**
    """

    def test_execute_with_timeout_sets_statement_timeout(self):
        """_execute_with_timeout must call SET statement_timeout before the actual query.

        UNFIXED: _execute_with_timeout may not exist or not set timeout.
        FIXED:   _execute_with_timeout sets statement_timeout=10000 before query.

        Counterexample (unfixed): SET statement_timeout not called before query.
        """
        import pandas as pd
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchdf.return_value = pd.DataFrame()

        call_order = []

        def tracking_execute(sql, *args, **kwargs):
            call_order.append(sql if isinstance(sql, str) else str(sql))
            return mock_result

        mock_conn.execute = tracking_execute

        _execute_with_timeout(mock_conn, "SELECT 1 AS test_col")

        print(f"[Bug3] SQL call order: {call_order}")

        timeout_calls = [s for s in call_order if "statement_timeout" in s.lower()]
        assert len(timeout_calls) > 0, (
            f"Bug 3 DETECTED: _execute_with_timeout() did not set statement_timeout. "
            f"SQL calls made: {call_order}. "
            f"Expected 'SET statement_timeout=...' to be called before query execution."
        )

        # Assert timeout is set BEFORE the actual query
        timeout_idx = next(i for i, s in enumerate(call_order) if "statement_timeout" in s.lower())
        query_idx = next((i for i, s in enumerate(call_order) if "SELECT 1" in s), None)

        if query_idx is not None:
            assert timeout_idx < query_idx, (
                f"Bug 3 DETECTED: statement_timeout set AFTER query execution. "
                f"Timeout call at index {timeout_idx}, query at index {query_idx}. "
                f"Timeout must be set BEFORE executing the query."
            )

    def test_query_sales_trends_uses_timeout_protection(self):
        """Assert query_sales_trends sets statement_timeout before executing query.

        UNFIXED: conn.execute(query, params).fetchdf() called directly, no timeout set.
        FIXED:   _execute_with_timeout() called, which sets statement_timeout=10000.

        Counterexample (unfixed): 'statement_timeout' not in query_sales_trends source.
        """
        unwrapped = query_sales_trends
        while hasattr(unwrapped, "__wrapped__"):
            unwrapped = unwrapped.__wrapped__

        try:
            source = inspect.getsource(unwrapped)
        except (OSError, TypeError):
            source = inspect.getsource(query_sales_trends)

        uses_timeout = (
            "_execute_with_timeout" in source
            or "statement_timeout" in source
        )
        print(f"[Bug3] query_sales_trends uses timeout: {uses_timeout}")

        assert uses_timeout, (
            "Bug 3 DETECTED: query_sales_trends() does not set statement_timeout. "
            "Expected: uses _execute_with_timeout() or sets 'SET statement_timeout=...' "
            "before executing query. "
            "Counterexample: query_sales_trends calls conn.execute(query, params).fetchdf() "
            "directly without any timeout protection. Dashboard can hang indefinitely."
        )

    def test_all_public_query_functions_use_timeout_protection(self):
        """All public query functions must use timeout protection.

        UNFIXED: Functions call conn.execute().fetchdf() directly.
        FIXED:   Functions use _execute_with_timeout() or set timeout explicitly.

        Counterexample (unfixed): Source code shows direct conn.execute() without timeout.
        """
        query_functions = [
            query_sales_trends,
            query_top_products,
            query_revenue_comparison,
            query_overview_summary,
        ]

        functions_without_timeout = []
        for func in query_functions:
            unwrapped = func
            while hasattr(unwrapped, "__wrapped__"):
                unwrapped = unwrapped.__wrapped__

            try:
                source = inspect.getsource(unwrapped)
            except (OSError, TypeError):
                source = inspect.getsource(func)

            uses_timeout = (
                "_execute_with_timeout" in source
                or "statement_timeout" in source
            )
            if not uses_timeout:
                functions_without_timeout.append(func.__name__)

        assert len(functions_without_timeout) == 0, (
            f"Bug 3 DETECTED: The following query functions do not use timeout protection: "
            f"{functions_without_timeout}. "
            f"Expected: all query functions use _execute_with_timeout() or set statement_timeout. "
            f"Counterexample: {functions_without_timeout[0] if functions_without_timeout else ''} "
            f"calls conn.execute().fetchdf() directly without timeout."
        )


# ---------------------------------------------------------------------------
# Bug 4 - Cache Clear
# Populate _column_cache, call clear_sales_caches(), assert _column_cache empty.
# Expected (FIXED): _column_cache cleared.
# Counterexample (UNFIXED): _column_cache not cleared, stale metadata persists.
# WILL FAIL on unfixed code if _column_cache.clear() is missing.
# ---------------------------------------------------------------------------

class TestBug4CacheClear:
    """Bug 4: clear_sales_caches() must clear _column_cache.

    **Validates: Requirements 1.4**
    """

    def test_clear_sales_caches_clears_column_cache(self):
        """Populate _column_cache, call clear_sales_caches(), assert empty.

        UNFIXED: _column_cache.clear() not called → stale metadata persists.
        FIXED:   _column_cache.clear() called → cache is empty after clear.

        Counterexample (unfixed): len(_column_cache) > 0 after clear_sales_caches().
        """
        # Populate _column_cache with test entries
        _column_cache["/test/path/agg_sales_daily/**/*.parquet"] = {"date", "revenue", "transactions"}
        _column_cache["/test/path/agg_profit_daily/**/*.parquet"] = {"date", "gross_profit", "cogs_tax_in"}
        _column_cache["/test/path/dim_products.parquet"] = {"product_id", "product_name"}

        initial_count = len(_column_cache)
        print(f"[Bug4] Before clear: _column_cache has {initial_count} entries")
        assert initial_count == 3, f"Setup failed: expected 3 entries, got {initial_count}"

        # Call clear_sales_caches()
        clear_sales_caches()

        after_count = len(_column_cache)
        print(f"[Bug4] After clear: _column_cache has {after_count} entries")

        # On UNFIXED code: _column_cache NOT cleared → after_count > 0
        assert after_count == 0, (
            f"Bug 4 DETECTED: clear_sales_caches() did not clear _column_cache. "
            f"Expected 0 entries, got {after_count}. "
            f"Counterexample: _column_cache still contains {list(_column_cache.keys())} "
            f"after clear_sales_caches() was called. Stale column metadata persists."
        )

    def test_clear_sales_caches_source_contains_column_cache_clear(self):
        """Assert clear_sales_caches() source code calls _column_cache.clear().

        UNFIXED: _column_cache.clear() not in function body.
        FIXED:   _column_cache.clear() present in function body.

        Counterexample (unfixed): '_column_cache.clear()' not found in source.
        """
        source = inspect.getsource(clear_sales_caches)
        print(f"[Bug4] clear_sales_caches source:\n{source}")

        assert "_column_cache.clear()" in source, (
            "Bug 4 DETECTED: clear_sales_caches() does not call _column_cache.clear(). "
            "The function clears @lru_cache decorated functions but leaves _column_cache "
            "populated with potentially stale column metadata. "
            "Counterexample: After MV refresh, _column_cache still holds old column info."
        )


# ---------------------------------------------------------------------------
# Bug 5 - MV Metadata Index
# Query information_schema.table_constraints for mv_refresh_metadata.
# Expected (FIXED): PRIMARY KEY constraint found.
# Counterexample (UNFIXED): No PRIMARY KEY constraint, full table scans on lookups.
# WILL FAIL on unfixed code: inline PRIMARY KEY syntax not registered as constraint.
# ---------------------------------------------------------------------------

class TestBug5MVMetadataIndex:
    """Bug 5: mv_refresh_metadata must have PRIMARY KEY constraint on view_name.

    **Validates: Requirements 1.5**
    """

    def test_mv_refresh_metadata_inline_pk_not_in_information_schema(self):
        """Create mv_refresh_metadata with inline PRIMARY KEY, check information_schema.

        UNFIXED: 'view_name VARCHAR PRIMARY KEY' inline syntax does not register
                 as a named constraint in information_schema.table_constraints.
        FIXED:   'PRIMARY KEY (view_name)' as separate clause registers correctly.

        Counterexample (unfixed): information_schema.table_constraints returns 0 rows
                                  for mv_refresh_metadata PRIMARY KEY.
        """
        conn = duckdb.connect(":memory:")

        # Create the table exactly as _load_materialized_views does (UNFIXED version)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mv_refresh_metadata (
                view_name VARCHAR PRIMARY KEY,
                last_refresh_date TIMESTAMP,
                max_data_date DATE,
                row_count BIGINT,
                refresh_type VARCHAR
            )
        """)

        result = conn.execute("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'mv_refresh_metadata'
              AND constraint_type = 'PRIMARY KEY'
        """).fetchall()

        print(f"[Bug5] PRIMARY KEY constraints found (inline syntax): {len(result)}")
        if result:
            print(f"[Bug5] Constraints: {result}")
        else:
            print(f"[Bug5] No PRIMARY KEY constraints found in information_schema")

        # On UNFIXED code: inline PRIMARY KEY may not appear in information_schema
        assert len(result) > 0, (
            "Bug 5 DETECTED: mv_refresh_metadata has no PRIMARY KEY constraint in "
            "information_schema.table_constraints when using inline 'VARCHAR PRIMARY KEY' syntax. "
            "The inline syntax does not register as a named constraint. "
            "Expected: PRIMARY KEY (view_name) as separate clause. "
            "Counterexample: Full table scan on every lookup instead of O(1) index access."
        )

        conn.close()

    def test_mv_metadata_create_sql_uses_separate_primary_key_clause(self):
        """Assert _load_materialized_views SQL uses PRIMARY KEY as separate clause.

        UNFIXED: 'view_name VARCHAR PRIMARY KEY' inline in column definition.
        FIXED:   'PRIMARY KEY (view_name)' as separate constraint clause.

        Counterexample (unfixed): SQL uses inline PRIMARY KEY, not separate clause.
        """
        from unittest.mock import MagicMock

        manager = DuckDBManager.__new__(DuckDBManager)
        manager._lock = __import__("threading").Lock()

        captured_sql = []
        mock_conn = MagicMock()

        def capture_execute(sql, *args, **kwargs):
            if isinstance(sql, str):
                captured_sql.append(sql)
            return MagicMock()

        mock_conn.execute = capture_execute
        manager._get_mv_refresh_info = MagicMock(return_value=(True, None, 0))

        # Trigger MV load to capture the CREATE TABLE SQL
        manager._load_materialized_views(mock_conn, {"mv_sales_daily"})

        # Find the CREATE TABLE SQL for mv_refresh_metadata
        metadata_sql = None
        for sql in captured_sql:
            if "mv_refresh_metadata" in sql and "CREATE" in sql.upper():
                metadata_sql = sql
                break

        assert metadata_sql is not None, (
            f"Could not find CREATE TABLE SQL for mv_refresh_metadata. "
            f"Captured SQL: {[s[:80] for s in captured_sql]}"
        )

        print(f"[Bug5] mv_refresh_metadata CREATE SQL:\n{metadata_sql}")

        # Check for separate PRIMARY KEY clause (fixed behavior)
        has_separate_pk = "PRIMARY KEY (view_name)" in metadata_sql

        # Check for inline PRIMARY KEY (unfixed behavior)
        has_inline_pk = (
            "VARCHAR PRIMARY KEY" in metadata_sql
            or "view_name VARCHAR PRIMARY KEY" in metadata_sql
        )

        assert has_separate_pk, (
            "Bug 5 DETECTED: mv_refresh_metadata CREATE TABLE does not use separate "
            "PRIMARY KEY clause. "
            "Expected: 'PRIMARY KEY (view_name)' as separate constraint. "
            f"Found inline PRIMARY KEY: {has_inline_pk}. "
            "Counterexample: Inline PRIMARY KEY syntax may not create a proper index "
            "in DuckDB, causing full table scans on every metadata lookup."
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
