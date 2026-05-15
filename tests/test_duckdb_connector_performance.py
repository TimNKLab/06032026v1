"""Tests for column detection caching in DuckDB connector."""
import pytest
from unittest.mock import MagicMock, patch
from services.duckdb_connector import DuckDBManager


class TestColumnDetectionCaching:
    """Test _parquet_columns caching behavior."""

    def test_column_detection_caches_results(self):
        """Repeated calls with same path should cache results."""
        manager = DuckDBManager.__new__(DuckDBManager)
        manager._lock = __import__('threading').Lock()
        
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ('product_id', 'BIGINT', 'YES', '', None, ''),
            ('product_name', 'VARCHAR', 'YES', '', None, ''),
        ]
        
        # First call - should execute DESCRIBE
        result1 = manager._parquet_columns(mock_conn, "/fake/path/products.parquet")
        assert 'product_id' in result1
        assert 'product_name' in result1
        
        # Second call with same path - should use cache
        result2 = manager._parquet_columns(mock_conn, "/fake/path/products.parquet")
        assert result1 == result2
        
        # DESCRIBE should only be called once
        mock_conn.execute.assert_called_once()

    def test_column_detection_different_paths(self):
        """Different paths should not use same cache entry."""
        manager = DuckDBManager.__new__(DuckDBManager)
        manager._lock = __import__('threading').Lock()
        
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ('col1', 'VARCHAR', 'YES', '', None, ''),
        ]
        
        result1 = manager._parquet_columns(mock_conn, "/fake/path1/file.parquet")
        result2 = manager._parquet_columns(mock_conn, "/fake/path2/file.parquet")
        
        # DESCRIBE should be called twice for different paths
        assert mock_conn.execute.call_count == 2
        assert result1 != result2  # Different results expected


class TestMVDateParsing:
    """Test materialized view date parsing optimization."""

    def test_mv_loading_uses_direct_date_column(self):
        """MV loading should use direct date column when available."""
        manager = DuckDBManager.__new__(DuckDBManager)
        manager._lock = __import__('threading').Lock()
        
        mock_conn = MagicMock()
        
        # Mock the _get_mv_refresh_info to return full refresh needed
        manager._get_mv_refresh_info = MagicMock(return_value=(True, None, 0))
        
        # Mock the connection to capture the SQL
        captured_sql = []
        def capture_execute(sql, *args):
            captured_sql.append(sql)
            return MagicMock()
        
        mock_conn.execute = capture_execute
        manager._load_materialized_views(mock_conn, {"mv_sales_daily"})
        
        # Check that the SQL uses direct date column, not MAKE_DATE parsing
        mv_sql = captured_sql[0]
        assert "date," in mv_sql.lower() or "date AS date" in mv_sql.lower()
        # Should NOT contain complex SPLIT_PART parsing
        assert "SPLIT_PART" not in mv_sql or "filename" not in mv_sql


class TestQueryTimeout:
    """Test query timeout configuration."""

    def test_execute_with_timeout_sets_timeout(self):
        """_execute_with_timeout should set statement_timeout before executing."""
        from services.duckdb_connector import _execute_with_timeout
        
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_conn.execute.return_value = mock_result
        
        # Mock the fetchdf to return empty DataFrame
        mock_result.fetchdf.return_value = __import__('pandas').DataFrame()
        
        _execute_with_timeout(mock_conn, "SELECT * FROM test", [1, 2])
        
        # Verify timeout was set
        mock_conn.execute.assert_any_call("SET statement_timeout=10000")
        # Verify query was executed
        mock_conn.execute.assert_any_call("SELECT * FROM test", [1, 2])

    def test_execute_with_timeout_raises_on_timeout(self):
        """_execute_with_timeout should handle timeout exceptions."""
        from services.duckdb_connector import _execute_with_timeout
        
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = [
            None,  # SET statement_timeout
            Exception("Statement timeout exceeded")  # Actual query
        ]
        
        with pytest.raises(Exception, match="Statement timeout exceeded"):
            _execute_with_timeout(mock_conn, "SELECT * FROM test")


class TestPerformanceMonitoring:
    """Test performance monitoring and timing metrics."""

    def test_query_timing_prints_duration(self):
        """Query functions should print timing information."""
        from services.duckdb_connector import query_sales_trends
        import io
        import sys
        
        # Capture stdout
        captured_output = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured_output
        
        try:
            with patch('services.duckdb_connector.get_duckdb_connection') as mock_conn:
                mock_result = MagicMock()
                mock_result.fetchdf.return_value = __import__('pandas').DataFrame()
                mock_conn.return_value.execute.return_value = mock_result
                
                query_sales_trends(date(2025, 1, 1), date(2025, 1, 31))
        except Exception:
            pass  # May fail due to mocking
        
        sys.stdout = old_stdout
        output = captured_output.getvalue()
        
        # Should contain timing information
        assert "[TIMING]" in output or "s]" in output
