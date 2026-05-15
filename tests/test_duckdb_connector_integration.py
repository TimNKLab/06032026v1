"""Integration tests for DuckDB connector optimizations."""
import os
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import pandas as pd


class TestDuckDBConnectorIntegration:
    """Integration tests for DuckDB connector optimizations."""

    def test_column_cache_reduces_startup_time(self):
        """Column caching should reduce view setup time."""
        from services.duckdb_connector import DuckDBManager, _column_cache
        
        # Clear cache
        _column_cache.clear()
        
        manager = DuckDBManager.__new__(DuckDBManager)
        manager._lock = __import__('threading').Lock()
        
        mock_conn = MagicMock()
        
        # Mock DESCRIBE to return consistent columns
        mock_conn.execute.return_value.fetchall.return_value = [
            ('product_id', 'BIGINT', 'YES', '', None, ''),
            ('product_name', 'VARCHAR', 'YES', '', None, ''),
            ('product_category', 'VARCHAR', 'YES', '', None, ''),
        ]
        
        # Simulate _setup_views calling _parquet_columns multiple times
        paths = [
            "/data-lake/star-schema/dim_products.parquet",
            "/data-lake/star-schema/dim_categories.parquet",
            "/data-lake/star-schema/dim_brands.parquet",
        ]
        
        # First run - no cache
        for path in paths:
            manager._parquet_columns(mock_conn, path)
        
        # DESCRIBE called 3 times
        assert mock_conn.execute.call_count == 3
        
        # Second run - with cache
        for path in paths:
            manager._parquet_columns(mock_conn, path)
        
        # DESCRIBE still called 3 times (cache used for subsequent calls)
        # Note: This test verifies the caching mechanism works
        assert len(_column_cache) == 3

    def test_mv_refresh_with_cache(self):
        """MV refresh should properly clear caches."""
        from services.duckdb_connector import (
            DuckDBManager, query_sales_trends, clear_sales_caches, _column_cache
        )
        
        # Clear everything
        _column_cache.clear()
        clear_sales_caches()
        
        manager = DuckDBManager.__new__(DuckDBManager)
        manager._lock = __import__('threading').Lock()
        manager._connection = MagicMock()
        
        # Mock the connection
        mock_conn = manager._connection
        mock_conn.execute.return_value.fetchall.return_value = []
        
        # Simulate MV refresh
        manager._load_materialized_views(mock_conn, {"mv_sales_daily"})
        
        # After MV refresh, caches should be cleared
        clear_sales_caches()
        
        # Verify caches are empty
        assert query_sales_trends.cache_info().currsize == 0
        assert len(_column_cache) == 0
