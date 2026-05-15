"""Tests for query result caching in DuckDB connector."""
import pytest
from datetime import date
from unittest.mock import patch, MagicMock


class TestQueryResultCaching:
    """Test query result caching layer."""

    def test_query_sales_trends_uses_cache(self):
        """query_sales_trends should use lru_cache for identical queries."""
        from services.duckdb_connector import query_sales_trends
        
        # First call - should execute query
        with patch('services.duckdb_connector.get_duckdb_connection') as mock_conn:
            mock_result = MagicMock()
            mock_result.fetchdf.return_value = __import__('pandas').DataFrame()
            mock_conn.return_value.execute.return_value = mock_result
            
            result1 = query_sales_trends(date(2025, 1, 1), date(2025, 1, 31))
        
        # Second call with same parameters - should use cache
        with patch('services.duckdb_connector.get_duckdb_connection') as mock_conn:
            mock_result = MagicMock()
            mock_result.fetchdf.return_value = __import__('pandas').DataFrame()
            mock_conn.return_value.execute.return_value = mock_result
            
            result2 = query_sales_trends(date(2025, 1, 1), date(2025, 1, 31))
        
        # Cache should have hit (check cache_info)
        assert query_sales_trends.cache_info().hits >= 1
        assert query_sales_trends.cache_info().currsize >= 1

    def test_query_sales_trends_different_params(self):
        """Different query parameters should not use cache."""
        from services.duckdb_connector import query_sales_trends
        
        with patch('services.duckdb_connector.get_duckdb_connection') as mock_conn:
            mock_result = MagicMock()
            mock_result.fetchdf.return_value = __import__('pandas').DataFrame()
            mock_conn.return_value.execute.return_value = mock_result
            
            # Different date range
            result1 = query_sales_trends(date(2025, 1, 1), date(2025, 1, 31))
            result2 = query_sales_trends(date(2025, 2, 1), date(2025, 2, 28))
        
        # Cache should have 2 entries
        assert query_sales_trends.cache_info().currsize == 2

    def test_clear_sales_caches_clears_all_caches(self):
        """clear_sales_caches should clear all query caches."""
        from services.duckdb_connector import (
            query_sales_trends, query_top_products, 
            query_revenue_comparison, query_overview_summary,
            clear_sales_caches
        )
        
        # Populate caches
        with patch('services.duckdb_connector.get_duckdb_connection') as mock_conn:
            mock_result = MagicMock()
            mock_result.fetchdf.return_value = __import__('pandas').DataFrame()
            mock_conn.return_value.execute.return_value = mock_result
            
            try:
                query_sales_trends(date(2025, 1, 1), date(2025, 1, 31))
                query_top_products(date(2025, 1, 1), date(2025, 1, 31))
                query_revenue_comparison(date(2025, 1, 1), date(2025, 1, 31))
                query_overview_summary(date(2025, 1, 1), date(2025, 1, 31))
            except Exception:
                pass  # May fail due to mocking, but cache is populated
        
        # Verify cache has entries
        assert query_sales_trends.cache_info().currsize >= 1
        
        # Clear caches
        clear_sales_caches()
        
        # Verify all caches are cleared
        assert query_sales_trends.cache_info().currsize == 0
        assert query_top_products.cache_info().currsize == 0
        assert query_revenue_comparison.cache_info().currsize == 0
        assert query_overview_summary.cache_info().currsize == 0
