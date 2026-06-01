"""Test cross-domain joins in inventory_metrics after SQLite migration."""
import pytest
import os
import sys
import tempfile
from datetime import date

# Add project root to sys.path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_get_snapshot_date_uses_sqlite():
    """Test that _get_snapshot_date uses SQLiteManager."""
    from services.inventory_metrics import _get_snapshot_date
    import inspect
    
    source = inspect.getsource(_get_snapshot_date)
    
    # Verify SQLiteManager is used
    assert "SQLiteManager" in source
    # Verify DuckDB is NOT used
    assert "duckdb_connector" not in source.lower()
    assert "get_duckdb_connection" not in source
    assert "ensure_duckdb_view_groups" not in source

def test_get_snapshot_date_sqlite_mv_query():
    """Test that _get_snapshot_date queries mv_inventory_daily."""
    from services.inventory_metrics import _get_snapshot_date
    import inspect
    
    source = inspect.getsource(_get_snapshot_date)
    
    # Verify it queries the SQLite MV
    assert "mv_inventory_daily" in source
    assert "snapshot_date" in source

def test_query_abc_products_uses_sqlite_and_polars():
    """Test that _query_abc_products uses SQLite + Polars for cross-domain join."""
    from services.inventory_metrics import _query_abc_products
    import inspect
    
    source = inspect.getsource(_query_abc_products)
    
    # Verify SQLiteManager is used for sales aggregates
    assert "SQLiteManager" in source
    # Verify Polars is used for dimensions
    assert "polars" in source.lower() or "pl." in source
    # Verify DuckDB is NOT used
    assert "get_duckdb_connection" not in source
    assert "ensure_duckdb_view_groups" not in source

def test_query_abc_products_sqlite_mv_query():
    """Test that _query_abc_products queries mv_sales_by_product."""
    from services.inventory_metrics import _query_abc_products
    import inspect
    
    source = inspect.getsource(_query_abc_products)
    
    # Verify it queries the SQLite MV for sales
    assert "mv_sales_by_product" in source
    # Verify it reads dimensions from parquet
    assert "dim_products.parquet" in source

def test_query_abc_products_join_pattern():
    """Test that _query_abc_products uses pandas merge for cross-domain join."""
    from services.inventory_metrics import _query_abc_products
    import inspect
    
    source = inspect.getsource(_query_abc_products)
    
    # Verify it uses pandas merge for joining
    assert "merge" in source

def test_query_stock_levels_uses_sqlite_and_polars():
    """Test that _query_stock_levels uses SQLite + Polars for cross-domain join."""
    from services.inventory_metrics import _query_stock_levels
    import inspect
    
    source = inspect.getsource(_query_stock_levels)
    
    # Verify SQLiteManager is used
    assert "SQLiteManager" in source
    # Verify Polars is used for dimensions
    assert "polars" in source.lower() or "pl." in source
    # Verify DuckDB is NOT used
    assert "get_duckdb_connection" not in source
    assert "ensure_duckdb_view_groups" not in source

def test_query_stock_levels_sqlite_mv_queries():
    """Test that _query_stock_levels queries both mv_inventory_daily and mv_sales_by_product."""
    from services.inventory_metrics import _query_stock_levels
    import inspect
    
    source = inspect.getsource(_query_stock_levels)
    
    # Verify it queries SQLite MVs
    assert "mv_inventory_daily" in source
    assert "mv_sales_by_product" in source
    # Verify it reads dimensions from parquet
    assert "dim_products.parquet" in source

def test_query_stock_levels_join_pattern():
    """Test that _query_stock_levels uses pandas merge for cross-domain joins."""
    from services.inventory_metrics import _query_stock_levels
    import inspect
    
    source = inspect.getsource(_query_stock_levels)
    
    # Verify it uses pandas merge for joining
    assert "merge" in source

def test_query_stock_levels_handles_missing_dimensions():
    """Test that _query_stock_levels handles missing dimension parquet gracefully."""
    from services.inventory_metrics import _query_stock_levels
    import inspect
    
    source = inspect.getsource(_query_stock_levels)
    
    # Verify it has fallback for missing dimensions
    assert "else:" in source or "except:" in source
    assert "fillna" in source  # Should fill missing values

def test_cross_domain_join_no_duckdb_dependencies():
    """Test that migrated functions have no DuckDB dependencies."""
    from services.inventory_metrics import _get_snapshot_date, _query_abc_products, _query_stock_levels
    import inspect
    
    for func in [_get_snapshot_date, _query_abc_products, _query_stock_levels]:
        source = inspect.getsource(func)
        assert "duckdb_connector" not in source.lower(), f"{func.__name__} still imports duckdb_connector"
        assert "get_duckdb_connection" not in source, f"{func.__name__} still uses get_duckdb_connection"
        assert "ensure_duckdb_view_groups" not in source, f"{func.__name__} still uses ensure_duckdb_view_groups"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
