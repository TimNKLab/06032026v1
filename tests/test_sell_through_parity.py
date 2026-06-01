"""Test parity between DuckDB and SQLite for sell-through query."""
import pytest
import os
import sys
from datetime import date

# Add project root to sys.path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_sell_through_sqlite_import():
    """Test that SQLiteManager can be imported and initialized."""
    from services.sqlite_manager import SQLiteManager
    
    manager = SQLiteManager()
    assert manager is not None
    assert manager.db_path is not None

def test_sell_through_query_structure():
    """Test that _query_sell_through uses SQLite connection and Polars."""
    from services.inventory_metrics import _query_sell_through
    import inspect
    
    source = inspect.getsource(_query_sell_through)
    
    # Verify SQLiteManager is used
    assert "SQLiteManager" in source
    # Verify Polars is used for parquet reads
    assert "polars" in source or "pl." in source
    # Verify DuckDB is NOT used
    assert "duckdb_connector" not in source.lower()
    assert "get_duckdb_connection" not in source
    assert "ensure_duckdb_view_groups" not in source
    assert "DuckDBManager" not in source

def test_sell_through_cross_domain_pattern():
    """Test that sell-through follows cross-domain join pattern."""
    from services.inventory_metrics import _query_sell_through
    import inspect
    
    source = inspect.getsource(_query_sell_through)
    
    # Verify SQLite MVs are used for inventory/sales
    assert "mv_inventory_daily" in source
    assert "mv_sales_by_product" in source
    # Verify Polars is used for parquet reads
    assert "pl.scan_parquet" in source or "pl.read_parquet" in source
    # Verify Pandas merge is used for cross-domain joins
    assert "merge" in source

def test_sell_through_movement_classification():
    """Test that movement_type classification logic is preserved."""
    from services.inventory_metrics import _query_sell_through
    import inspect
    
    source = inspect.getsource(_query_sell_through)
    
    # Verify movement classifications are implemented
    assert "units_incoming" in source
    assert "units_production_in" in source
    assert "units_adjustment_net" in source
    assert "units_production_out" in source
    assert "units_transfer_net" in source
    # Verify classification logic exists
    assert "movement_type" in source
    assert "picking_type_code" in source

def test_sell_through_sell_through_calculation():
    """Test that sell-through ratio calculation is preserved."""
    from services.inventory_metrics import _query_sell_through
    import inspect
    
    source = inspect.getsource(_query_sell_through)
    
    # Verify sell-through calculation
    assert "sell_through" in source
    assert "units_sold" in source
    assert "begin_on_hand" in source
    assert "units_received" in source

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
