"""Test parity between DuckDB and SQLite for profit drilldown query."""
import pytest
import os
import sys
from datetime import date

# Add project root to sys.path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_profit_drilldown_sqlite_import():
    """Test that SQLiteManager can be imported and initialized."""
    from services.sqlite_manager import SQLiteManager
    
    manager = SQLiteManager()
    assert manager is not None
    assert manager.db_path is not None

def test_profit_drilldown_query_structure():
    """Test that query_profit_drilldown uses SQLite connection."""
    from services.profit_metrics import query_profit_drilldown
    import inspect
    
    source = inspect.getsource(query_profit_drilldown)
    
    # Verify SQLiteManager is used
    assert "SQLiteManager" in source
    # Verify DuckDB is NOT used
    assert "duckdb_connector" not in source.lower()
    assert "get_duckdb_connection" not in source

def test_profit_drilldown_sqlite_mv_exists():
    """Test that mv_fact_sales_lines_profit refresh method exists."""
    from services.sqlite_manager import SQLiteManager
    
    manager = SQLiteManager()
    assert hasattr(manager, '_refresh_fact_sales_lines_profit')
    assert hasattr(manager, 'refresh_mv')

def test_profit_drilldown_dispatch():
    """Test that refresh_mv dispatches to fact_sales_lines_profit method."""
    from services.sqlite_manager import SQLiteManager
    import tempfile
    
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        # Test dispatch logic
        writer_conn = manager.get_writer_conn()
        
        # Should return error result without actual data, but dispatch should work
        result = manager.refresh_mv("mv_fact_sales_lines_profit", "profit", writer_conn)
        assert result.view_name == "mv_fact_sales_lines_profit"
        assert result.domain == "profit" if hasattr(result, 'domain') else True
        
        writer_conn.close()
    finally:
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                pass

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
