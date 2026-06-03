"""Test parity for DuckDB cleanup in inventory metrics."""
import pytest
import os
import sys
from datetime import date

# Add project root to sys.path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_query_inventory_summary_sqlite_import():
    """Test that query_inventory_summary uses SQLiteManager."""
    from services.inventory_metrics import query_inventory_summary
    import inspect
    
    source = inspect.getsource(query_inventory_summary)
    
    # Verify SQLiteManager is used
    assert "SQLiteManager" in source
    # Verify DuckDB is NOT used
    assert "duckdb_connector" not in source.lower()
    assert "get_duckdb_connection" not in source
    assert "ensure_duckdb_view_groups" not in source

def test_query_location_ledger_deltas_polars():
    """Test that _query_location_ledger_deltas uses Polars parquet reads."""
    from services.inventory_metrics import _query_location_ledger_deltas
    import inspect
    
    source = inspect.getsource(_query_location_ledger_deltas)
    
    # Verify Polars is used
    assert "polars" in source or "pl." in source
    # Verify DuckDB is NOT used
    assert "duckdb_connector" not in source.lower()
    assert "get_duckdb_connection" not in source
    assert "ensure_duckdb_view_groups" not in source

def test_get_inventory_costs_polars():
    """Test that get_inventory_costs uses Polars parquet reads."""
    from services.inventory_metrics import get_inventory_costs
    import inspect
    
    source = inspect.getsource(get_inventory_costs)
    
    # Verify Polars is used
    assert "polars" in source or "pl." in source
    # Verify DuckDB is NOT used
    assert "duckdb_connector" not in source.lower()
    assert "get_duckdb_connection" not in source
    assert "ensure_duckdb_view_groups" not in source

def test_inventory_metrics_no_duckdb_imports():
    """Test that inventory_metrics.py has no DuckDB imports."""
    with open('services/inventory_metrics.py', 'r') as f:
        content = f.read()
    
    # Verify no DuckDB imports
    assert "from services.duckdb_connector import" not in content
    assert "import duckdb" not in content

def test_app_mv_diagnostics_sqlite():
    """Test that app.py mv_diagnostics uses SQLite."""
    with open('app.py', 'r') as f:
        content = f.read()
    
    # Find mv_diagnostics function
    assert "def mv_diagnostics():" in content
    
    # Verify SQLiteManager is used in mv_diagnostics
    diagnostics_start = content.find("def mv_diagnostics():")
    diagnostics_end = content.find("\n\n", diagnostics_start)
    diagnostics_section = content[diagnostics_start:diagnostics_end]
    
    assert "SQLiteManager" in diagnostics_section
    assert "duckdb_connector" not in diagnostics_section.lower()

def test_app_health_check_sqlite():
    """Test that app.py health_check uses SQLite."""
    with open('app.py', 'r') as f:
        content = f.read()
    
    # Find health_check function
    assert "def health_check():" in content
    
    # Verify SQLiteManager is used in health_check
    health_start = content.find("def health_check():")
    health_end = content.find("\n\n", health_start)
    health_section = content[health_start:health_end]
    
    assert "SQLiteManager" in health_section
    assert "duckdb_connector" not in health_section.lower()

def test_app_no_precreate_views():
    """Test that app.py has no _precreate_views function."""
    with open('app.py', 'r') as f:
        content = f.read()
    
    # Verify _precreate_views is removed
    assert "def _precreate_views()" not in content
    assert "ensure_duckdb_view_groups" not in content
    assert "ensure_materialized_views" not in content

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
