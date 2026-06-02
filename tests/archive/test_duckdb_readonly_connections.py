import duckdb
import os
import pytest

def test_readonly_connection_prevents_writes():
    """Test that read-only connections cannot write."""
    data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    db_path = f"{data_lake}/cache/nkdash.duckdb"
    
    # Create read-only connection
    conn = duckdb.connect(database=db_path, read_only=True)
    
    # Try to write - should fail
    with pytest.raises(Exception):
        conn.execute("CREATE TABLE test_table (id INTEGER)")
    
    conn.close()

def test_readonly_connection_can_read():
    """Test that read-only connections can read."""
    from services.duckdb_connector import DuckDBManager
    
    # Use DuckDBManager to get read-only connection
    manager = DuckDBManager()
    conn = manager.get_readonly_connection()
    
    # Try to read - should succeed
    result = conn.execute("SELECT 1").fetchone()
    assert result[0] == 1
    
    conn.close()
