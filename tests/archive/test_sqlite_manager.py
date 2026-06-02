import pytest
import os
import sys
import tempfile
import sqlite3
from datetime import date
import polars as pl

# Add project root to sys.path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.sqlite_manager import SQLiteManager

def test_initialize_db_creates_metadata_table():
    """Test that initialize_db creates the metadata table."""
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        with manager.reader_conn() as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='mv_refresh_metadata'"
            ).fetchall()
            assert len(tables) == 1
            assert tables[0][0] == 'mv_refresh_metadata'
    finally:
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                pass  # Windows file locking

def test_reader_conn_context_manager():
    """Test that reader_conn properly closes connection."""
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        with manager.reader_conn() as conn:
            result = conn.execute("SELECT 1").fetchone()
            assert result[0] == 1
        
        # Connection should be closed now - verify by attempting to execute
        try:
            conn.execute("SELECT 1")
            assert False, "Connection should be closed"
        except sqlite3.ProgrammingError:
            # Expected - connection is closed
            pass
    finally:
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                pass  # Windows file locking

def test_writer_conn_reuse():
    """Test that writer_conn returns same connection instance."""
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        conn1 = manager.get_writer_conn()
        conn2 = manager.get_writer_conn()
        assert conn1 is conn2
        
        conn1.close()
    finally:
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                pass  # Windows file locking

def test_get_metadata_returns_none_for_nonexistent_view():
    """Test that get_metadata returns None for non-existent view."""
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        metadata = manager.get_metadata("nonexistent")
        assert metadata is None
    finally:
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                pass  # Windows file locking

def test_get_metadata_returns_correct_data():
    """Test that get_metadata returns correct metadata."""
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        with manager.reader_conn() as conn:
            conn.execute(
                "INSERT INTO mv_refresh_metadata VALUES (?, ?, ?, ?, ?)",
                ("test_view", "2026-05-26 12:00:00", "2026-05-25", 100, "incremental")
            )
            conn.commit()
        
        metadata = manager.get_metadata("test_view")
        assert metadata.view_name == "test_view"
        assert metadata.row_count == 100
        assert metadata.refresh_type == "incremental"
    finally:
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                pass  # Windows file locking

def test_get_refresh_strategy_returns_full_for_first_run():
    """Test that get_refresh_strategy returns 'full' for first run."""
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        strategy, max_date = manager.get_refresh_strategy("test_view")
        assert strategy == "full"
        assert max_date is None
    finally:
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                pass  # Windows file locking

def test_get_refresh_strategy_returns_incremental_for_existing_view():
    """Test that get_refresh_strategy returns 'incremental' for existing view."""
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        with manager.reader_conn() as conn:
            conn.execute(
                "INSERT INTO mv_refresh_metadata VALUES (?, ?, ?, ?, ?)",
                ("test_view", "2026-05-26 12:00:00", "2026-05-25", 100, "incremental")
            )
            conn.commit()
        
        strategy, max_date = manager.get_refresh_strategy("test_view")
        assert strategy == "incremental"
        assert max_date == date(2026, 5, 25)
    finally:
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                pass  # Windows file locking

def test_full_refresh_atomic_swap():
    """Test that atomic swap works correctly."""
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        # Create initial data
        df1 = pl.DataFrame({
            "date": ["2026-05-25"],
            "revenue": [100.0],
            "transactions": [10]
        })
        
        writer_conn = manager.get_writer_conn()
        result1 = manager._full_refresh_atomic_swap(writer_conn, "test_view", df1)
        assert result1.success
        assert result1.rows_affected == 1
        
        # Verify table exists with correct data
        with manager.reader_conn() as reader_conn:
            rows = reader_conn.execute("SELECT * FROM test_view").fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 100.0
        
        # Refresh with new data
        df2 = pl.DataFrame({
            "date": ["2026-05-26"],
            "revenue": [200.0],
            "transactions": [20]
        })
        
        result2 = manager._full_refresh_atomic_swap(writer_conn, "test_view", df2)
        assert result2.success
        assert result2.rows_affected == 1
        
        # Verify data was swapped
        with manager.reader_conn() as reader_conn:
            rows = reader_conn.execute("SELECT * FROM test_view").fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 200.0
        
        # Close writer connection before cleanup
        writer_conn.close()
    finally:
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                pass  # Windows file locking

def test_incremental_refresh():
    """Test that incremental refresh works correctly."""
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        # Create initial data with full refresh
        df1 = pl.DataFrame({
            "date": ["2026-05-25"],
            "revenue": [100.0],
            "transactions": [10]
        })
        
        writer_conn = manager.get_writer_conn()
        result1 = manager._full_refresh_atomic_swap(writer_conn, "test_view", df1)
        assert result1.success
        
        # Add incremental data
        df2 = pl.DataFrame({
            "date": ["2026-05-26"],
            "revenue": [200.0],
            "transactions": [20]
        })
        
        result2 = manager._incremental_refresh(writer_conn, "test_view", df2)
        assert result2.success
        assert result2.rows_affected == 1
        
        # Verify both rows exist
        with manager.reader_conn() as reader_conn:
            rows = reader_conn.execute("SELECT * FROM test_view ORDER BY date").fetchall()
            assert len(rows) == 2
            assert rows[0][1] == 100.0
            assert rows[1][1] == 200.0
        
        # Close writer connection before cleanup
        writer_conn.close()
    finally:
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                pass  # Windows file locking

def test_refresh_mv_dispatches_to_domain_method():
    """Test that refresh_mv dispatches to correct domain method."""
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        writer_conn = manager.get_writer_conn()
        
        # Sales domain is now implemented - should attempt to read from parquet
        # This will fail without actual parquet data, but that's expected
        result = manager.refresh_mv("mv_sales_daily", "sales", writer_conn)
        # Should return a RefreshResult (success=False due to missing data)
        assert result.view_name == "mv_sales_daily"
        assert not result.success  # Expected to fail without parquet data
        
        writer_conn.close()
    finally:
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                pass  # Windows file locking
