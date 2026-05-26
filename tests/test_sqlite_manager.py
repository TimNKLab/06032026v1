import pytest
import os
import sys
import tempfile
import sqlite3
from datetime import date

# Add project root to sys.path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.sqlite_manager import SQLiteManager

def test_initialize_db_creates_metadata_table():
    """Test that initialize_db creates the metadata table."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.sqlite') as f:
        db_path = f.name
    
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
        os.unlink(db_path)

def test_reader_conn_context_manager():
    """Test that reader_conn properly closes connection."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.sqlite') as f:
        db_path = f.name
    
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
        os.unlink(db_path)

def test_writer_conn_reuse():
    """Test that writer_conn returns same connection instance."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.sqlite') as f:
        db_path = f.name
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        conn1 = manager.get_writer_conn()
        conn2 = manager.get_writer_conn()
        assert conn1 is conn2
    finally:
        os.unlink(db_path)

def test_get_metadata_returns_none_for_nonexistent_view():
    """Test that get_metadata returns None for non-existent view."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.sqlite') as f:
        db_path = f.name
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        metadata = manager.get_metadata("nonexistent")
        assert metadata is None
    finally:
        os.unlink(db_path)

def test_get_metadata_returns_correct_data():
    """Test that get_metadata returns correct metadata."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.sqlite') as f:
        db_path = f.name
    
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
        os.unlink(db_path)

def test_get_refresh_strategy_returns_full_for_first_run():
    """Test that get_refresh_strategy returns 'full' for first run."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.sqlite') as f:
        db_path = f.name
    
    try:
        manager = SQLiteManager(db_path)
        manager.initialize_db()
        
        strategy, max_date = manager.get_refresh_strategy("test_view")
        assert strategy == "full"
        assert max_date is None
    finally:
        os.unlink(db_path)

def test_get_refresh_strategy_returns_incremental_for_existing_view():
    """Test that get_refresh_strategy returns 'incremental' for existing view."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.sqlite') as f:
        db_path = f.name
    
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
        os.unlink(db_path)
