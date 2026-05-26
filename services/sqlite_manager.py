import sqlite3
import os
import logging
from contextlib import contextmanager
from datetime import datetime, date
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

@dataclass
class MVMetadata:
    view_name: str
    last_refresh_date: datetime
    max_data_date: date
    row_count: int
    refresh_type: str

@dataclass
class RefreshResult:
    view_name: str
    strategy: str
    rows_affected: int
    duration_seconds: float
    success: bool
    error_message: Optional[str] = None

class SQLiteManager:
    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            data_lake = os.environ.get('DATA_LAKE_ROOT') or os.environ.get('DATA_LAKE_PATH', '/data-lake')
            db_path = f"{data_lake}/cache/nkdash.sqlite"
        self.db_path = db_path
        self._writer_conn: Optional[sqlite3.Connection] = None

    @contextmanager
    def reader_conn(self):
        """Context manager for short-lived reader connections."""
        conn = self._create_conn()
        try:
            yield conn
        finally:
            conn.close()

    def get_writer_conn(self) -> sqlite3.Connection:
        """Long-lived connection for Celery writes."""
        if self._writer_conn is None:
            self._writer_conn = self._create_conn()
        return self._writer_conn

    def _create_conn(self) -> sqlite3.Connection:
        """Create a new SQLite connection with WAL mode."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def initialize_db(self) -> None:
        """Create metadata table, enable WAL mode, create indexes."""
        conn = self.get_writer_conn()
        try:
            # Create metadata table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mv_refresh_metadata (
                    view_name VARCHAR PRIMARY KEY,
                    last_refresh_date TIMESTAMP,
                    max_data_date DATE,
                    row_count BIGINT,
                    refresh_type VARCHAR
                )
            """)
            
            # Clean up orphaned temp tables from previous crashes
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE '%_new' OR name LIKE '%_old')"
            ).fetchall()
            for (table,) in tables:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
                logger.warning(f"Cleaned up orphaned temp table: {table}")
            
            conn.commit()
            logger.info("SQLite database initialized")
        finally:
            conn.close()

    def get_metadata(self, view_name: str) -> Optional[MVMetadata]:
        """Get refresh metadata for a view."""
        with self.reader_conn() as conn:
            row = conn.execute(
                "SELECT view_name, last_refresh_date, max_data_date, row_count, refresh_type "
                "FROM mv_refresh_metadata WHERE view_name=?",
                (view_name,)
            ).fetchone()
            
            if row is None:
                return None
            
            return MVMetadata(
                view_name=row[0],
                last_refresh_date=datetime.fromisoformat(row[1]),
                max_data_date=date.fromisoformat(row[2]),
                row_count=row[3],
                refresh_type=row[4]
            )

    def get_refresh_strategy(self, view_name: str) -> tuple[str, Optional[date]]:
        """Determine refresh strategy (full/incremental) and max_date."""
        with self.reader_conn() as conn:
            row = conn.execute(
                "SELECT max_data_date FROM mv_refresh_metadata WHERE view_name=?",
                (view_name,)
            ).fetchone()
            
            if row is None or row[0] is None:
                return "full", None  # First run or recovery
            return "incremental", date.fromisoformat(row[0])
