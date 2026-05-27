import sqlite3
import os
import logging
from contextlib import contextmanager
from datetime import datetime, date
from dataclasses import dataclass
from typing import Optional
from services.data_validator import DataValidator
from services.performance_monitor import monitor_performance

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
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to initialize database: {e}")
            raise

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

    def _full_refresh_atomic_swap(self, conn: sqlite3.Connection, view_name: str, df) -> RefreshResult:
        """Full refresh with atomic swap - no downtime."""
        import time
        start = time.time()
        temp_name = f"{view_name}_new"
        old_name = f"{view_name}_old"
        
        try:
            # Step 1: Create temp table OUTSIDE the swap transaction
            df.to_pandas().to_sql(temp_name, conn, if_exists="replace", index=False)
            
            # Step 2: Atomic swap INSIDE transaction
            with conn:
                # Check if view exists for first run
                table_exists = conn.execute(
                    f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{view_name}'"
                ).fetchone()[0] > 0
                
                if table_exists:
                    conn.execute(f"ALTER TABLE {view_name} RENAME TO {old_name}")
                
                conn.execute(f"ALTER TABLE {temp_name} RENAME TO {view_name}")
                
                if table_exists:
                    conn.execute(f"DROP TABLE {old_name}")
                
                # Create index after swap (correct name)
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{view_name}_date ON {view_name}(date)")
                
                # Update metadata
                new_max_date = df["date"].max()
                new_row_count = len(df)
                conn.execute(
                    "INSERT OR REPLACE INTO mv_refresh_metadata VALUES (?, ?, ?, ?, ?)",
                    (view_name, datetime.now(), new_max_date, new_row_count, 'full')
                )
            
            # Step 3: WAL checkpoint after commit
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            
            duration = time.time() - start
            return RefreshResult(
                view_name=view_name,
                strategy="full",
                rows_affected=new_row_count,
                duration_seconds=duration,
                success=True
            )
        except Exception as e:
            logger.error(f"Full refresh failed for {view_name}: {e}")
            return RefreshResult(
                view_name=view_name,
                strategy="full",
                rows_affected=0,
                duration_seconds=time.time() - start,
                success=False,
                error_message=str(e)
            )

    def _incremental_refresh(self, conn: sqlite3.Connection, view_name: str, df) -> RefreshResult:
        """Incremental refresh with transaction protection."""
        import time
        start = time.time()
        
        try:
            with conn:
                # Insert new rows using executemany
                rows = df.to_pandas().itertuples(index=False, name=None)
                placeholders = ','.join('?' * len(df.columns))
                conn.executemany(
                    f"INSERT INTO {view_name} VALUES ({placeholders})",
                    rows
                )
                
                # Update metadata atomically
                new_max_date = df["date"].max()
                current_row_count = conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
                new_row_count = len(df) + current_row_count
                conn.execute(
                    "UPDATE mv_refresh_metadata SET max_data_date=?, row_count=?, last_refresh_date=?, refresh_type=? WHERE view_name=?",
                    (new_max_date, new_row_count, datetime.now(), 'incremental', view_name)
                )
            
            duration = time.time() - start
            return RefreshResult(
                view_name=view_name,
                strategy="incremental",
                rows_affected=len(df),
                duration_seconds=duration,
                success=True
            )
        except Exception as e:
            logger.error(f"Incremental refresh failed for {view_name}: {e}")
            return RefreshResult(
                view_name=view_name,
                strategy="incremental",
                rows_affected=0,
                duration_seconds=time.time() - start,
                success=False,
                error_message=str(e)
            )

    def _refresh_sales_daily(self, conn: sqlite3.Connection, max_date: Optional[date]) -> RefreshResult:
        """Refresh mv_sales_daily using Polars parquet reads, SQLite for storage."""
        import polars as pl
        import time
        import os
        start = time.time()
        
        try:
            data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
            parquet_path = f"{data_lake_root}/star-schema/agg_sales_daily/**/*.parquet"
            
            if max_date is None:
                # First run or full refresh - use Polars lazy scan
                df = pl.scan_parquet(parquet_path, hive_partitioning=True).collect()
            else:
                # Incremental load - filter by date using Polars lazy scan
                df = pl.scan_parquet(parquet_path, hive_partitioning=True).filter(
                    pl.col("date") > max_date
                ).collect()
            
            # Validate data quality
            validator = DataValidator()
            expected_cols = ["date", "revenue", "transactions", "items_sold", "lines"]
            if not validator.validate_schema(df, expected_cols):
                raise ValueError(f"Schema validation failed: {validator.get_errors()}")
            
            if not validator.validate_row_count(df, min_rows=1):
                raise ValueError(f"Row count validation failed: {validator.get_errors()}")
            
            if max_date is None:
                return self._full_refresh_atomic_swap(conn, "mv_sales_daily", df)
            else:
                return self._incremental_refresh(conn, "mv_sales_daily", df)
        except Exception as e:
            logger.error(f"Sales daily refresh failed: {e}")
            return RefreshResult(
                view_name="mv_sales_daily",
                strategy="incremental" if max_date else "full",
                rows_affected=0,
                duration_seconds=time.time() - start,
                success=False,
                error_message=str(e)
            )
    
    def _refresh_sales_by_product(self, conn: sqlite3.Connection, max_date: Optional[date]) -> RefreshResult:
        """Refresh mv_sales_by_product using Polars parquet reads, SQLite for storage."""
        import polars as pl
        import time
        import os
        start = time.time()
        
        try:
            data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
            parquet_path = f"{data_lake_root}/star-schema/agg_sales_daily_by_product/**/*.parquet"
            
            if max_date is None:
                df = pl.scan_parquet(parquet_path, hive_partitioning=True).collect()
            else:
                df = pl.scan_parquet(parquet_path, hive_partitioning=True).filter(
                    pl.col("date") > max_date
                ).collect()
            
            # Validate data quality
            validator = DataValidator()
            expected_cols = ["date", "product_id", "revenue", "quantity"]
            if not validator.validate_schema(df, expected_cols):
                raise ValueError(f"Schema validation failed: {validator.get_errors()}")
            
            if not validator.validate_row_count(df, min_rows=1):
                raise ValueError(f"Row count validation failed: {validator.get_errors()}")
            
            if max_date is None:
                return self._full_refresh_atomic_swap(conn, "mv_sales_by_product", df)
            else:
                return self._incremental_refresh(conn, "mv_sales_by_product", df)
        except Exception as e:
            logger.error(f"Sales by product refresh failed: {e}")
            return RefreshResult(
                view_name="mv_sales_by_product",
                strategy="incremental" if max_date else "full",
                rows_affected=0,
                duration_seconds=time.time() - start,
                success=False,
                error_message=str(e)
            )
    
    def _refresh_sales_by_principal(self, conn: sqlite3.Connection, max_date: Optional[date]) -> RefreshResult:
        """Refresh mv_sales_by_principal using Polars parquet reads, SQLite for storage."""
        import polars as pl
        import time
        import os
        start = time.time()
        
        try:
            data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
            parquet_path = f"{data_lake_root}/star-schema/agg_sales_daily_by_principal/**/*.parquet"
            
            if max_date is None:
                df = pl.scan_parquet(parquet_path, hive_partitioning=True).collect()
            else:
                df = pl.scan_parquet(parquet_path, hive_partitioning=True).filter(
                    pl.col("date") > max_date
                ).collect()
            
            # Validate data quality
            validator = DataValidator()
            expected_cols = ["date", "principal", "revenue"]
            if not validator.validate_schema(df, expected_cols):
                raise ValueError(f"Schema validation failed: {validator.get_errors()}")
            
            if not validator.validate_row_count(df, min_rows=1):
                raise ValueError(f"Row count validation failed: {validator.get_errors()}")
            
            if max_date is None:
                return self._full_refresh_atomic_swap(conn, "mv_sales_by_principal", df)
            else:
                return self._incremental_refresh(conn, "mv_sales_by_principal", df)
        except Exception as e:
            logger.error(f"Sales by principal refresh failed: {e}")
            return RefreshResult(
                view_name="mv_sales_by_principal",
                strategy="incremental" if max_date else "full",
                rows_affected=0,
                duration_seconds=time.time() - start,
                success=False,
                error_message=str(e)
            )
    
    def _refresh_profit_daily(self, conn: sqlite3.Connection, max_date: Optional[date]) -> RefreshResult:
        """Refresh mv_profit_daily using Polars parquet reads, SQLite for storage."""
        import polars as pl
        import time
        import os
        start = time.time()
        
        try:
            data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
            parquet_path = f"{data_lake_root}/star-schema/agg_profit_daily/**/*.parquet"
            
            if max_date is None:
                df = pl.scan_parquet(parquet_path, hive_partitioning=True).collect()
            else:
                df = pl.scan_parquet(parquet_path, hive_partitioning=True).filter(
                    pl.col("date") > max_date
                ).collect()
            
            # Validate data quality
            validator = DataValidator()
            expected_cols = ["date", "revenue_tax_in", "gross_profit", "cost"]
            if not validator.validate_schema(df, expected_cols):
                raise ValueError(f"Schema validation failed: {validator.get_errors()}")
            
            if not validator.validate_row_count(df, min_rows=1):
                raise ValueError(f"Row count validation failed: {validator.get_errors()}")
            
            if max_date is None:
                return self._full_refresh_atomic_swap(conn, "mv_profit_daily", df)
            else:
                return self._incremental_refresh(conn, "mv_profit_daily", df)
        except Exception as e:
            logger.error(f"Profit daily refresh failed: {e}")
            return RefreshResult(
                view_name="mv_profit_daily",
                strategy="incremental" if max_date else "full",
                rows_affected=0,
                duration_seconds=time.time() - start,
                success=False,
                error_message=str(e)
            )
    
    def _refresh_inventory_daily(self, conn: sqlite3.Connection, max_date: Optional[date]) -> RefreshResult:
        """Refresh mv_inventory_daily using Polars parquet reads, SQLite for storage."""
        import polars as pl
        import time
        import os
        start = time.time()
        
        try:
            data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
            parquet_path = f"{data_lake_root}/star-schema/fact_stock_on_hand_snapshot/**/*.parquet"
            
            # Note: Inventory is a snapshot, always full refresh
            df = pl.scan_parquet(parquet_path, hive_partitioning=True).collect()
            
            # Validate data quality
            validator = DataValidator()
            expected_cols = ["snapshot_date", "product_id", "qty_on_hand"]
            if not validator.validate_schema(df, expected_cols):
                raise ValueError(f"Schema validation failed: {validator.get_errors()}")
            
            if not validator.validate_row_count(df, min_rows=1):
                raise ValueError(f"Row count validation failed: {validator.get_errors()}")
            
            return self._full_refresh_atomic_swap(conn, "mv_inventory_daily", df)
        except Exception as e:
            logger.error(f"Inventory daily refresh failed: {e}")
            return RefreshResult(
                view_name="mv_inventory_daily",
                strategy="full",
                rows_affected=0,
                duration_seconds=time.time() - start,
                success=False,
                error_message=str(e)
            )

    @monitor_performance("refresh_mv")
    def refresh_mv(self, view_name: str, domain: str, conn: sqlite3.Connection, 
                   date_range: Optional[tuple[str, str]] = None) -> RefreshResult:
        """Refresh MV based on domain-specific strategy."""
        strategy, max_date = self.get_refresh_strategy(view_name)
        
        if date_range:
            strategy = "full"
        
        if domain == "sales":
            if view_name == "mv_sales_daily":
                return self._refresh_sales_daily(conn, max_date)
            elif view_name == "mv_sales_by_product":
                return self._refresh_sales_by_product(conn, max_date)
            elif view_name == "mv_sales_by_principal":
                return self._refresh_sales_by_principal(conn, max_date)
        elif domain == "profit":
            return self._refresh_profit_daily(conn, max_date)
        elif domain == "inventory":
            return self._refresh_inventory_daily(conn, max_date)
        
        return RefreshResult(
            view_name=view_name,
            strategy=strategy,
            rows_affected=0,
            duration_seconds=0,
            success=False,
            error_message=f"Unknown view: {view_name}"
        )
