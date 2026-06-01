# SQLite Materialized View Migration Design

**Date:** 2026-05-26  
**Author:** Cascade AI  
**Status:** Approved  
**Workstream:** NK_20260526_sqlite_mv_migration  

## Overview

Migrate from DuckDB to SQLite for materialized view (MV) creation to eliminate lock conflicts, memory allocation failures, and crashes. Replace DuckDB entirely with Polars for parquet reads and SQLite for MV storage.

## Problem Statement

**Current Issues with DuckDB MVs:**
- Lock conflicts between dash-app and celery-worker
- Memory allocation failures during MV creation
- DuckDB crashes during heavy operations
- Complex coordination via Redis for MV refresh signals
- File lock contention on nkdash.duckdb

**Root Cause:**
DuckDB's in-memory MV creation pattern doesn't handle concurrent access well in the current architecture, leading to instability.

## Solution Architecture

### Data Flow

```
Parquet files (star-schema/, date-partitioned)
    ↓
Polars lazy scan + join (no DuckDB)
    ↓
Python aggregation
    ↓
SQLite (WAL mode)
    ├── Writes: Celery worker only (single writer connection)
    └── Reads: Dash app (short-lived connections, open → query → close)
```

### Key Design Decisions

1. **Remove DuckDB entirely**: No hybrid architecture - complete cutover
2. **Polars for parquet reads**: Lazy evaluation with date partitioning
3. **SQLite WAL mode**: Concurrent readers, single writer
4. **Domain-specific refresh strategies**: Different approaches per data type
5. **Explicit boundaries**: Celery writes only, dash reads only
6. **Atomic operations**: No downtime windows during refresh

### Parquet File Characteristics

**Partitioning:** All parquet files are date-partitioned (year/month/day hive structure)

**File counts:**
- agg_sales_daily: 431 files (daily partitions, Feb 2025 - May 2026)
- fact_sales: 420 files (daily partitions, Feb 2025 - May 2026)

**Row counts:**
- agg_sales_daily: 406 rows (daily aggregates)
- fact_sales: 5,016,281 rows (raw fact table)

**Conclusion:** Polars lazy scanning is fast enough to replace DuckDB for parquet reads.

## Materialized Views

### Current DuckDB MVs

| MV | Purpose | Refresh Frequency | Strategy |
|---|---|---|---|
| mv_sales_daily | Daily sales totals | Daily | Incremental by date |
| mv_sales_by_product | Daily sales by product | Daily | Incremental by date |
| mv_sales_by_principal | Daily sales by principal | Daily | Incremental by date |
| mv_profit_daily | Daily profit totals | Daily | Incremental by date |
| mv_inventory_daily | Daily inventory snapshots | Daily | Full refresh |
| mv_product_velocity | Product sales velocity | Weekly | Full refresh |
| mv_inventory_status | Inventory status with cover | On-demand | Full refresh |

### Refresh Strategy Details

**Incremental Refresh (append-only data):**
- Sales and profit MVs
- INSERT new rows WHERE date > max_date
- Transaction-wrapped for atomicity
- Metadata update in same transaction

**Full Refresh (snapshot data):**
- Inventory MVs
- Atomic swap pattern (no downtime)
- WAL checkpoint after refresh

**On-Demand Refresh:**
- mv_inventory_status
- Triggered via Celery task (not direct SQLite write)
- Dash app enqueues task, polls for completion

## SQLiteManager Design

### Connection Pattern

```python
class SQLiteManager:
    @contextmanager
    def reader_conn(self) -> sqlite3.Connection:
        """Context manager for short-lived reader connections.
        Automatically closes connection after use."""
        conn = self._create_conn()
        try:
            yield conn
        finally:
            conn.close()
        
    def get_writer_conn(self) -> sqlite3.Connection:
        """Long-lived connection for Celery writes.
        Single writer pattern - only one active writer.
        Caller is responsible for closing."""
```

**WAL Mode Configuration:**
```python
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA busy_timeout=5000")
```

### Core Methods

```python
class SQLiteManager:
    def initialize_db(self) -> None:
        """Create metadata table, enable WAL mode, create indexes.
        
        Also cleans up any orphaned temp tables from previous crashes."""
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
        
        # Enable WAL mode
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        
        # Clean up orphaned temp tables from previous crashes
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE '%_new' OR name LIKE '%_old')"
        ).fetchall()
        for (table,) in tables:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            logger.warning(f"Cleaned up orphaned temp table: {table}")
        
    def refresh_mv(self, view_name: str, domain: str, conn: sqlite3.Connection, 
                   date_range: tuple[str, str] | None = None) -> RefreshResult:
        """Refresh MV based on domain-specific strategy.
        Internally decides full vs incremental.
        
        Args:
            view_name: Name of the materialized view
            domain: 'sales', 'profit', or 'inventory'
            conn: Writer connection (from get_writer_conn)
            date_range: Optional (start_date, end_date) for backfill scenarios.
                        If provided, forces full refresh for the date range.
        """
        
    def get_metadata(self, view_name: str) -> MVMetadata | None:
        """Get refresh metadata for a view."""
        
    def get_refresh_strategy(self, view_name: str) -> tuple[str, date | None]:
        """Determine refresh strategy (full/incremental) and max_date."""
```

### Metadata Table

```sql
CREATE TABLE mv_refresh_metadata (
    view_name VARCHAR PRIMARY KEY,
    last_refresh_date TIMESTAMP,
    max_data_date DATE,
    row_count BIGINT,
    refresh_type VARCHAR  -- 'full' or 'incremental'
);
```

### Refresh Result

```python
@dataclass
class RefreshResult:
    view_name: str
    strategy: str  # 'full' or 'incremental'
    rows_affected: int
    duration_seconds: float
    success: bool
    error_message: str | None = None
```

## Refresh Implementation

### Incremental Refresh Pattern

```python
def _incremental_refresh(conn, view_name: str, max_date: date) -> RefreshResult:
    """Incremental refresh with transaction protection."""
    # Read new data from parquet
    new_data = _read_parquet_incremental(view_name, max_date)
    
    with conn:  # BEGIN/COMMIT/ROLLBACK
        # Insert new rows using executemany (truly atomic with metadata update)
        rows = new_data.to_pandas().itertuples(index=False, name=None)
        placeholders = ','.join('?' * len(new_data.columns))
        conn.executemany(
            f"INSERT INTO {view_name} VALUES ({placeholders})",
            rows
        )
        
        # Update metadata atomically
        new_max_date = _get_new_max_date(view_name)
        new_row_count = _get_row_count(view_name, conn)
        conn.execute(
            "UPDATE mv_refresh_metadata SET max_data_date=?, row_count=?, last_refresh_date=?, refresh_type=? WHERE view_name=?",
            (new_max_date, new_row_count, datetime.now(), 'incremental', view_name)
        )
    # Both succeed or both roll back
```

### Full Refresh Pattern (Atomic Swap)

```python
def _full_refresh_atomic_swap(conn, view_name: str, df: pl.DataFrame) -> RefreshResult:
    """Full refresh with atomic swap - no downtime."""
    temp_name = f"{view_name}_new"
    old_name = f"{view_name}_old"
    
    # Step 1: Create temp table OUTSIDE the swap transaction
    # (pandas manages its own commit here — that's fine at this stage)
    df.to_pandas().to_sql(temp_name, conn, if_exists="replace", index=False)
    
    # Step 2: Atomic swap INSIDE transaction — only renames and metadata, no pandas
    with conn:
        conn.execute(f"ALTER TABLE {view_name} RENAME TO {old_name}")
        conn.execute(f"ALTER TABLE {temp_name} RENAME TO {view_name}")
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
```

### First-Run Bootstrap

```python
def get_refresh_strategy(conn, view_name: str) -> tuple[str, date | None]:
    """Determine refresh strategy with first-run handling."""
    row = conn.execute(
        "SELECT max_data_date FROM mv_refresh_metadata WHERE view_name=?",
        (view_name,)
    ).fetchone()
    if row is None:
        return "full", None  # First run or recovery
    return "incremental", row[0]
```

## Domain-Specific Refresh Logic

### Sales Domain

**Views:** mv_sales_daily, mv_sales_by_product, mv_sales_by_principal

**Strategy:** Incremental by date

**Polars Query:**
```python
def _refresh_sales_daily(conn, max_date: date | None):
    if max_date is None:
        # First run or full refresh - use atomic swap
        df = pl.scan_parquet("/data-lake/star-schema/agg_sales_daily/**/*.parquet", 
                           hive_partitioning=True).collect()
        _full_refresh_atomic_swap(conn, "mv_sales_daily", df)
    else:
        # Incremental load
        df = pl.scan_parquet("/data-lake/star-schema/agg_sales_daily/**/*.parquet", 
                           hive_partitioning=True).filter(
            pl.col("date") > max_date
        ).collect()
        
        # Write to SQLite using executemany (truly atomic with metadata update)
        with conn:
            rows = df.to_pandas().itertuples(index=False, name=None)
            placeholders = ','.join('?' * len(df.columns))
            conn.executemany(
                f"INSERT INTO mv_sales_daily VALUES ({placeholders})",
                rows
            )
            
            # Update metadata
            new_max_date = df["date"].max()
            new_row_count = len(df) + _get_row_count("mv_sales_daily", conn)
            conn.execute(
                "UPDATE mv_refresh_metadata SET max_data_date=?, row_count=?, last_refresh_date=?, refresh_type=? WHERE view_name=?",
                (new_max_date, new_row_count, datetime.now(), 'incremental', 'mv_sales_daily')
            )
```

### Profit Domain

**Views:** mv_profit_daily

**Strategy:** Incremental by date

**Polars Query:**
```python
def _refresh_profit_daily(conn, max_date: date | None):
    if max_date is None:
        # First run or full refresh - use atomic swap
        df = pl.scan_parquet("/data-lake/star-schema/agg_profit_daily/**/*.parquet", 
                           hive_partitioning=True).collect()
        _full_refresh_atomic_swap(conn, "mv_profit_daily", df)
    else:
        # Incremental load
        df = pl.scan_parquet("/data-lake/star-schema/agg_profit_daily/**/*.parquet", 
                           hive_partitioning=True).filter(
            pl.col("date") > max_date
        ).collect()
        
        # Write to SQLite using executemany (truly atomic with metadata update)
        with conn:
            rows = df.to_pandas().itertuples(index=False, name=None)
            placeholders = ','.join('?' * len(df.columns))
            conn.executemany(
                f"INSERT INTO mv_profit_daily VALUES ({placeholders})",
                rows
            )
            
            # Update metadata
            new_max_date = df["date"].max()
            new_row_count = len(df) + _get_row_count("mv_profit_daily", conn)
            conn.execute(
                "UPDATE mv_refresh_metadata SET max_data_date=?, row_count=?, last_refresh_date=?, refresh_type=? WHERE view_name=?",
                (new_max_date, new_row_count, datetime.now(), 'incremental', 'mv_profit_daily')
            )
```

### Inventory Domain

**Views:** mv_inventory_daily, mv_product_velocity, mv_inventory_status

**Strategy:** Full refresh (snapshot data)

**Polars Query:**
```python
def _refresh_inventory_daily(conn):
    # Full load - inventory is a snapshot, not append-only
    df = pl.scan_parquet("/data-lake/star-schema/fact_stock_on_hand_snapshot/**/*.parquet",
                       hive_partitioning=True).collect()
    
    # Use atomic swap
    _full_refresh_atomic_swap(conn, "mv_inventory_daily", df)
```

## Celery Task Design

### Task Boundaries

**Celery Worker:**
- Owns all writes to SQLite
- Never reads from SQLite
- Uses single writer connection

**Dash App:**
- Reads only from SQLite
- Never writes to SQLite
- Uses short-lived reader connections

**Redis:**
- Only used for Celery task state
- Not used for MV coordination

### Celery Task Interface

```python
DOMAIN_VIEWS = {
    "sales": ["mv_sales_daily", "mv_sales_by_product", "mv_sales_by_principal"],
    "profit": ["mv_profit_daily"],
    "inventory": ["mv_inventory_daily", "mv_product_velocity", "mv_inventory_status"],
}

@app.task(bind=True)
def refresh_sqlite_mvs(self, domain: str, date_range: tuple[str, str] | None = None):
    """Refresh SQLite MVs for a domain.
    
    Args:
        domain: 'sales', 'profit', or 'inventory'
        date_range: Optional (start_date, end_date) for backfill scenarios.
                    If provided, forces full refresh for the date range.
    """
    manager = SQLiteManager()
    conn = manager.get_writer_conn()
    
    try:
        views = DOMAIN_VIEWS[domain]
        results = []
        
        for view_name in views:
            result = manager.refresh_mv(view_name, domain, conn, date_range=date_range)
            results.append(result)
        
        return {"success": True, "results": results}
    finally:
        conn.close()
```

### On-Demand Refresh Pattern

```python
@app.task(bind=True)
def refresh_inventory_status(self):
    """On-demand refresh of mv_inventory_status.
    Triggered from UI, executed by Celery."""
    manager = SQLiteManager()
    conn = manager.get_writer_conn()
    
    try:
        result = manager.refresh_mv("mv_inventory_status", "inventory", conn)
        return result
    finally:
        conn.close()
```

## Phase Implementation Plan

### Phase 1: Foundation

**Goal:** Build SQLiteManager infrastructure

**Tasks:**
1. Create services/sqlite_manager.py
2. Implement SQLiteManager class with:
   - initialize_db() method
   - get_reader_conn() method
   - get_writer_conn() method
   - get_metadata() method
   - get_refresh_strategy() method
3. Add WAL mode configuration
4. Create metadata table schema
5. Add unit tests for connection management

**Files:**
- services/sqlite_manager.py (new)
- tests/test_sqlite_manager.py (new)

### Phase 2: Sales Domain

**Goal:** Migrate sales MVs with full cutover

**Tasks:**
1. Implement refresh_mv() for sales domain
2. Add incremental refresh logic for mv_sales_daily
3. Add incremental refresh logic for mv_sales_by_product
4. Add incremental refresh logic for mv_sales_by_principal
5. Update services/sales_metrics.py to use SQLite queries
6. Update etl_tasks.py to use SQLite MV refresh
7. Remove DuckDB sales MV code
8. Validate query parity

**Files:**
- services/sqlite_manager.py (update)
- services/sales_metrics.py (update)
- etl_tasks.py (update)
- services/duckdb_connector.py (remove sales MV code)

### Phase 3: Profit Domain

**Goal:** Migrate profit MVs with full cutover

**Tasks:**
1. Implement refresh_mv() for profit domain
2. Add incremental refresh logic for mv_profit_daily
3. Update services/profit_metrics.py to use SQLite queries
4. Update etl_tasks.py to use SQLite MV refresh
5. Remove DuckDB profit MV code
6. Validate query parity

**Files:**
- services/sqlite_manager.py (update)
- services/profit_metrics.py (update)
- etl_tasks.py (update)
- services/duckdb_connector.py (remove profit MV code)

### Phase 4: Inventory Domain

**Goal:** Migrate inventory MVs with full refresh logic

**Tasks:**
1. Implement refresh_mv() for inventory domain
2. Add full refresh logic for mv_inventory_daily
3. Add full refresh logic for mv_product_velocity
4. Add on-demand refresh for mv_inventory_status
5. Update services/inventory_metrics.py to use SQLite queries
6. Update pages/operational.py to use Celery task for on-demand refresh
7. Remove DuckDB inventory MV code
8. Validate query parity

**Files:**
- services/sqlite_manager.py (update)
- services/inventory_metrics.py (update)
- pages/operational.py (update)
- etl_tasks.py (update)
- services/duckdb_connector.py (remove inventory MV code)

### Phase 5: Remove DuckDB

**Goal:** Complete DuckDB removal

**Tasks:**
1. Remove services/duckdb_connector.py
2. Update app.py to remove DuckDB initialization
3. Remove DuckDB from requirements.txt
4. Update documentation
5. Clean up any remaining DuckDB references

**Files:**
- services/duckdb_connector.py (delete)
- app.py (update)
- requirements.txt (update)
- DOCUMENTATION.md (update)
- ARCHITECTURE.md (update)

## Error Handling

### Transaction Rollback

All incremental refresh operations are wrapped in transactions:

```python
with conn:
    # Multiple operations
    # All succeed or all roll back
```

### Connection Error Handling

```python
def get_reader_conn(self) -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Failed to create reader connection: {e}")
        raise
```

### Refresh Failure Handling

```python
def refresh_mv(self, view_name: str, domain: str) -> RefreshResult:
    try:
        # Refresh logic
        return RefreshResult(success=True, ...)
    except Exception as e:
        logger.error(f"Failed to refresh {view_name}: {e}")
        return RefreshResult(success=False, error_message=str(e))
```

## Performance Considerations

### Polars Lazy Evaluation

Polars lazy scanning with date partitioning ensures efficient reads:

```python
df = pl.scan_parquet("/data-lake/star-schema/agg_sales_daily/**/*.parquet",
                     hive_partitioning=True).filter(
    pl.col("date") >= start_date
).collect()
```

### SQLite Indexes

Add indexes for common query patterns:

```sql
CREATE INDEX idx_mv_sales_daily_date ON mv_sales_daily(date);
CREATE INDEX idx_mv_sales_by_product_date ON mv_sales_by_product(date, product_id);
CREATE INDEX idx_mv_profit_daily_date ON mv_profit_daily(date);
```

### WAL Checkpointing

Prevent unbounded WAL file growth:

```python
conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
```

## Testing Strategy

### Unit Tests

- Test SQLiteManager connection management
- Test metadata table operations
- Test refresh strategy detection
- Test transaction rollback on failure

### Integration Tests

- Test end-to-end MV refresh for each domain
- Test query parity between DuckDB and SQLite
- Test concurrent read/write scenarios
- Test on-demand refresh via Celery

### Performance Tests

- Benchmark incremental refresh performance
- Benchmark full refresh performance
- Benchmark query performance vs DuckDB
- Monitor WAL file growth

## Monitoring

### Metrics to Track

- MV refresh duration per view
- MV refresh success/failure rates
- SQLite WAL file size
- Query performance vs DuckDB baseline
- Row counts per MV

### Logging

```python
logger.info(f"Refreshed {view_name} ({strategy}): {rows_affected} rows in {duration:.2f}s")
logger.error(f"Failed to refresh {view_name}: {error_message}")
```

## Rollback Plan

If issues occur during migration:

1. **Phase rollback**: Revert individual phases by restoring DuckDB code
2. **Data rollback**: SQLite database can be deleted and rebuilt
3. **Feature flag**: Add environment variable to switch between DuckDB and SQLite
4. **Parallel run**: Query both DuckDB and SQLite, compare results during validation

## Success Criteria

1. **No lock conflicts**: SQLite single-writer pattern eliminates conflicts
2. **No crashes**: SQLite is stable and battle-tested
3. **Query parity**: SQLite query results match DuckDB results
4. **Performance**: Query performance meets or exceeds DuckDB baseline
5. **Reliability**: MV refresh success rate > 99%

## Next Steps

1. Review and approve this design document
2. Create implementation plan using writing-plans skill
3. Begin Phase 1 implementation
4. Validate each phase before proceeding to next
5. Complete Phase 5 and remove DuckDB entirely
