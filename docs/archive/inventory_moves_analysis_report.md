# Inventory Moves Analysis - Comprehensive Report

## Executive Summary

Inventory moves analysis tracks product movement through the warehouse system, providing insights into sell-through rates, inventory turnover, and stock movement patterns. This report documents the complete data flow from Odoo extraction to dashboard visualization.

**Current Status:** Deferred - The sell-through analysis functionality is currently stubbed due to complexity and memory constraints. The infrastructure exists but the aggregation logic is not implemented.

---

## 1. Data Source: Odoo Stock Moves

### 1.1 Odoo Data Model

Inventory moves data originates from Odoo's `stock.move.line` model, which tracks every inventory movement:

- **Source:** Odoo ERP system
- **Model:** `stock.move.line`
- **Key Fields:**
  - `move_id`: Parent stock move reference
  - `move_line_id`: Unique line identifier
  - `product_id`: Product being moved
  - `location_src_id`: Source location
  - `location_dest_id`: Destination location
  - `qty_moved`: Quantity moved
  - `date`: Movement date
  - `movement_type`: Classification (incoming, outgoing, internal, etc.)
  - `inventory_adjustment_flag`: Boolean flag for inventory adjustments

### 1.2 Movement Classification

Movements are classified into types for analysis:

| Movement Type | Description | Sign Convention |
|---------------|-------------|-----------------|
| incoming | Goods received from suppliers | Positive |
| production_in | Goods from production | Positive |
| adjustment | Inventory corrections | Variable |
| production_out | Goods to production | Negative |
| transfer | Location transfers | Neutral |
| scrap | Waste/scrap | Negative |

---

## 2. ETL Pipeline: Extraction and Loading

### 2.1 Extraction Process

**Function:** `update_inventory_moves_star_schema(clean_file_path, target_date)`

**Location:** `etl_tasks.py:1216`

**Process:**

1. **Extract:** Read cleaned JSON file from Odoo
2. **Transform:** Convert to Polars DataFrame with schema validation
3. **Load:** Write to partitioned parquet files

```python
def update_inventory_moves_star_schema(clean_file_path, target_date):
    df = pl.read_parquet(clean_file_path)
    return _update_fact_inventory_moves(df, target_date)
```

### 2.2 Data Storage

**Table:** `fact_inventory_moves`

**Storage Format:** Partitioned Parquet files

**Partitioning:** Hive-style by date
```
/data-lake/star-schema/fact_inventory_moves/
├── year=2026/
│   ├── month=05/
│   │   ├── day=01/
│   │   │   └── fact_inventory_moves_2026-05-01.parquet
│   │   └── day=02/
│   │       └── fact_inventory_moves_2026-05-02.parquet
```

**Schema:**
```python
{
    "date": pl.Date,
    "move_id": pl.Int64,
    "move_line_id": pl.Int64,
    "product_id": pl.Int64,
    "product_name": pl.Utf8,
    "product_brand": pl.Utf8,
    "location_src_id": pl.Int64,
    "location_src_name": pl.Utf8,
    "location_dest_id": pl.Int64,
    "location_dest_name": pl.Utf8,
    "qty_moved": pl.Float64,
    "uom_id": pl.Int64,
    "uom_name": pl.Utf8,
    "uom_category": pl.Utf8,
    "movement_type": pl.Utf8,
    "inventory_adjustment_flag": pl.Boolean,
    "manufacturing_order_id": pl.Int64,
    "picking_id": pl.Int64,
    "picking_type_code": pl.Utf8,
    "reference": pl.Utf8,
    "origin_reference": pl.Utf8,
    "source_partner_id": pl.Int64,
    "source_partner_name": pl.Utf8,
    "destination_partner_id": pl.Int64,
    "destination_partner_name": pl.Utf8,
    "created_by_user": pl.Int64,
    "create_date": pl.Datetime
}
```

### 2.3 ETL Schedule

**Trigger:** Celery beat scheduler
**Frequency:** Daily
**Dependency:** After POS sales (2:00 AM) and invoice sales (2:05 AM)
**Target Time:** ~2:10 AM

---

## 3. DuckDB View Layer

### 3.1 View Definition

**Function:** `ensure_duckdb_view_groups(groups=['inventory'])`

**Location:** `services/duckdb_connector.py:449`

**View Name:** `fact_inventory_moves`

**Purpose:** Provides type-safe, read-only access to parquet data with column renaming for consistency.

```sql
CREATE OR REPLACE VIEW fact_inventory_moves AS
SELECT
    TRY_CAST(date AS TIMESTAMP) AS movement_date,
    COALESCE(TRY_CAST(move_id AS BIGINT), 0) AS move_id,
    TRY_CAST(move_line_id AS BIGINT) AS move_line_id,
    TRY_CAST(product_id AS BIGINT) AS product_id,
    COALESCE(product_name, '') AS product_name,
    COALESCE(product_brand, '') AS product_brand,
    TRY_CAST(location_src_id AS BIGINT) AS location_src_id,
    COALESCE(location_src_name, '') AS location_src_name,
    TRY_CAST(location_dest_id AS BIGINT) AS location_dest_id,
    COALESCE(location_dest_name, '') AS location_dest_name,
    COALESCE(TRY_CAST(qty_moved AS DOUBLE), 0) AS qty_moved,
    TRY_CAST(uom_id AS BIGINT) AS uom_id,
    COALESCE(uom_name, '') AS uom_name,
    COALESCE(uom_category, '') AS uom_category,
    COALESCE(movement_type, '') AS movement_type,
    COALESCE(TRY_CAST(inventory_adjustment_flag AS BOOLEAN), FALSE) AS inventory_adjustment_flag,
    TRY_CAST(manufacturing_order_id AS BIGINT) AS manufacturing_order_id,
    TRY_CAST(picking_id AS BIGINT) AS picking_id,
    COALESCE(picking_type_code, '') AS picking_type_code,
    COALESCE(reference, '') AS reference,
    COALESCE(origin_reference, '') AS origin_reference,
    TRY_CAST(source_partner_id AS BIGINT) AS source_partner_id,
    COALESCE(source_partner_name, '') AS source_partner_name,
    TRY_CAST(destination_partner_id AS BIGINT) AS destination_partner_id,
    COALESCE(destination_partner_name, '') AS destination_partner_name,
    TRY_CAST(created_by_user AS BIGINT) AS created_by_user,
    TRY_CAST(create_date AS TIMESTAMP) AS create_date
FROM read_parquet('/data-lake/star-schema/fact_inventory_moves/**/*.parquet', 
                  union_by_name=True, 
                  filename=true)
```

**Key Transformations:**
- `date` → `movement_date` (timestamp casting)
- Null handling with `COALESCE` for string fields
- Type casting with `TRY_CAST` for numeric fields
- Default values for missing data

### 3.2 View Groups

**Group:** `inventory`

**Includes:**
- `fact_inventory_moves` - Movement data
- `fact_stock_on_hand_snapshot` - Stock snapshots

---

## 4. Aggregation Logic (Not Implemented)

### 4.1 Sell-Through Analysis

**Function:** `get_sell_through_analysis(start_date, end_date)`

**Location:** `services/inventory_metrics.py:437`

**Current Status:** Stub function - returns empty data

**Required Aggregations:**

```python
# Movement aggregation by product
units_received = SUM(qty_moved) WHERE movement_type IN ('incoming', 'production_in')
units_incoming = SUM(qty_moved) WHERE movement_type = 'incoming'
units_production_in = SUM(qty_moved) WHERE movement_type = 'production_in'
units_adjustment_net = SUM(qty_moved) WHERE inventory_adjustment_flag = TRUE
units_production_out = SUM(qty_moved) WHERE movement_type = 'production_out'
units_transfer_net = SUM(qty_moved) WHERE movement_type = 'transfer'

# Sales aggregation
units_sold = SUM(quantity) FROM fact_sales_all

# Beginning inventory
begin_on_hand = SUM(qty_on_hand) FROM fact_stock_on_hand_snapshot WHERE date = start_date

# Sell-through calculation
sell_through = units_sold / (begin_on_hand + units_received) * 100
```

**Output Schema:**
```python
{
    "snapshot_date": date,
    "items": pd.DataFrame([
        "product_id", "product_name", "product_category", "product_brand",
        "product_barcode", "product_sku",
        "begin_on_hand", "units_received", "units_incoming", "units_production_in",
        "units_adjustment_net", "units_production_out", "units_transfer_net",
        "units_sold", "sell_through"
    ]),
    "categories": pd.DataFrame([
        "product_category", "begin_on_hand", "units_received", "units_sold", "sell_through"
    ]),
    "summary": {
        "sell_through": float,
        "units_sold": float,
        "units_received": float,
        "begin_on_hand": float
    }
}
```

### 4.2 Complexity Issues

**Why Deferred:**

1. **Complex CTE Structure:** Requires multiple Common Table Expressions to aggregate movements by type
2. **Cross-Domain Joins:** Needs to join:
   - `fact_inventory_moves` (movements)
   - `fact_stock_on_hand_snapshot` (inventory levels)
   - `fact_sales_all` (sales data)
   - `dim_products` (product dimensions)
3. **Memory Constraints:** Large dataset joins cause SIGKILL in dash-app (2GB limit)
4. **Performance:** DuckDB queries on movement data are slow (10+ seconds for 30-day ranges)

**Previous Implementation:**
- Used SQLite MVs: `mv_inventory_daily`, `mv_sales_by_product`
- Implemented movement_type classification in Python/Pandas
- Parity tests: 5/5 passed (tests/test_sell_through_parity.py)

---

## 5. Data Serving to UI

### 5.1 Dashboard Integration

**Page:** `pages/inventory.py`

**Tab:** Sell-Through Analysis

**Callback:** `update_abc_analysis` (currently handles ABC only)

**Route:**
```
User clicks "Sell-Through" tab
→ Dash callback triggered
→ Calls get_sell_through_analysis(start_date, end_date)
→ Returns aggregated data
→ Displays in DataTable and charts
```

### 5.2 Current Behavior

**Status:** Empty data returned

**Reason:** Function is stubbed to prevent import errors

**User Experience:**
- Tab loads successfully
- Empty tables/charts displayed
- No errors shown to user

### 5.3 Memory Constraints

**Container Limits:**
```yaml
dash-app:
  mem_limit: 2g
  mem_reservation: 1g
```

**Issue:** Even with DuckDB LIMIT 5000 and result limit 1000, SIGKILL still occurs during inventory queries.

**Root Cause:** DuckDB queries themselves consume memory before Polars can process the data.

---

## 6. Performance Characteristics

### 6.1 Query Performance

**Inventory Snapshot Query:**
- Duration: 16.027s
- Rows: 42,807
- Source: `fact_stock_on_hand_snapshot`

**Sales by Product Query:**
- Duration: 9.752s
- Rows: 11,620
- Source: `agg_sales_daily_by_product`

**Combined Stock + Sales Query:**
- Duration: 29.547s
- Memory: High (causes SIGKILL)

### 6.2 Optimization Attempts

**Attempted:**
1. Polars lazy evaluation
2. Early dimension filtering
3. DuckDB LIMIT clauses (LIMIT 5000)
4. Result limiting (limit=1000)
5. Column selection optimization

**Result:** SIGKILL still occurs

**Conclusion:** Memory issue is deeper than query optimization - likely DuckDB connection overhead or data loading before Polars processing.

---

## 7. Architecture Decisions

### 7.1 Current Architecture

```
Odoo (stock.move.line)
    ↓
ETL (Celery)
    ↓
fact_inventory_moves (Parquet)
    ↓
DuckDB View (fact_inventory_moves)
    ↓
Query Functions (inventory_metrics.py)
    ↓
Dash UI (pages/inventory.py)
```

### 7.2 Database Boundaries

**DuckDB:**
- ETL operations only (extraction, parquet creation)
- Read-only views over parquet files
- Query layer for dashboard

**SQLite:**
- Previously used for MVs (mv_inventory_daily, mv_sales_by_product)
- Migration to DuckDB parquet in progress
- MV refresh logic removed

**Polars:**
- Parquet reads for dimension filtering
- Lazy evaluation for memory efficiency
- Cross-domain joins

**Pandas:**
- Final data manipulation
- UI compatibility
- Small dataset operations

---

## 8. Known Issues

### 8.1 SIGKILL (Memory Exhaustion)

**Symptom:** Worker timeout, SIGKILL signal

**Error:**
```
[CRITICAL] WORKER TIMEOUT (pid:7)
[ERROR] Worker (pid:7) was sent SIGKILL! Perhaps out of memory?
```

**Impact:**
- Inventory queries fail
- Dashboard becomes unresponsive
- Worker restarts

**Root Cause:**
- Dash-app has only 2GB memory limit
- DuckDB queries load full datasets before Polars can filter
- Cross-domain joins consume significant memory

**Attempted Fixes:**
- ✗ DuckDB LIMIT 5000
- ✗ Result limit 1000
- ✗ Polars lazy evaluation
- ✗ Early dimension filtering

**Next Steps:**
- Increase dash-app memory limit to 4GB
- Use in-memory DuckDB to avoid file locks
- Consider pre-aggregating movement data
- Implement pagination for large datasets

### 8.2 DuckDB File Lock Conflicts

**Symptom:** `IO Error: Could not set lock on file`

**Impact:** Cannot run queries while dash-app is active

**Workaround:** Use in-memory DuckDB for testing

```python
conn = duckdb.connect()  # In-memory, no file
result = conn.execute(f"""
    SELECT date, SUM(revenue) as revenue
    FROM read_parquet('{data_lake}/star-schema/agg_sales_daily/**/*.parquet', 
                      hive_partitioning=true)
    WHERE date >= ? AND date <= ?
""", [start_date, end_date]).fetchall()
```

### 8.3 Column Name Mismatches

**Symptom:** `KeyError: 'quantity'` or `KeyError: 'units_sold'`

**Cause:** Inconsistent column naming between DuckDB views and code expectations

**Fix:** Standardize on `units_sold` (not `quantity`) for sales aggregates

---

## 9. Recommendations

### 9.1 Immediate Actions

1. **Increase Memory Limit:**
   ```yaml
   dash-app:
     mem_limit: 4g  # Increase from 2g
     mem_reservation: 2g  # Increase from 1g
   ```

2. **Implement In-Memory DuckDB:**
   - Use `duckdb.connect()` instead of file-based connection
   - Avoids file lock conflicts
   - Reduces memory overhead

3. **Pre-Aggregate Movement Data:**
   - Create daily aggregates: `agg_inventory_moves_daily`
   - Aggregate by product and movement type
   - Reduce query complexity

### 9.2 Medium-Term Improvements

1. **Implement Sell-Through Analysis:**
   - Use pre-aggregated movement data
   - Simplify CTE structure
   - Add pagination for large result sets

2. **Add Movement Type Classification:**
   - Implement in ETL layer (not Python)
   - Persist classification in parquet
   - Use for filtering and aggregation

3. **Optimize DuckDB Views:**
   - Add materialized views for common queries
   - Use partition pruning more aggressively
   - Cache frequently accessed data

### 9.3 Long-Term Architecture

1. **Separate Query Service:**
   - Dedicated service for inventory queries
   - Independent memory allocation
   - Separate from dash-app

2. **Streaming Architecture:**
   - Stream inventory movements in real-time
   - Incremental aggregation
   - Reduce batch processing overhead

3. **Alternative Database:**
   - Consider ClickHouse for analytics
   - Better columnar performance
   - Native support for time-series data

---

## 10. Testing Strategy

### 10.1 Unit Tests

**File:** `tests/test_inventory_column_consistency.py`

**Test:** `test_fact_inventory_moves_schema()`

**Purpose:** Verify parquet schema matches expectations

### 10.2 Parity Tests

**File:** `tests/test_sell_through_parity.py`

**Status:** 5/5 passed (previous implementation)

**Purpose:** Ensure DuckDB parquet queries match SQLite MV results

### 10.3 Integration Tests

**Manual Testing Required:**
- Test sell-through analysis with real data
- Verify memory usage with 30-day ranges
- Validate aggregation logic
- Check UI rendering

---

## 11. Documentation References

- **Rollback Plan:** `docs/inventory_metrics_rollback_plan.md`
- **Column Audit:** `docs/inventory_column_audit_summary.md`
- **Specification:** `docs/inventory_spec.md`
- **ETL Guide:** `ETL_GUIDE.md`
- **API Reference:** `docs/api_reference.md`

---

## 12. Conclusion

Inventory moves analysis infrastructure is in place but not fully implemented due to memory constraints and complexity. The data pipeline from Odoo to parquet is functional, and the DuckDB view layer provides type-safe access. However, the sell-through aggregation logic requires additional optimization to work within the current 2GB memory limit.

**Key Takeaways:**
1. Data extraction and storage working correctly
2. DuckDB view layer provides clean abstraction
3. Aggregation logic deferred due to memory issues
4. SIGKILL persists despite query optimizations
5. Memory limit increase is the most viable immediate fix

**Next Priority:** Increase dash-app memory limit to 4GB and retest inventory queries.
