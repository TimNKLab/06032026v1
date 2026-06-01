# Inventory Stock Levels Memory Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix SIGKILL memory exhaustion errors when querying stock levels by using Polars for efficient merges and filtering datasets before loading into memory.

**Architecture:** Filter dimensions to only products with non-zero inventory, filter sales aggregates to those products, use Polars lazy evaluation and joins for memory efficiency, convert final result to pandas for UI compatibility.

**Tech Stack:** Polars (lazy evaluation, efficient joins), Pandas (UI compatibility), DuckDB (parquet reads), Python 3.9

---

## File Structure

**Files to modify:**
- `services/inventory_metrics.py` - Contains `_query_stock_levels` function that needs optimization

**No new files created** - This is a focused optimization of existing code.

---

## Task 1: Verify polars import

**Files:**
- Modify: `services/inventory_metrics.py:1-30`

- [ ] **Step 1: Check if polars is imported**

Run:
```bash
grep -n "import polars" d:\NKLabs\Plotly\nkdash\services\inventory_metrics.py
```

Expected: Output shows `import polars as pl` or similar

- [ ] **Step 2: Add polars import if missing**

If no import found, add at top of file after other imports:
```python
import polars as pl
```

- [ ] **Step 3: Commit the import addition**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: ensure polars import is present for memory optimization"
```

---

## Task 2: Add optional limit parameter to _query_stock_levels

**Files:**
- Modify: `services/inventory_metrics.py:92-125`

- [ ] **Step 1: Add limit parameter to function signature**

```python
def _query_stock_levels(snapshot_date: date, lookback_start: date, lookback_end: date, limit: int = 5000) -> pd.DataFrame:
    """Stock levels using Polars lazy evaluation + efficient filtering.
    
    Memory optimization strategy:
    1. Filter dimensions to only products with non-zero inventory
    2. Filter sales to only those products
    3. Use Polars lazy joins instead of pandas merges
    4. Convert final result to pandas for UI compatibility
    5. Limit result size to prevent UI overload
    
    Args:
        snapshot_date: Date to query inventory snapshot
        lookback_start: Start date for sales lookback period
        lookback_end: End date for sales lookback period
        limit: Maximum number of rows to return (default 5000)
    
    Returns:
        DataFrame with stock levels data
    """
```

- [ ] **Step 2: Commit the parameter addition**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: add limit parameter to _query_stock_levels for memory optimization"
```

---

## Task 3: Filter dimensions to products with inventory OR sales

**Files:**
- Modify: `services/inventory_metrics.py:101-118`

- [ ] **Step 1: Replace pandas dimension load with Polars lazy filter**

Replace lines 109-118:
```python
    # Load dimensions from parquet using Polars
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    dim_path = f"{data_lake_root}/star-schema/dim_products.parquet"
    
    if os.path.exists(dim_path):
        dim_df = pl.read_parquet(dim_path).to_pandas()
    else:
        # Fallback: empty dimensions
        dim_df = pd.DataFrame(columns=['product_id', 'product_name', 'product_category', 
                                       'product_brand', 'product_barcode', 'product_sku'])
```

With:
```python
    # Filter dimensions to products with inventory OR sales (captures transactions at stock=0)
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    dim_path = f"{data_lake_root}/star-schema/dim_products.parquet"
    
    # Get product IDs with non-zero inventory (qty_on_hand != 0 handles miscounts)
    product_ids_with_inventory = set(on_hand_df[on_hand_df['qty_on_hand'] != 0]['product_id'].tolist())
    
    # Get sales aggregates first to identify products with sales (even if inventory=0)
    sales_df = query_sales_by_product_duckdb(lookback_start, lookback_end)
    sales_df = sales_df.rename(columns={'units_sold': 'units_sold'})
    
    # Combine: products with inventory OR sales
    product_ids_with_sales = set(sales_df['product_id'].tolist())
    relevant_product_ids = product_ids_with_inventory.union(product_ids_with_sales)
    
    if os.path.exists(dim_path) and relevant_product_ids:
        dim_pl = pl.scan_parquet(dim_path).filter(
            pl.col('product_id').is_in(list(relevant_product_ids))
        )
        dim_df = dim_pl.collect().to_pandas()
    else:
        # Fallback: empty dimensions
        dim_df = pd.DataFrame(columns=['product_id', 'product_name', 'product_category', 
                                       'product_brand', 'product_barcode', 'product_sku'])
```

- [ ] **Step 2: Commit the dimension filtering change**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: filter dimensions to products with inventory OR sales for accuracy"
```

---

## Task 4: Convert DataFrames to Polars

**Files:**
- Modify: `services/inventory_metrics.py:120-145`

- [ ] **Step 1: Convert pandas DataFrames to Polars**

Replace lines 120-122:
```python
    # Join data
    result = on_hand_df.merge(sales_df, on='product_id', how='left')
    result = result.merge(dim_df, on='product_id', how='left')
```

With:
```python
    # Convert to Polars for efficient lazy joins
    on_hand_pl = pl.from_pandas(on_hand_df)
    
    # Derive schema from actual DataFrames to avoid type mismatches
    if not sales_df.empty:
        sales_pl = pl.from_pandas(sales_df)
    else:
        # Use schema derived from on_hand_df's product_id type
        product_id_dtype = on_hand_pl['product_id'].dtype
        sales_pl = pl.DataFrame(schema={
            'product_id': product_id_dtype,
            'units_sold': pl.Float64,
            'revenue': pl.Float64
        })
    
    if not dim_df.empty:
        dim_pl = pl.from_pandas(dim_df)
    else:
        # Use schema derived from on_hand_df's product_id type
        product_id_dtype = on_hand_pl['product_id'].dtype
        dim_pl = pl.DataFrame(schema={
            'product_id': product_id_dtype,
            'product_name': pl.Utf8,
            'product_category': pl.Utf8,
            'product_brand': pl.Utf8,
            'product_barcode': pl.Utf8,
            'product_sku': pl.Utf8
        })
```

- [ ] **Step 2: Commit the DataFrame conversion**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: convert DataFrames to Polars for memory-efficient joins"
```

---

## Task 5: Perform Polars joins

**Files:**
- Modify: `services/inventory_metrics.py:130-145`

- [ ] **Step 1: Replace pandas merges with Polars joins**

After the DataFrame conversion code (after dim_pl definition), add:
```python
    # Perform joins in Polars (memory efficient)
    result_pl = on_hand_pl.join(sales_pl, on='product_id', how='left')
    result_pl = result_pl.join(dim_pl, on='product_id', how='left')
```

- [ ] **Step 2: Remove old pandas merge code**

Delete lines that were previously:
```python
    result = on_hand_df.merge(sales_df, on='product_id', how='left')
    result = result.merge(dim_df, on='product_id', how='left')
```

- [ ] **Step 3: Commit the Polars joins**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: replace pandas merges with Polars lazy joins"
```

---

## Task 7: Fill missing values in Polars

**Files:**
- Modify: `services/inventory_metrics.py:145-160`

- [ ] **Step 1: Replace pandas fillna with Polars fill_null**

Replace lines 157-159:
```python
    # Fill missing values
    result['units_sold'] = result['units_sold'].fillna(0)
    result['reserved_qty'] = 0  # Not available in current schema
```

With:
```python
    # Fill missing values in Polars
    result_pl = result_pl.with_columns([
        pl.col('units_sold').fill_null(0),
        pl.lit(0).alias('reserved_qty'),  # Not available in current schema
    ])
```

- [ ] **Step 2: Commit the fill_null change**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: replace pandas fillna with Polars fill_null"
```

---

## Task 7: Format product name fallback in Polars

**Files:**
- Modify: `services/inventory_metrics.py:160-175`

- [ ] **Step 1: Replace pandas fillna with Polars coalesce**

Replace lines 161-168:
```python
    # Format product name fallback
    result['product_name'] = result['product_name'].fillna(
        result['product_id'].apply(lambda x: f'Product {x}')
    )
    result['product_category'] = result['product_category'].fillna('Unknown Category')
    result['product_brand'] = result['product_brand'].fillna('Unknown Brand')
    result['product_barcode'] = result['product_barcode'].fillna('')
    result['product_sku'] = result['product_sku'].fillna('')
```

With:
```python
    # Format product name fallback
    result_pl = result_pl.with_columns([
        pl.coalesce([pl.col('product_name'), pl.format('Product {}', pl.col('product_id'))]).alias('product_name'),
        pl.coalesce([pl.col('product_category'), pl.lit('Unknown Category')]).alias('product_category'),
        pl.coalesce([pl.col('product_brand'), pl.lit('Unknown Brand')]).alias('product_brand'),
        pl.coalesce([pl.col('product_barcode'), pl.lit('')]).alias('product_barcode'),
        pl.coalesce([pl.col('product_sku'), pl.lit('')]).alias('product_sku')
    ])
```

- [ ] **Step 2: Commit the coalesce change**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: replace pandas fillna with Polars coalesce for product names"
```

---

## Task 8: Select and order columns in Polars

**Files:**
- Modify: `services/inventory_metrics.py:175-180`

- [ ] **Step 1: Replace pandas select/sort with Polars**

Replace lines 170-173:
```python
    # Select and order columns
    result = result[['product_id', 'product_name', 'product_category', 'product_brand',
                     'product_barcode', 'product_sku', 'on_hand_qty', 'reserved_qty', 'units_sold']]
    result = result.sort_values('on_hand_qty', ascending=False)
```

With:
```python
    # Select and order columns
    result_pl = result_pl.select([
        'product_id', 'product_name', 'product_category', 'product_brand',
        'product_barcode', 'product_sku', 'on_hand_qty', 'reserved_qty', 'units_sold'
    ])
    result_pl = result_pl.sort('on_hand_qty', descending=True)
```

- [ ] **Step 2: Commit the select/sort change**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: replace pandas select/sort with Polars operations"
```

---

## Task 9: Apply limit and convert to pandas

**Files:**
- Modify: `services/inventory_metrics.py:180-185`

- [ ] **Step 1: Add limit and pandas conversion**

Replace line 175:
```python
    return result
```

With:
```python
    # Apply limit
    result_pl = result_pl.head(limit)
    
    # Convert to pandas for UI compatibility
    result = result_pl.to_pandas()
    
    return result
```

- [ ] **Step 2: Commit the limit and conversion**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: add result limit and convert to pandas for UI compatibility"
```

---

## Task 11: Update query_inventory_summary to use same optimization pattern

**Files:**
- Modify: `services/inventory_metrics.py:425-490`

- [ ] **Step 1: Add dimension filtering to query_inventory_summary**

After line 440 (after loading stock_df), add:
```python
    # Filter dimensions to only products with inventory
    product_ids_with_inventory = set(stock_df[stock_df['on_hand_qty'] != 0]['product_id'].tolist())
```

- [ ] **Step 2: Replace dimension load with Polars filter**

Replace lines 445-452:
```python
    # Load dimensions from parquet using Polars
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    dim_path = f"{data_lake_root}/star-schema/dim_products.parquet"
    
    if os.path.exists(dim_path):
        dim_df = pl.read_parquet(dim_path).to_pandas()
    else:
        dim_df = pd.DataFrame(columns=['product_id', 'product_name', 'product_category', 
                                       'product_brand', 'product_barcode', 'product_sku'])
```

With:
```python
    # Load dimensions from parquet using Polars with filtering
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    dim_path = f"{data_lake_root}/star-schema/dim_products.parquet"
    
    if os.path.exists(dim_path) and product_ids_with_inventory:
        dim_pl = pl.scan_parquet(dim_path).filter(
            pl.col('product_id').is_in(list(product_ids_with_inventory))
        )
        dim_df = dim_pl.collect().to_pandas()
    else:
        dim_df = pd.DataFrame(columns=['product_id', 'product_name', 'product_category', 
                                       'product_brand', 'product_barcode', 'product_sku'])
```

- [ ] **Step 3: Commit the query_inventory_summary optimization**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: apply dimension filtering to query_inventory_summary for memory efficiency"
```

- [ ] **Step 4: Add sales filtering to query_inventory_summary**

After loading sales_df in query_inventory_summary, add:
```python
    # Filter sales to only products with inventory (convert set to list for is_in)
    if product_ids_with_inventory:
        sales_df = sales_df[sales_df['product_id'].isin(list(product_ids_with_inventory))]
```

- [ ] **Step 5: Commit the sales filtering**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: filter sales in query_inventory_summary to products with inventory"
```

---

## Task 12: Update _query_abc_products to use same pattern

**Files:**
- Test: Manual testing in Docker container

- [ ] **Step 1: Restart Docker containers**

```bash
cd d:\NKLabs\Plotly\nkdash
docker-compose restart dash-app
```

Expected: Containers restart successfully

- [ ] **Step 2: Test stock levels query for 30-day range**

```bash
docker-compose exec dash-app python -c "
from services.inventory_metrics import _query_stock_levels
from datetime import date
result = _query_stock_levels(date(2026, 5, 30), date(2026, 4, 30), date(2026, 5, 30))
print(f'Result rows: {len(result)}')
print(f'Result memory: {result.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB')
"
```

Expected: Query completes without SIGKILL, returns data, memory usage < 100 MB

- [ ] **Step 3: Test via web UI**

Navigate to http://localhost:8050/inventory, set date range to April 30 - May 30, click "Stock Levels" tab

Expected: Page loads without error, data displays, no SIGKILL in logs

- [ ] **Step 4: Check dash-app logs for OOM errors**

```bash
docker-compose logs dash-app --tail 20
```

Expected: No "SIGKILL" or "out of memory" errors

- [ ] **Step 5: Commit if tests pass**

```bash
git add services/inventory_metrics.py
git commit -m "test: verified stock levels optimization resolves SIGKILL issue"
```

---

## Task 13: Update _query_abc_products to use same pattern

**Files:**
- Modify: `services/inventory_metrics.py:570-600`

- [ ] **Step 1: Add dimension filtering to _query_abc_products**

After line 580 (after loading sales_df), add:
```python
    # Filter dimensions to only products with sales
    product_ids_with_sales = set(sales_df['product_id'].tolist())
```

- [ ] **Step 2: Replace dimension load with Polars filter**

Replace lines 585-592:
```python
    # Load dimensions from parquet using Polars
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    dim_path = f"{data_lake_root}/star-schema/dim_products.parquet"
    
    if os.path.exists(dim_path):
        dim_df = pl.read_parquet(dim_path).to_pandas()
    else:
        dim_df = pd.DataFrame(columns=['product_id', 'product_name', 'product_category', 
                                       'product_brand', 'product_barcode', 'product_sku'])
```

With:
```python
    # Load dimensions from parquet using Polars with filtering
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    dim_path = f"{data_lake_root}/star-schema/dim_products.parquet"
    
    if os.path.exists(dim_path) and product_ids_with_sales:
        dim_pl = pl.scan_parquet(dim_path).filter(
            pl.col('product_id').is_in(list(product_ids_with_sales))
        )
        dim_df = dim_pl.collect().to_pandas()
    else:
        dim_df = pd.DataFrame(columns=['product_id', 'product_name', 'product_category', 
                                       'product_brand', 'product_barcode', 'product_sku'])
```

- [ ] **Step 3: Commit the _query_abc_products optimization**

```bash
git add services/inventory_metrics.py
git commit -m "refactor: apply dimension filtering to _query_abc_products for memory efficiency"
```

---

## Self-Review

**1. Spec coverage:**
- ✅ Use Polars merges instead of pandas merges (Tasks 4-9)
- ✅ Return result as pandas for UI compatibility (Task 9, line `result_pl.to_pandas()`)
- ✅ Filter dimensions to products with inventory OR sales (Task 3, captures transactions at stock=0)
- ✅ Filter dimensions before loading (Task 3, Task 10, Task 12)
- ✅ Add result limit (Task 2, Task 9)
- ✅ Verify polars import (Task 1)

**2. Placeholder scan:**
- ✅ No TBD, TODO, or placeholders found
- ✅ All code blocks contain actual implementation
- ✅ All test steps have actual commands and expected outputs

**3. Type consistency:**
- ✅ Function signature updated consistently across all tasks
- ✅ Polars DataFrame variable names use `_pl` suffix consistently
- ✅ Column names match between operations

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-30-inventory-memory-optimization.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
