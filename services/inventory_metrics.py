import csv
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
import math
import time
from typing import Dict, Optional

import pandas as pd

# COLUMN REFERENCE AUDIT LOG
# Updated: 2026-05-30
#
# Polars Direct Parquet Access (uses raw parquet column names):
# ----------------------------------------------------------------------
# _query_location_ledger_deltas (line 170-214):
#   - Source: fact_inventory_moves/**/*.parquet
#   - Filter column: "date" ✓ (matches parquet schema)
#   - Access columns: location_dest_id, location_src_id, qty_moved ✓ (verified)
#
# get_inventory_costs (line 516-567):
#   - Source: fact_product_cost/**/*.parquet  
#   - Filter column: "date" ✓ (matches DuckDB view definition)
#   - Access columns: product_id, cost_unit_tax_in ✓ (verified)
#
# DuckDB View Access (uses view column names):
# ----------------------------------------------------------------------
# query_inventory_snapshot: Uses DuckDB view fact_stock_on_hand_snapshot
# query_sales_by_product_duckdb: Uses DuckDB view agg_sales_daily_by_product
# These return column names as defined in DuckDB views, not raw parquet

DEFAULT_ABC_THRESHOLDS = {
    "a": 0.2,
    "b": 0.5,
}

DEFAULT_STOCK_LOOKBACK_DAYS = 30
DEFAULT_LOW_STOCK_DAYS = 7

_LEDGER_BASELINE_TS = datetime(2025, 2, 10, 7, 0, 0)
_LEDGER_LOCATION_ID = 44
_LEDGER_LOCATION_POOL = {44, 8, 154, 155, 156, 53, 157, 158}
_LEDGER_BASELINE_FILENAME = 'reconcile stocks.csv'
STOCK_LEDGER_BASELINE_DATE = _LEDGER_BASELINE_TS.date()


def _to_utc_from_jakarta(dt_local: datetime) -> datetime:
    return dt_local - timedelta(hours=7)


def _normalize_snapshot_date(value: Optional[object]) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _get_snapshot_date(as_of_date: date) -> Optional[date]:
    """Get snapshot date from DuckDB in-memory connection over parquet."""
    import duckdb
    import os

    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    snapshot_path = f"{data_lake_root}/star-schema/fact_stock_on_hand_snapshot/**/*.parquet"

    conn = duckdb.connect(database=':memory:')
    conn.execute("SET threads = 4")
    query = f"""
    SELECT DISTINCT snapshot_date
    FROM read_parquet('{snapshot_path}', hive_partitioning=1)
    WHERE snapshot_date = ?
    LIMIT 1
    """

    result = conn.execute(query, [as_of_date]).fetchone()
    conn.close()
    return _normalize_snapshot_date(result[0] if result else None)


def _query_stock_levels(snapshot_date: date, lookback_start: date, lookback_end: date, limit: int = 1000) -> pd.DataFrame:
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
    import polars as pl
    from services.duckdb_connector import query_inventory_snapshot, query_sales_by_product_duckdb
    
    on_hand_df = query_inventory_snapshot(snapshot_date)
    on_hand_df = on_hand_df.rename(columns={'qty_on_hand': 'on_hand_qty'})
    
    sales_df = query_sales_by_product_duckdb(lookback_start, lookback_end)
    sales_df = sales_df.rename(columns={'units_sold': 'units_sold'})
    
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    dim_path = f"{data_lake_root}/star-schema/dim_products.parquet"
    
    product_ids_with_inventory = set(on_hand_df[on_hand_df['qty_on_hand'] != 0]['product_id'].tolist())
    product_ids_with_sales = set(sales_df['product_id'].tolist())
    relevant_product_ids = product_ids_with_inventory.union(product_ids_with_sales)
    
    if os.path.exists(dim_path) and relevant_product_ids:
        dim_pl = pl.scan_parquet(dim_path).filter(
            pl.col('product_id').is_in(list(relevant_product_ids))
        )
        dim_df = dim_pl.collect().to_pandas()
    else:
        dim_df = pd.DataFrame(columns=['product_id', 'product_name', 'product_category', 
                                      'product_brand', 'product_barcode', 'product_sku'])
    
    on_hand_pl = pl.from_pandas(on_hand_df)
    
    if not sales_df.empty:
        sales_pl = pl.from_pandas(sales_df)
    else:
        product_id_dtype = on_hand_pl['product_id'].dtype
        sales_pl = pl.DataFrame(schema={
            'product_id': product_id_dtype,
            'units_sold': pl.Float64,
            'revenue': pl.Float64
        })
    
    if not dim_df.empty:
        dim_pl = pl.from_pandas(dim_df)
    else:
        product_id_dtype = on_hand_pl['product_id'].dtype
        dim_pl = pl.DataFrame(schema={
            'product_id': product_id_dtype,
            'product_name': pl.Utf8,
            'product_category': pl.Utf8,
            'product_brand': pl.Utf8,
            'product_barcode': pl.Utf8,
            'product_sku': pl.Utf8
        })
    
    result_pl = on_hand_pl.join(sales_pl, on='product_id', how='left')
    result_pl = result_pl.join(dim_pl, on='product_id', how='left')
    
    result_pl = result_pl.with_columns([
        pl.col('units_sold').fill_null(0),
        pl.lit(0).alias('reserved_qty'),
    ])
    
    result_pl = result_pl.with_columns([
        pl.coalesce([pl.col('product_name'), pl.format('Product {}', pl.col('product_id'))]).alias('product_name'),
        pl.coalesce([pl.col('product_category'), pl.lit('Unknown Category')]).alias('product_category'),
        pl.coalesce([pl.col('product_brand'), pl.lit('Unknown Brand')]).alias('product_brand'),
        pl.coalesce([pl.col('product_barcode'), pl.lit('')]).alias('product_barcode'),
        pl.coalesce([pl.col('product_sku'), pl.lit('')]).alias('product_sku')
    ])
    
    result_pl = result_pl.select([
        'product_id', 'product_name', 'product_category', 'product_brand',
        'product_barcode', 'product_sku', 'on_hand_qty', 'reserved_qty', 'units_sold'
    ])
    result_pl = result_pl.sort('on_hand_qty', descending=True)
    result_pl = result_pl.head(limit)
    
    result = result_pl.to_pandas()
    
    return result


def _data_lake_root() -> str:
    return os.environ.get('DATA_LAKE_ROOT') or os.environ.get('DATA_LAKE_PATH', '/app/data-lake')


def _resolve_ledger_baseline_path() -> Path:
    env_path = os.environ.get('STOCK_LEDGER_BASELINE_PATH')
    if env_path:
        return Path(env_path)
    
    data_lake_path = Path(_data_lake_root()) / 'star-schema' / _LEDGER_BASELINE_FILENAME
    if data_lake_path.exists():
        return data_lake_path
    
    return Path(__file__).resolve().parents[1] / _LEDGER_BASELINE_FILENAME


def _parse_reconcile_stocks_csv(path: Path) -> pd.DataFrame:
    rows = []
    with path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f)
        for outer in reader:
            if not outer:
                continue
            inner_text = outer[0]
            inner = next(csv.reader([inner_text]))
            if not inner or inner[0] == 'Product/ID':
                continue
            
            product_id = inner[1] if len(inner) > 1 else None
            qty = inner[3] if len(inner) > 3 else None
            rows.append({'product_id': product_id, 'qty': qty})
    
    df = pd.DataFrame(rows)
    df['product_id'] = pd.to_numeric(df.get('product_id'), errors='coerce')
    df['qty'] = pd.to_numeric(df.get('qty'), errors='coerce')
    df = df.dropna(subset=['product_id']).copy()
    df['product_id'] = df['product_id'].astype('int64')
    df['qty'] = df['qty'].fillna(0.0).astype('float64')
    df = df.groupby('product_id', as_index=False)['qty'].sum()
    return df


def _load_ledger_baseline() -> pd.DataFrame:
    baseline_path = _resolve_ledger_baseline_path()
    return _parse_reconcile_stocks_csv(baseline_path)


def _query_location_ledger_deltas(
    start_ts: datetime,
    end_ts: datetime,
    location_pool: set,
) -> pd.DataFrame:
    """Query inventory movement deltas using Polars parquet read."""
    import polars as pl
    
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    moves_path = f"{data_lake_root}/star-schema/fact_inventory_moves/**/*.parquet"
    
    pool_values = sorted(location_pool)
    
    if os.path.exists(moves_path.replace("/**/*.parquet", "")):
        df = pl.scan_parquet(moves_path, hive_partitioning=True).filter(
            (pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S") > start_ts) & 
            (pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S") <= end_ts)
        ).collect().to_pandas()
        
        if df.empty:
            return pd.DataFrame(columns=['product_id', 'qty_delta'])
        
        def calculate_delta(row):
            dest_id = row.get('location_dest_id')
            src_id = row.get('location_src_id')
            qty = abs(row.get('qty_moved', 0))
            
            if dest_id in pool_values and (src_id is None or src_id not in pool_values):
                return qty
            elif src_id in pool_values and (dest_id is None or dest_id not in pool_values):
                return -qty
            else:
                return 0
        
        df['qty_delta'] = df.apply(calculate_delta, axis=1)
        result = df.groupby('product_id', as_index=False).agg({'qty_delta': 'sum'})
        return result
    else:
        return pd.DataFrame(columns=['product_id', 'qty_delta'])


def get_stock_levels_ledger(
    as_of_date: date,
    lookback_days: int = DEFAULT_STOCK_LOOKBACK_DAYS,
    low_stock_days: int = DEFAULT_LOW_STOCK_DAYS,
) -> Dict[str, object]:
    if not isinstance(as_of_date, date):
        as_of_date = date.today()
    
    lookback_days = max(1, int(lookback_days or DEFAULT_STOCK_LOOKBACK_DAYS))
    
    cutoff_ts_local = datetime.combine(as_of_date, datetime.min.time()).replace(hour=7, minute=0, second=0)
    if cutoff_ts_local < _LEDGER_BASELINE_TS:
        empty_items = pd.DataFrame(columns=[
            'product_id', 'product_name', 'product_category', 'product_brand',
            'product_barcode', 'product_sku',
            'on_hand_qty', 'reserved_qty', 'units_sold', 'avg_daily_sold',
            'days_of_cover', 'low_stock_flag', 'dead_stock_flag',
        ])
        return {
            'snapshot_date': as_of_date,
            'as_of_ts': cutoff_ts_local,
            'baseline_ts': _LEDGER_BASELINE_TS,
            'location_id': _LEDGER_LOCATION_ID,
            'items': empty_items,
            'summary': {
                'total_on_hand': 0.0,
                'low_stock_count': 0,
                'dead_stock_count': 0,
                'lookback_days': lookback_days,
                'low_stock_days': low_stock_days,
            },
        }
    
    baseline_start = time.time()
    baseline_df = _load_ledger_baseline()
    print(f"[TIMING] _load_ledger_baseline: {time.time() - baseline_start:.3f}s")
    
    baseline_ts_utc = _to_utc_from_jakarta(_LEDGER_BASELINE_TS)
    cutoff_ts_utc = _to_utc_from_jakarta(cutoff_ts_local)
    
    delta_start = time.time()
    deltas_df = _query_location_ledger_deltas(baseline_ts_utc, cutoff_ts_utc, _LEDGER_LOCATION_POOL)
    print(f"[TIMING] _query_location_ledger_deltas: {time.time() - delta_start:.3f}s")
    
    df = baseline_df.merge(deltas_df, on='product_id', how='outer')
    df['qty'] = pd.to_numeric(df.get('qty'), errors='coerce').fillna(0.0)
    df['qty_delta'] = pd.to_numeric(df.get('qty_delta'), errors='coerce').fillna(0.0)
    df['on_hand_qty'] = df['qty'] + df['qty_delta']
    df['reserved_qty'] = 0.0
    df = df[['product_id', 'on_hand_qty', 'reserved_qty']].copy()
    
    lookback_start = as_of_date - timedelta(days=lookback_days - 1)
    
    from services.duckdb_connector import get_duckdb_connection
    
    conn = get_duckdb_connection()
    
    sales_start = time.time()
    sales_df = conn.execute(
        """
        SELECT product_id, SUM(quantity) AS units_sold
        FROM fact_sales_all
        WHERE date >= ? AND date < ? + INTERVAL 1 DAY
        GROUP BY 1
        """,
        [lookback_start, as_of_date],
    ).df()
    print(f"[TIMING] sales query (ledger): {time.time() - sales_start:.3f}s")
    
    df = df.merge(sales_df, on='product_id', how='left')
    df['units_sold'] = pd.to_numeric(df.get('units_sold'), errors='coerce').fillna(0.0)
    
    prod_start = time.time()
    products_df = conn.execute(
        """
        SELECT
            product_id,
            product_name,
            product_category,
            product_brand,
            product_barcode,
            product_sku
        FROM dim_products
        """
    ).df()
    print(f"[TIMING] products query: {time.time() - prod_start:.3f}s")
    
    df = df.merge(products_df, on='product_id', how='left')
    
    df['product_name'] = df['product_name'].fillna(df['product_id'].apply(lambda v: f'Product {v}'))
    df['product_category'] = df['product_category'].fillna('Unknown Category')
    df['product_brand'] = df['product_brand'].fillna('Unknown Brand')
    if 'product_barcode' in df.columns:
        df['product_barcode'] = df['product_barcode'].fillna('')
    if 'product_sku' in df.columns:
        df['product_sku'] = df['product_sku'].fillna('')
    
    df['avg_daily_sold'] = df['units_sold'] / float(lookback_days)
    df['days_of_cover'] = df['on_hand_qty'] / df['avg_daily_sold'].replace(0, pd.NA)
    df['low_stock_flag'] = df['days_of_cover'].notna() & (df['days_of_cover'] < low_stock_days)
    df['dead_stock_flag'] = (df['on_hand_qty'] > 0) & (df['units_sold'] <= 0)
    
    total_on_hand = float(df['on_hand_qty'].sum())
    if math.isclose(total_on_hand, 0.0, abs_tol=1e-9):
        total_on_hand = 0.0
    
    summary = {
        'total_on_hand': total_on_hand,
        'low_stock_count': int(df['low_stock_flag'].sum()),
        'dead_stock_count': int(df['dead_stock_flag'].sum()),
        'lookback_days': lookback_days,
        'low_stock_days': low_stock_days,
    }
    
    df = df[[
        'product_id', 'product_name', 'product_category', 'product_brand',
        'product_barcode', 'product_sku',
        'on_hand_qty', 'reserved_qty', 'units_sold', 'avg_daily_sold',
        'days_of_cover', 'low_stock_flag', 'dead_stock_flag',
    ]].copy()
    
    return {
        'snapshot_date': as_of_date,
        'as_of_ts': cutoff_ts_local,
        'baseline_ts': _LEDGER_BASELINE_TS,
        'location_pool': sorted(_LEDGER_LOCATION_POOL),
        'items': df,
        'summary': summary,
    }


# =============================================================================
# SELL-THROUGH ANALYSIS (IMPLEMENTED)
# =============================================================================

def get_sell_through_analysis(start_date: date, end_date: date) -> Dict[str, object]:
    """Get sell-through analysis combining inventory movements and sales.
    
    This function replaces the stubbed implementation with a memory-optimized
    approach using pre-aggregated data and Polars for efficient parquet reads.
    
    Sell-through calculation:
        sell_through = units_sold / (begin_on_hand + units_received) * 100
    
    Args:
        start_date: Analysis period start date (begin_on_hand is stock as of this date)
        end_date: Analysis period end date (ending inventory + final sales)
    
    Returns:
        Dictionary with items, categories, and summary DataFrames
    """
    import polars as pl
    from services.duckdb_connector import (
        get_duckdb_connection,
        query_inventory_snapshot,
        query_sales_by_product_duckdb,
    )
    
    query_start = time.time()
    
    # Get stock at beginning of period (as of start_date)
    begin_as_of_ts = datetime.combine(start_date, datetime.min.time()).replace(hour=7, minute=0, second=0)
    
    # Get stock at end of period (as of end_date)
    end_as_of_ts = datetime.combine(end_date, datetime.min.time()).replace(hour=7, minute=0, second=0)
    
    # Load baseline and compute beginning inventory
    # For simplicity, we'll use the DuckDB snapshot approach
    # This gives us stock levels as of a specific date
    
    # Get beginning inventory from DuckDB snapshot (or compute from ledger)
    conn = get_duckdb_connection()
    
    # Try to get stock at start_date from snapshots
    begin_stock_query = f"""
    SELECT 
        product_id,
        SUM(quantity) as begin_on_hand
    FROM read_parquet('{_data_lake_root()}/star-schema/fact_stock_on_hand_snapshot/**/*.parquet', 
                      hive_partitioning=1)
    WHERE snapshot_date <= ?
    GROUP BY product_id
    """
    
    # Get ending inventory from snapshots
    end_stock_query = f"""
    SELECT 
        product_id,
        SUM(quantity) as end_on_hand
    FROM read_parquet('{_data_lake_root()}/star-schema/fact_stock_on_hand_snapshot/**/*.parquet', 
                      hive_partitioning=1)
    WHERE snapshot_date <= ?
    GROUP BY product_id
    """
    
    try:
        begin_stock_df = conn.execute(begin_stock_query, [start_date]).fetchdf()
        end_stock_df = conn.execute(end_stock_query, [end_date]).fetchdf()
    except Exception as e:
        print(f"[WARN] DuckDB snapshot query failed, using empty: {e}")
        begin_stock_df = pd.DataFrame(columns=['product_id', 'begin_on_hand'])
        end_stock_df = pd.DataFrame(columns=['product_id', 'end_on_hand'])
    
    # Get sales in period from pre-aggregated table
    sales_df = query_sales_by_product_duckdb(start_date, end_date)
    sales_df = sales_df.rename(columns={'units_sold': 'units_sold', 'revenue': 'revenue'})
    
    print(f"[TIMING] sell-through data fetch: {time.time() - query_start:.3f}s")
    
    # Build result DataFrame with all product_ids
    all_products = set(begin_stock_df['product_id'].tolist()) if not begin_stock_df.empty else set()
    all_products.update(end_stock_df['product_id'].tolist() if not end_stock_df.empty else [])
    all_products.update(sales_df['product_id'].tolist() if not sales_df.empty else [])
    
    if not all_products:
        return _empty_sell_through_result(end_date)
    
    result = pd.DataFrame({'product_id': list(all_products)})
    
    # Merge stock data
    if not begin_stock_df.empty:
        result = result.merge(begin_stock_df, on='product_id', how='left')
    else:
        result['begin_on_hand'] = 0.0
    
    if not end_stock_df.empty:
        result = result.merge(end_stock_df, on='product_id', how='left')
    else:
        result['end_on_hand'] = 0.0
    
    # Merge sales data
    if not sales_df.empty:
        result = result.merge(sales_df[['product_id', 'units_sold']], on='product_id', how='left')
    else:
        result['units_sold'] = 0.0
    
    # Fill missing values
    result['begin_on_hand'] = pd.to_numeric(result['begin_on_hand'], errors='coerce').fillna(0.0)
    result['end_on_hand'] = pd.to_numeric(result['end_on_hand'], errors='coerce').fillna(0.0)
    result['units_sold'] = pd.to_numeric(result['units_sold'], errors='coerce').fillna(0.0)
    
    # Calculate units_received: ending - beginning + sold (rearranged inventory equation)
    # Inventory: begin + received - sold = end
    # Therefore: received = end - begin + sold
    result['units_received'] = result['end_on_hand'] - result['begin_on_hand'] + result['units_sold']
    result['units_received'] = result['units_received'].clip(lower=0)  # Only positive values
    
    # Get product details from dimensions
    dim_path = f"{_data_lake_root()}/star-schema/dim_products.parquet"
    
    if os.path.exists(dim_path):
        dim_pl = pl.scan_parquet(dim_path).filter(
            pl.col('product_id').is_in(result['product_id'].tolist())
        )
        dim_df = dim_pl.collect().to_pandas()
        
        if not dim_df.empty:
            result = result.merge(dim_df, on='product_id', how='left')
    
    # Fill missing product info
    result['product_name'] = result['product_name'].fillna(
        result['product_id'].apply(lambda x: f"Product {x}")
    )
    result['product_category'] = result['product_category'].fillna('Unknown Category')
    result['product_brand'] = result['product_brand'].fillna('Unknown Brand')
    result['product_barcode'] = result.get('product_barcode', '').fillna('')
    result['product_sku'] = result.get('product_sku', '').fillna('')
    
    # Calculate sell-through percentage
    # sell_through = units_sold / (begin_on_hand + units_received)
    denom = result['begin_on_hand'] + result['units_received']
    result['sell_through'] = result['units_sold'] / denom.where(denom > 0, 1)
    
    # Add additional breakdown columns (for UI compatibility)
    result['units_incoming'] = result['units_received'].copy()  # Simplified
    result['units_production_in'] = 0.0
    result['units_adjustment_net'] = 0.0
    result['units_production_out'] = 0.0
    result['units_transfer_net'] = 0.0
    
    # Apply memory-safe limit (top products by sales)
    MAX_SELL_THROUGH_ROWS = 5000
    if len(result) > MAX_SELL_THROUGH_ROWS:
        result = result.nlargest(MAX_SELL_THROUGH_ROWS, 'units_sold')
    
    print(f"[TIMING] get_sell_through_analysis total: {time.time() - query_start:.3f}s, rows={len(result)}")
    
    # Build categories summary
    if not result.empty:
        categories_df = (
            result.groupby('product_category')
            .agg(
                begin_on_hand=('begin_on_hand', 'sum'),
                units_received=('units_received', 'sum'),
                units_sold=('units_sold', 'sum'),
            )
            .reset_index()
        )
        denom = categories_df['begin_on_hand'] + categories_df['units_received']
        categories_df['sell_through'] = categories_df['units_sold'] / denom.where(denom > 0, 1)
    else:
        categories_df = pd.DataFrame(columns=[
            'product_category', 'begin_on_hand', 'units_received', 
            'units_sold', 'sell_through'
        ])
    
    # Calculate summary totals
    total_begin = float(result['begin_on_hand'].sum())
    total_received = float(result['units_received'].sum())
    total_sold = float(result['units_sold'].sum())
    overall_st = total_sold / (total_begin + total_received) if (total_begin + total_received) > 0 else 0
    
    summary = {
        'sell_through': overall_st,
        'units_sold': total_sold,
        'units_received': total_received,
        'begin_on_hand': total_begin,
    }
    
    # Select final columns for items
    result = result[[
        'product_id', 'product_name', 'product_category', 'product_brand',
        'product_barcode', 'product_sku',
        'begin_on_hand', 'units_received', 'units_incoming', 'units_production_in',
        'units_adjustment_net', 'units_production_out', 'units_transfer_net',
        'units_sold', 'sell_through',
    ]].copy()
    
    return {
        'snapshot_date': end_date,
        'items': result,
        'categories': categories_df,
        'summary': summary,
    }


def _empty_sell_through_result(snapshot_date: date) -> Dict[str, object]:
    """Return empty sell-through result structure."""
    return {
        'snapshot_date': snapshot_date,
        'items': pd.DataFrame(columns=[
            'product_id', 'product_name', 'product_category', 'product_brand',
            'product_barcode', 'product_sku',
            'begin_on_hand', 'units_received', 'units_incoming', 'units_production_in',
            'units_adjustment_net', 'units_production_out', 'units_transfer_net',
            'units_sold', 'sell_through',
        ]),
        'categories': pd.DataFrame(columns=[
            'product_category', 'begin_on_hand', 'units_received', 'units_sold', 'sell_through',
        ]),
        'summary': {
            'sell_through': 0.0,
            'units_sold': 0.0,
            'units_received': 0.0,
            'begin_on_hand': 0.0,
        },
    }


# =============================================================================
# ABC ANALYSIS (existing implementation)
# =============================================================================

def _query_abc_products(start_date: date, end_date: date) -> pd.DataFrame:
    """ABC analysis using DuckDB sales aggregates + Polars for dimensions."""
    import polars as pl
    from services.duckdb_connector import query_sales_by_product_duckdb
    
    query_start = time.time()
    
    sales_df = query_sales_by_product_duckdb(start_date, end_date)
    
    product_ids_with_sales = set(sales_df['product_id'].tolist())
    
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    dim_path = f"{data_lake_root}/star-schema/dim_products.parquet"
    
    if os.path.exists(dim_path) and product_ids_with_sales:
        dim_pl = pl.scan_parquet(dim_path).filter(
            pl.col('product_id').is_in(list(product_ids_with_sales))
        )
        dim_df = dim_pl.collect().to_pandas()
        result = sales_df.merge(dim_df, on='product_id', how='left')
        result = result[['product_id', 'revenue', 'units_sold', 'product_name', 'product_category', 'product_brand']]
        result = result.sort_values('revenue', ascending=False)
    else:
        result = sales_df
        result['product_name'] = None
        result['product_category'] = None
        result['product_brand'] = None
    
    print(f"[TIMING] get_abc_analysis: {time.time() - query_start:.3f}s")
    return result


def get_abc_analysis(
    start_date: date,
    end_date: date,
    a_threshold: float = DEFAULT_ABC_THRESHOLDS["a"],
    b_threshold: float = DEFAULT_ABC_THRESHOLDS["b"],
) -> Dict[str, pd.DataFrame]:
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    
    df = _query_abc_products(start_date, end_date)
    
    if df.empty:
        empty_summary = pd.DataFrame(columns=["abc_class", "sku_count", "revenue", "revenue_share"])
        empty_categories = pd.DataFrame(columns=["product_category", "abc_class", "revenue"])
        return {
            "items": df,
            "summary": empty_summary,
            "categories": empty_categories,
            "total_revenue": 0.0,
        }
    
    df = df.copy()
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0)
    df["units_sold"] = pd.to_numeric(df["units_sold"], errors="coerce").fillna(0)
    
    df = df.sort_values("revenue", ascending=False).reset_index(drop=True)
    total_revenue = float(df["revenue"].sum())
    sku_count = int(len(df))
    
    if total_revenue > 0:
        df["revenue_share"] = df["revenue"] / total_revenue
        df["cumulative_revenue"] = df["revenue"].cumsum()
        df["cumulative_share"] = df["cumulative_revenue"] / total_revenue
    else:
        df["revenue_share"] = 0.0
        df["cumulative_revenue"] = 0.0
        df["cumulative_share"] = 0.0
    
    df["sku_rank"] = df.index + 1
    df["cumulative_sku_share"] = df["sku_rank"] / float(sku_count) if sku_count > 0 else 0.0
    
    a_threshold = float(a_threshold or 0)
    b_threshold = float(b_threshold or 0)
    a_threshold = max(0.0, min(1.0, a_threshold))
    b_threshold = max(0.0, min(1.0, b_threshold))
    
    a_cutoff = max(1, int(math.ceil(a_threshold * sku_count))) if sku_count > 0 else 0
    b_cutoff = max(a_cutoff, int(math.ceil(b_threshold * sku_count))) if sku_count > 0 else 0
    
    def _classify(rank: int) -> str:
        if rank <= a_cutoff:
            return "A"
        if rank <= b_cutoff:
            return "B"
        return "C"
    
    df["abc_class"] = df["sku_rank"].apply(_classify)
    
    summary = (
        df.groupby("abc_class", as_index=False)
        .agg(
            sku_count=("product_id", "count"),
            revenue=("revenue", "sum"),
        )
    )
    summary["revenue_share"] = (
        summary["revenue"] / total_revenue if total_revenue > 0 else 0.0
    )
    
    summary["abc_class"] = pd.Categorical(summary["abc_class"], ["A", "B", "C"], ordered=True)
    summary = summary.sort_values("abc_class")
    
    categories = (
        df.groupby(["product_category", "abc_class"], as_index=False)
        .agg(revenue=("revenue", "sum"))
    )
    
    return {
        "items": df,
        "summary": summary,
        "categories": categories,
        "total_revenue": total_revenue,
    }


# =============================================================================
# INVENTORY SUMMARY (existing implementation)
# =============================================================================

def query_inventory_summary(
    snapshot_date: date,
    lookback_days: int = DEFAULT_STOCK_LOOKBACK_DAYS,
    overstock_days: int = 90,
    low_stock_days: int = 14,
) -> Dict:
    """Return summary counts for executive summary cards."""
    from services.duckdb_connector import query_inventory_snapshot, query_sales_by_product_duckdb
    
    lookback_days = min(lookback_days, 90)  # Max 90 days lookback
    lookback_start = snapshot_date - timedelta(days=lookback_days - 1)
    
    query_start = time.time()
    
    stock_df = query_inventory_snapshot(snapshot_date)
    stock_df = stock_df.rename(columns={'qty_on_hand': 'on_hand_qty'})
    
    sales_df = query_sales_by_product_duckdb(lookback_start, snapshot_date)
    
    product_ids_with_inventory = set(stock_df[stock_df['on_hand_qty'] != 0]['product_id'].tolist())
    if product_ids_with_inventory:
        sales_df = sales_df[sales_df['product_id'].isin(list(product_ids_with_inventory))]
    
    if not sales_df.empty:
        sales_df['avg_daily_sold'] = sales_df['units_sold'] / lookback_days
    else:
        sales_df = pd.DataFrame(columns=['product_id', 'units_sold', 'revenue', 'avg_daily_sold'])
    
    combined = stock_df.merge(sales_df, on='product_id', how='left')
    
    combined['units_sold'] = combined['units_sold'].fillna(0)
    combined['revenue'] = combined['revenue'].fillna(0)
    combined['avg_daily_sold'] = combined['avg_daily_sold'].fillna(0)
    
    combined['days_of_cover'] = combined.apply(
        lambda row: row['on_hand_qty'] / row['avg_daily_sold'] if row['avg_daily_sold'] > 0 else 999999,
        axis=1
    )
    
    def classify_stock_status(row):
        if row['units_sold'] == 0:
            return 'dead_stock'
        elif row['days_of_cover'] < low_stock_days:
            return 'low_stock'
        elif row['days_of_cover'] > overstock_days:
            return 'overstock'
        else:
            return 'healthy'
    
    combined['stock_status'] = combined.apply(classify_stock_status, axis=1)
    
    combined['est_stock_value'] = combined.apply(
        lambda row: row['on_hand_qty'] * (row['revenue'] / row['units_sold']) if row['units_sold'] > 0 else 0,
        axis=1
    )
    
    total_sku_count = len(combined)
    total_inventory_value = combined['est_stock_value'].sum()
    dead_stock_count = (combined['stock_status'] == 'dead_stock').sum()
    low_stock_count = (combined['stock_status'] == 'low_stock').sum()
    overstock_sku_count = (combined['stock_status'] == 'overstock').sum()
    overstock_value = combined[combined['stock_status'] == 'overstock']['est_stock_value'].sum()
    
    print(f"[TIMING] query_inventory_summary: {time.time() - query_start:.3f}s")
    
    return {
        'total_sku_count': int(total_sku_count),
        'total_inventory_value': float(total_inventory_value),
        'dead_stock_count': int(dead_stock_count),
        'low_stock_count': int(low_stock_count),
        'overstock_sku_count': int(overstock_sku_count),
        'overstock_value': float(overstock_value),
    }


# =============================================================================
# INVENTORY COSTS (existing implementation)
# =============================================================================

def get_inventory_costs(as_of_date: date) -> pd.DataFrame:
    """Fetch latest known unit cost per product as of the given date using Polars parquet reads."""
    import polars as pl
    
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    
    cost_latest_path = f"{data_lake_root}/star-schema/fact_product_cost_latest_daily/**/*.parquet"
    
    if os.path.exists(cost_latest_path.replace("/**/*.parquet", "")):
        purchase_df = pl.scan_parquet(cost_latest_path, hive_partitioning=True).filter(
            pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d") <= as_of_date
        ).collect().to_pandas()
        
        if not purchase_df.empty:
            purchase_df = purchase_df.sort_values(['product_id', 'date'], ascending=[True, False])
            purchase_df = purchase_df.drop_duplicates(subset=['product_id'], keep='first')
            purchase_costs = purchase_df[['product_id', 'cost_unit_tax_in']].copy()
        else:
            purchase_costs = pd.DataFrame(columns=['product_id', 'cost_unit_tax_in'])
    else:
        purchase_costs = pd.DataFrame(columns=['product_id', 'cost_unit_tax_in'])
    
    beginning_path = f"{data_lake_root}/star-schema/fact_product_beginning_costs.parquet"
    
    if os.path.exists(beginning_path):
        beginning_df = pl.read_parquet(beginning_path).to_pandas()
        
        if not beginning_df.empty:
            products_with_costs = set(purchase_costs['product_id']) if not purchase_costs.empty else set()
            beginning_df = beginning_df[
                (~beginning_df['product_id'].isin(products_with_costs)) &
                (beginning_df['is_active'] == True) &
                (beginning_df['effective_date'] <= as_of_date)
            ]
            beginning_costs = beginning_df[['product_id', 'cost_unit_tax_in']].copy()
        else:
            beginning_costs = pd.DataFrame(columns=['product_id', 'cost_unit_tax_in'])
    else:
        beginning_costs = pd.DataFrame(columns=['product_id', 'cost_unit_tax_in'])
    
    result = pd.concat([purchase_costs, beginning_costs], ignore_index=True)
    return result