import csv
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
import math
import time
from typing import Dict, Optional

import pandas as pd


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
    """Get snapshot date from DuckDB view over parquet."""
    from services.duckdb_connector import get_duckdb_connection
    import os
    
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    snapshot_path = f"{data_lake_root}/star-schema/fact_stock_on_hand_snapshot/**/*.parquet"
    
    conn = get_duckdb_connection()
    
    query = f"""
    SELECT DISTINCT snapshot_date
    FROM read_parquet('{snapshot_path}', hive_partitioning=1)
    WHERE snapshot_date = ?
    LIMIT 1
    """
    
    result = conn.execute(query, [as_of_date]).fetchone()
    return _normalize_snapshot_date(result[0] if result else None)


def _query_stock_levels(snapshot_date: date, lookback_start: date, lookback_end: date) -> pd.DataFrame:
    """Stock levels using DuckDB parquet reads + Polars for dimensions.
    
    Note: Cross-domain join (inventory + sales + dimensions).
    Uses DuckDB for inventory/sales aggregates, Polars for dimension parquet.
    """
    import polars as pl
    from services.duckdb_connector import query_inventory_snapshot, query_sales_by_product_duckdb
    
    # Get inventory snapshot from DuckDB parquet
    on_hand_df = query_inventory_snapshot(snapshot_date)
    on_hand_df = on_hand_df.rename(columns={'qty_on_hand': 'on_hand_qty'})
    
    # Get sales aggregates from DuckDB parquet
    sales_df = query_sales_by_product_duckdb(lookback_start, lookback_end)
    sales_df = sales_df.rename(columns={'units_sold': 'units_sold'})
    
    # Load dimensions from parquet using Polars
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    dim_path = f"{data_lake_root}/star-schema/dim_products.parquet"
    
    if os.path.exists(dim_path):
        dim_df = pl.read_parquet(dim_path).to_pandas()
    else:
        # Fallback: empty dimensions
        dim_df = pd.DataFrame(columns=['product_id', 'product_name', 'product_category', 
                                       'product_brand', 'product_barcode', 'product_sku'])
    
    # Join data
    result = on_hand_df.merge(sales_df, on='product_id', how='left')
    result = result.merge(dim_df, on='product_id', how='left')
    
    # Fill missing values
    result['units_sold'] = result['units_sold'].fillna(0)
    result['reserved_qty'] = 0  # Not available in current schema
    
    # Format product name fallback
    result['product_name'] = result['product_name'].fillna(
        result['product_id'].apply(lambda x: f'Product {x}')
    )
    result['product_category'] = result['product_category'].fillna('Unknown Category')
    result['product_brand'] = result['product_brand'].fillna('Unknown Brand')
    result['product_barcode'] = result['product_barcode'].fillna('')
    result['product_sku'] = result['product_sku'].fillna('')
    
    # Select and order columns
    result = result[['product_id', 'product_name', 'product_category', 'product_brand',
                     'product_barcode', 'product_sku', 'on_hand_qty', 'reserved_qty', 'units_sold']]
    result = result.sort_values('on_hand_qty', ascending=False)
    
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
    
    # Read and filter inventory moves using Polars
    if os.path.exists(moves_path.replace("/**/*.parquet", "")):
        df = pl.scan_parquet(moves_path, hive_partitioning=True).filter(
            (pl.col("movement_date") > start_ts) & 
            (pl.col("movement_date") <= end_ts)
        ).collect().to_pandas()
        
        if df.empty:
            return pd.DataFrame(columns=['product_id', 'qty_delta'])
        
        # Calculate qty_delta based on location pool logic
        def calculate_delta(row):
            dest_id = row.get('location_dest_id')
            src_id = row.get('location_src_id')
            qty = abs(row.get('qty_moved', 0))
            
            # Incoming to pool
            if dest_id in pool_values and (src_id is None or src_id not in pool_values):
                return qty
            # Outgoing from pool
            elif src_id in pool_values and (dest_id is None or dest_id not in pool_values):
                return -qty
            else:
                return 0
        
        df['qty_delta'] = df.apply(calculate_delta, axis=1)
        
        # Aggregate by product
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

    # movement_date in fact_inventory_moves is stored as UTC; convert local cutoffs (UTC+07) to UTC.
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
    ensure_duckdb_view_groups({"sales", "dims", "inventory"})
    
    # Load materialized views for faster queries
    db_manager = DuckDBManager()
    db_manager.ensure_materialized_views({"mv_inventory_status", "mv_inventory_daily", "mv_product_velocity"})
    
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



# _query_sell_through and get_sell_through_analysis removed - deferred due to complexity
# Previously used SQLite MVs which no longer exist


def _query_abc_products(start_date: date, end_date: date) -> pd.DataFrame:
    """ABC analysis using DuckDB sales aggregates + Polars for dimensions.
    
    Note: This query requires cross-domain join (sales + dimensions).
    Uses DuckDB parquet for sales aggregates and Polars for dimension parquet.
    """
    import polars as pl
    from services.duckdb_connector import query_sales_by_product_duckdb
    
    query_start = time.time()
    
    # Get sales aggregates from DuckDB parquet
    sales_df = query_sales_by_product_duckdb(start_date, end_date)
    
    # Load dimensions from parquet using Polars
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    dim_path = f"{data_lake_root}/star-schema/dim_products.parquet"
    
    if os.path.exists(dim_path):
        dim_df = pl.read_parquet(dim_path).to_pandas()
        # Join sales with dimensions
        result = sales_df.merge(dim_df, on='product_id', how='left')
        result = result[['product_id', 'revenue', 'quantity', 'product_name', 'product_category', 'product_brand']]
        result = result.sort_values('revenue', ascending=False)
    else:
        # Fallback: return sales data without dimensions
        result = sales_df
        result['product_name'] = None
        result['product_category'] = None
        result['product_brand'] = None
    
    print(f"[TIMING] get_abc_analysis: {time.time() - query_start:.3f}s")
    return result


def query_inventory_summary(
    snapshot_date: date,
    lookback_days: int = DEFAULT_STOCK_LOOKBACK_DAYS,
    overstock_days: int = 90,
    low_stock_days: int = 14,
) -> Dict:
    """
    Return summary counts for executive summary cards using DuckDB parquet reads.

    Returns: {
        'overstock_value': float,  # inventory value of overstock items
        'overstock_sku_count': int,
        'low_stock_count': int,     # SKUs with < low_stock_days cover
        'dead_stock_count': int,    # SKUs with no sales in lookback period
        'total_inventory_value': float,
        'total_sku_count': int,
    }
    """
    from services.duckdb_connector import query_inventory_snapshot, query_sales_by_product_duckdb
    
    # Defensive: Limit lookback to prevent excessive queries
    lookback_days = min(lookback_days, 90)  # Max 90 days lookback
    lookback_start = snapshot_date - timedelta(days=lookback_days - 1)

    query_start = time.time()
    
    # Get inventory snapshot from DuckDB parquet
    stock_df = query_inventory_snapshot(snapshot_date)
    stock_df = stock_df.rename(columns={'qty_on_hand': 'on_hand_qty'})
    
    # Get sales aggregates from DuckDB parquet
    sales_df = query_sales_by_product_duckdb(lookback_start, snapshot_date)
    
    # Calculate avg_daily_sold from sales data
    if not sales_df.empty:
        sales_df['avg_daily_sold'] = sales_df['units_sold'] / lookback_days
    else:
        sales_df = pd.DataFrame(columns=['product_id', 'units_sold', 'revenue', 'avg_daily_sold'])
    
    # Join stock and sales data
    combined = stock_df.merge(sales_df, on='product_id', how='left')
    
    # Fill missing values
    combined['units_sold'] = combined['units_sold'].fillna(0)
    combined['revenue'] = combined['revenue'].fillna(0)
    combined['avg_daily_sold'] = combined['avg_daily_sold'].fillna(0)
    
    # Calculate days_of_cover
    combined['days_of_cover'] = combined.apply(
        lambda row: row['on_hand_qty'] / row['avg_daily_sold'] if row['avg_daily_sold'] > 0 else 999999,
        axis=1
    )
    
    # Classify stock_status
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
    
    # Calculate est_stock_value
    combined['est_stock_value'] = combined.apply(
        lambda row: row['on_hand_qty'] * (row['revenue'] / row['units_sold']) if row['units_sold'] > 0 else 0,
        axis=1
    )
    
    # Calculate summary metrics
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


def get_inventory_costs(as_of_date: date) -> pd.DataFrame:
    """Fetch latest known unit cost per product as of the given date using Polars parquet reads.

    First tries purchase history from fact_product_cost_latest_daily.
    Falls back to beginning costs from CSV for products never purchased.

    Returns DataFrame with columns: product_id, cost_unit_tax_in
    """
    import polars as pl
    
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    
    # Query 1: Get costs from purchase history
    cost_latest_path = f"{data_lake_root}/star-schema/fact_product_cost_latest_daily/**/*.parquet"
    
    if os.path.exists(cost_latest_path.replace("/**/*.parquet", "")):
        purchase_df = pl.scan_parquet(cost_latest_path, hive_partitioning=True).filter(
            pl.col("date") <= as_of_date
        ).collect().to_pandas()
        
        if not purchase_df.empty:
            # Get latest cost per product as of as_of_date
            purchase_df = purchase_df.sort_values(['product_id', 'date'], ascending=[True, False])
            purchase_df = purchase_df.drop_duplicates(subset=['product_id'], keep='first')
            purchase_costs = purchase_df[['product_id', 'cost_unit_tax_in']].copy()
        else:
            purchase_costs = pd.DataFrame(columns=['product_id', 'cost_unit_tax_in'])
    else:
        purchase_costs = pd.DataFrame(columns=['product_id', 'cost_unit_tax_in'])
    
    # Query 2: Get beginning costs for products without purchase history
    # Only use beginning costs if as_of_date >= effective_date (2025-02-10)
    beginning_path = f"{data_lake_root}/star-schema/fact_product_beginning_costs.parquet"
    
    if os.path.exists(beginning_path):
        beginning_df = pl.read_parquet(beginning_path).to_pandas()
        
        if not beginning_df.empty:
            # Filter for products not in purchase_costs
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
    
    # Combine results
    result = pd.concat([purchase_costs, beginning_costs], ignore_index=True)
    return result

    # Combine both sources
    if purchase_costs.empty:
        return beginning_costs
    if beginning_costs.empty:
        return purchase_costs

    return pd.concat([purchase_costs, beginning_costs], ignore_index=True)


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
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

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
