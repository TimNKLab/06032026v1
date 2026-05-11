import csv
import os
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Iterable
import math
import time
from typing import Dict, Optional

import pandas as pd

from services.duckdb_connector import get_duckdb_connection
from services.duckdb_connector import ensure_duckdb_view_groups
from services.duckdb_connector import DuckDBManager

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
    ensure_duckdb_view_groups({"inventory"})
    conn = get_duckdb_connection()
    row = conn.execute(
        """
        SELECT snapshot_date
        FROM fact_stock_on_hand_snapshot
        WHERE snapshot_date = ?
        """,
        [as_of_date],
    ).fetchone()
    return _normalize_snapshot_date(row[0] if row else None)


def _query_stock_levels(snapshot_date: date, lookback_start: date, lookback_end: date) -> pd.DataFrame:
    ensure_duckdb_view_groups({"inventory", "sales", "dims"})
    conn = get_duckdb_connection()
    query = """
        WITH on_hand AS (
            SELECT
                product_id,
                SUM(quantity) AS on_hand_qty,
                SUM(reserved_quantity) AS reserved_qty
            FROM fact_stock_on_hand_snapshot
            WHERE snapshot_date = ?
            GROUP BY 1
        ),
        sales AS (
            SELECT
                product_id,
                SUM(quantity) AS units_sold
            FROM fact_sales_all
            WHERE date >= ? AND date < ? + INTERVAL 1 DAY
            GROUP BY 1
        )
        SELECT
            o.product_id,
            COALESCE(p.product_name, 'Product ' || o.product_id::VARCHAR) AS product_name,
            COALESCE(p.product_category, 'Unknown Category') AS product_category,
            COALESCE(p.product_brand, 'Unknown Brand') AS product_brand,
            COALESCE(p.product_barcode, '') AS product_barcode,
            COALESCE(p.product_sku, '') AS product_sku,
            o.on_hand_qty,
            o.reserved_qty,
            COALESCE(s.units_sold, 0) AS units_sold
        FROM on_hand o
        LEFT JOIN sales s ON o.product_id = s.product_id
        LEFT JOIN dim_products p ON o.product_id = p.product_id
        ORDER BY o.on_hand_qty DESC
    """

    return conn.execute(query, [snapshot_date, lookback_start, lookback_end]).df()


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


@lru_cache(maxsize=1)
def _load_ledger_baseline() -> pd.DataFrame:
    baseline_path = _resolve_ledger_baseline_path()
    return _parse_reconcile_stocks_csv(baseline_path)


def _query_location_ledger_deltas(
    start_ts: datetime,
    end_ts: datetime,
    location_pool: set,
) -> pd.DataFrame:
    ensure_duckdb_view_groups({"inventory"})
    conn = get_duckdb_connection()
    pool_values = sorted(location_pool)
    pool_sql = ",".join(str(v) for v in pool_values)
    query = """
        SELECT
            product_id,
            SUM(
                CASE
                    WHEN location_dest_id IN ({pool_sql}) AND (location_src_id IS NULL OR location_src_id NOT IN ({pool_sql}))
                        THEN ABS(qty_moved)
                    WHEN location_src_id IN ({pool_sql}) AND (location_dest_id IS NULL OR location_dest_id NOT IN ({pool_sql}))
                        THEN -ABS(qty_moved)
                    ELSE 0
                END
            ) AS qty_delta
        FROM fact_inventory_moves
        WHERE movement_date > ? AND movement_date <= ?
        GROUP BY 1
    """.format(pool_sql=pool_sql)
    return conn.execute(query, [start_ts, end_ts]).df()


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


def get_stock_levels(
    as_of_date: date,
    lookback_days: int = DEFAULT_STOCK_LOOKBACK_DAYS,
    low_stock_days: int = DEFAULT_LOW_STOCK_DAYS,
) -> Dict[str, object]:
    if not isinstance(as_of_date, date):
        as_of_date = date.today()

    lookback_days = max(1, int(lookback_days or DEFAULT_STOCK_LOOKBACK_DAYS))
    snapshot_date = _get_snapshot_date(as_of_date)

    empty_items = pd.DataFrame(columns=[
        "product_id", "product_name", "product_category", "product_brand",
        "on_hand_qty", "reserved_qty", "units_sold", "avg_daily_sold",
        "days_of_cover", "low_stock_flag", "dead_stock_flag",
    ])

    if snapshot_date is None:
        return {
            "snapshot_date": None,
            "items": empty_items,
            "summary": {
                "total_on_hand": 0.0,
                "low_stock_count": 0,
                "dead_stock_count": 0,
                "lookback_days": lookback_days,
                "low_stock_days": low_stock_days,
            },
        }

    lookback_start = as_of_date - timedelta(days=lookback_days - 1)
    df = _query_stock_levels(snapshot_date, lookback_start, as_of_date)

    if df.empty:
        return {
            "snapshot_date": snapshot_date,
            "items": empty_items,
            "summary": {
                "total_on_hand": 0.0,
                "low_stock_count": 0,
                "dead_stock_count": 0,
                "lookback_days": lookback_days,
                "low_stock_days": low_stock_days,
            },
        }

    df = df.copy()
    df["on_hand_qty"] = pd.to_numeric(df["on_hand_qty"], errors="coerce").fillna(0)
    df["reserved_qty"] = pd.to_numeric(df["reserved_qty"], errors="coerce").fillna(0)
    df["units_sold"] = pd.to_numeric(df["units_sold"], errors="coerce").fillna(0)

    df["avg_daily_sold"] = df["units_sold"] / float(lookback_days)
    df["days_of_cover"] = df["on_hand_qty"] / df["avg_daily_sold"].replace(0, pd.NA)

    df["low_stock_flag"] = df["days_of_cover"].notna() & (df["days_of_cover"] < low_stock_days)
    df["dead_stock_flag"] = (df["on_hand_qty"] > 0) & (df["units_sold"] <= 0)

    total_on_hand = float(df["on_hand_qty"].sum())
    if math.isclose(total_on_hand, 0.0, abs_tol=1e-9):
        total_on_hand = 0.0

    summary = {
        "total_on_hand": total_on_hand,
        "low_stock_count": int(df["low_stock_flag"].sum()),
        "dead_stock_count": int(df["dead_stock_flag"].sum()),
        "lookback_days": lookback_days,
        "low_stock_days": low_stock_days,
    }

    return {
        "snapshot_date": snapshot_date,
        "items": df,
        "summary": summary,
    }


def _query_sell_through(snapshot_date: date, start_date: date, end_date: date) -> pd.DataFrame:
    ensure_duckdb_view_groups({"inventory", "sales", "dims"})
    
    # Load inventory materialized views for faster queries
    db_manager = DuckDBManager()
    db_manager.ensure_materialized_views({"mv_inventory_status", "mv_inventory_daily", "mv_product_velocity"})
    
    conn = get_duckdb_connection()
    query = """
        WITH begin_on_hand AS (
            SELECT
                product_id,
                SUM(quantity) AS begin_on_hand
            FROM fact_stock_on_hand_snapshot
            WHERE snapshot_date = ?
            GROUP BY 1
        ),
        sales AS (
            SELECT
                product_id,
                SUM(quantity) AS units_sold
            FROM fact_sales_all
            WHERE date >= ? AND date < ? + INTERVAL 1 DAY
            GROUP BY 1
        ),
        moves AS (
            SELECT
                product_id,
                SUM(
                    CASE
                        WHEN qty_moved > 0
                             AND (
                                COALESCE(movement_type, '') = 'incoming'
                                OR (
                                    COALESCE(movement_type, '') = ''
                                    AND COALESCE(picking_type_code, '') = 'incoming'
                                )
                             )
                        THEN qty_moved
                        ELSE 0
                    END
                ) AS units_incoming,
                SUM(
                    CASE
                        WHEN qty_moved > 0 AND COALESCE(movement_type, '') = 'production_in'
                        THEN qty_moved
                        ELSE 0
                    END
                ) AS units_production_in,
                SUM(
                    CASE
                        WHEN COALESCE(movement_type, '') = 'adjustment'
                        THEN qty_moved
                        ELSE 0
                    END
                ) AS units_adjustment_net,
                SUM(
                    CASE
                        WHEN COALESCE(movement_type, '') = 'production_out'
                        THEN qty_moved
                        ELSE 0
                    END
                ) AS units_production_out,
                SUM(
                    CASE
                        WHEN COALESCE(movement_type, '') = 'transfer'
                        THEN qty_moved
                        ELSE 0
                    END
                ) AS units_transfer_net
            FROM fact_inventory_moves
            WHERE movement_date >= ? AND movement_date < ? + INTERVAL 1 DAY
            GROUP BY 1
        ),
        combined AS (
            SELECT
                COALESCE(b.product_id, s.product_id, m.product_id) AS product_id,
                COALESCE(b.begin_on_hand, 0) AS begin_on_hand,
                COALESCE(m.units_incoming, 0) + COALESCE(m.units_production_in, 0) AS units_received,
                COALESCE(m.units_incoming, 0) AS units_incoming,
                COALESCE(m.units_production_in, 0) AS units_production_in,
                COALESCE(m.units_adjustment_net, 0) AS units_adjustment_net,
                COALESCE(m.units_production_out, 0) AS units_production_out,
                COALESCE(m.units_transfer_net, 0) AS units_transfer_net,
                COALESCE(s.units_sold, 0) AS units_sold
            FROM begin_on_hand b
            FULL JOIN sales s ON b.product_id = s.product_id
            FULL JOIN moves m ON COALESCE(b.product_id, s.product_id) = m.product_id
        )
        SELECT
            c.product_id,
            COALESCE(p.product_name, 'Product ' || c.product_id::VARCHAR) AS product_name,
            COALESCE(p.product_category, 'Unknown Category') AS product_category,
            COALESCE(p.product_brand, 'Unknown Brand') AS product_brand,
            COALESCE(p.product_barcode, '') AS product_barcode,
            COALESCE(p.product_sku, '') AS product_sku,
            c.begin_on_hand,
            c.units_received,
            c.units_incoming,
            c.units_production_in,
            c.units_adjustment_net,
            c.units_production_out,
            c.units_transfer_net,
            c.units_sold,
            CASE
                WHEN (c.begin_on_hand + c.units_received) = 0 THEN NULL
                ELSE c.units_sold / (c.begin_on_hand + c.units_received)
            END AS sell_through
        FROM combined c
        LEFT JOIN dim_products p ON c.product_id = p.product_id
        WHERE c.product_id IS NOT NULL
        ORDER BY c.units_sold DESC
    """

    return conn.execute(query, [snapshot_date, start_date, end_date, start_date, end_date]).df()


def get_sell_through_analysis(start_date: date, end_date: date) -> Dict[str, object]:
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    sell_through_start = time.time()
    snapshot_date = _get_snapshot_date(start_date)
    empty_items = pd.DataFrame(columns=[
        "product_id", "product_name", "product_category", "product_brand",
        "product_barcode", "product_sku",
        "begin_on_hand", "units_received", "units_incoming", "units_production_in",
        "units_adjustment_net", "units_production_out", "units_transfer_net",
        "units_sold", "sell_through",
    ])

    empty_categories = pd.DataFrame(columns=[
        "product_category", "begin_on_hand", "units_received", "units_sold", "sell_through",
    ])

    if snapshot_date is None:
        return {
            "snapshot_date": None,
            "items": empty_items,
            "categories": empty_categories,
            "summary": {
                "sell_through": 0.0,
                "units_sold": 0.0,
                "units_received": 0.0,
                "begin_on_hand": 0.0,
            },
        }

    items_df = _query_sell_through(snapshot_date, start_date, end_date)
    if items_df.empty:
        return {
            "snapshot_date": snapshot_date,
            "items": empty_items,
            "categories": empty_categories,
            "summary": {
                "sell_through": 0.0,
                "units_sold": 0.0,
                "units_received": 0.0,
                "begin_on_hand": 0.0,
            },
        }

    items_df = items_df.copy()
    for col in [
        "begin_on_hand",
        "units_received",
        "units_incoming",
        "units_production_in",
        "units_adjustment_net",
        "units_production_out",
        "units_transfer_net",
        "units_sold",
        "sell_through",
    ]:
        items_df[col] = pd.to_numeric(items_df[col], errors="coerce").fillna(0)

    categories_df = (
        items_df
        .groupby("product_category", as_index=False)
        .agg(
            begin_on_hand=("begin_on_hand", "sum"),
            units_received=("units_received", "sum"),
            units_sold=("units_sold", "sum"),
        )
    )

    categories_df["sell_through"] = categories_df.apply(
        lambda row: row["units_sold"] / (row["begin_on_hand"] + row["units_received"])
        if (row["begin_on_hand"] + row["units_received"]) > 0 else 0,
        axis=1,
    )

    total_begin = float(items_df["begin_on_hand"].sum())
    total_received = float(items_df["units_received"].sum())
    total_sold = float(items_df["units_sold"].sum())

    denom = total_begin + total_received
    overall_sell_through = total_sold / denom if denom > 0 else 0.0

    print(f"[TIMING] get_sell_through_analysis: {time.time() - sell_through_start:.3f}s")
    
    return {
        "snapshot_date": snapshot_date,
        "items": items_df,
        "categories": categories_df,
        "summary": {
            "sell_through": overall_sell_through,
            "units_sold": total_sold,
            "units_received": total_received,
            "begin_on_hand": total_begin,
        },
    }


def _query_abc_products(start_date: date, end_date: date) -> pd.DataFrame:
    ensure_duckdb_view_groups({"sales", "dims", "sales_agg"})
    
    # Load sales materialized views for faster queries
    db_manager = DuckDBManager()
    db_manager.ensure_materialized_views({"mv_sales_daily", "mv_sales_by_product"})
    
    conn = get_duckdb_connection()
    
    query_start = time.time()
    query = """
        SELECT
            s.product_id,
            SUM(s.revenue) as revenue,
            SUM(s.quantity) as quantity,
            p.product_name,
            p.product_category,
            p.product_brand
        FROM fact_sales_all s
        LEFT JOIN dim_products p ON s.product_id = p.product_id
        WHERE s.date >= ? AND s.date < ? + INTERVAL 1 DAY
        GROUP BY 1, 4, 5, 6
        ORDER BY 2 DESC
    """
    
    result = conn.execute(query, [start_date, end_date]).fetchdf()
    print(f"[TIMING] get_abc_analysis: {time.time() - query_start:.3f}s")
    return result


def query_inventory_summary(
    snapshot_date: date,
    lookback_days: int = DEFAULT_STOCK_LOOKBACK_DAYS,
    overstock_days: int = 90,
    low_stock_days: int = 14,
) -> Dict:
    """
    Return summary counts for executive summary cards.

    Returns: {
        'overstock_value': float,  # inventory value of overstock items
        'overstock_sku_count': int,
        'low_stock_count': int,     # SKUs with < low_stock_days cover
        'dead_stock_count': int,    # SKUs with no sales in lookback period
        'total_inventory_value': float,
        'total_sku_count': int,
    }
    """
    from services.duckdb_connector import get_duckdb_connection
    from services.duckdb_connector import ensure_duckdb_view_groups

    # Defensive: Limit lookback to prevent excessive queries
    lookback_days = min(lookback_days, 90)  # Max 90 days lookback
    lookback_start = snapshot_date - timedelta(days=lookback_days - 1)

    ensure_duckdb_view_groups({"inventory", "sales", "dims"})
    
    # Load materialized views for ultra-fast queries
    db_manager = DuckDBManager()
    db_manager.ensure_materialized_views({"mv_inventory_status", "mv_inventory_daily", "mv_product_velocity"})
    
    conn = get_duckdb_connection()

    query_start = time.time()
    
    # Use materialized views for faster query performance
    query = """
        WITH latest_stock AS (
            SELECT 
                product_id,
                on_hand_qty,
                available_qty,
                avg_daily_sold,
                days_of_cover,
                stock_status
            FROM mv_inventory_status
        ),
        sales_30d AS (
            SELECT 
                product_id,
                SUM(quantity) AS units_sold,
                SUM(revenue) AS revenue
            FROM fact_sales_all
            WHERE date >= ? AND date <= ?
            GROUP BY 1
        ),
        with_value AS (
            SELECT
                ls.product_id,
                ls.on_hand_qty,
                ls.days_of_cover,
                ls.stock_status,
                COALESCE(s.revenue, 0) AS revenue,
                COALESCE(s.units_sold, 0) AS units_sold,
                CASE
                    WHEN COALESCE(s.units_sold, 0) = 0 THEN 0
                    ELSE ls.on_hand_qty * (s.revenue / NULLIF(s.units_sold, 0))
                END AS est_stock_value
            FROM latest_stock ls
            LEFT JOIN sales_30d s ON ls.product_id = s.product_id
        )
        SELECT
            COUNT(*) AS total_sku_count,
            SUM(est_stock_value) AS total_inventory_value,
            SUM(CASE WHEN stock_status = 'dead_stock' THEN 1 ELSE 0 END) AS dead_stock_count,
            SUM(CASE WHEN stock_status = 'low_stock' THEN 1 ELSE 0 END) AS low_stock_count,
            SUM(CASE WHEN stock_status = 'overstock' THEN 1 ELSE 0 END) AS overstock_sku_count,
            SUM(CASE WHEN stock_status = 'overstock' THEN est_stock_value ELSE 0 END) AS overstock_value
        FROM with_value
    """

    result = conn.execute(
        query,
        [lookback_start, snapshot_date]
    ).fetchone()
    
    print(f"[TIMING] query_inventory_summary: {time.time() - query_start:.3f}s")

    return {
        'total_sku_count': int(result[0] or 0),
        'total_inventory_value': float(result[1] or 0),
        'dead_stock_count': int(result[2] or 0),
        'low_stock_count': int(result[3] or 0),
        'overstock_sku_count': int(result[4] or 0),
        'overstock_value': float(result[5] or 0),
    }


def get_inventory_costs(as_of_date: date) -> pd.DataFrame:
    """Fetch latest known unit cost per product as of the given date.

    First tries purchase history from fact_product_cost_latest_daily.
    Falls back to beginning costs from CSV for products never purchased.

    Returns DataFrame with columns: product_id, cost_unit_tax_in
    """
    ensure_duckdb_view_groups({"profit_detail"})
    conn = get_duckdb_connection()

    # Query 1: Get costs from purchase history
    purchase_query = """
        SELECT
            product_id,
            cost_unit_tax_in
        FROM fact_product_cost_latest_daily
        WHERE date <= ?
        QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY date DESC) = 1
    """
    purchase_costs = conn.execute(purchase_query, [as_of_date]).fetchdf()

    # Query 2: Get beginning costs for products without purchase history
    # Only use beginning costs if as_of_date >= effective_date (2025-02-10)
    beginning_query = """
        SELECT
            b.product_id,
            b.cost_unit_tax_in
        FROM fact_product_beginning_costs b
        LEFT JOIN (
            SELECT DISTINCT product_id
            FROM fact_product_cost_latest_daily
            WHERE date <= ?
        ) p ON b.product_id = p.product_id
        WHERE p.product_id IS NULL
          AND b.is_active = TRUE
          AND b.effective_date <= ?
    """
    beginning_costs = conn.execute(beginning_query, [as_of_date, as_of_date]).fetchdf()

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
