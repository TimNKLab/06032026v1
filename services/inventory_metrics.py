import csv
import os
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Iterable
import math
from typing import Dict, Optional

import pandas as pd

from services.duckdb_connector import get_duckdb_connection

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
    baseline_df = _load_ledger_baseline()

    # movement_date in fact_inventory_moves is stored as UTC; convert local cutoffs (UTC+07) to UTC.
    baseline_ts_utc = _to_utc_from_jakarta(_LEDGER_BASELINE_TS)
    cutoff_ts_utc = _to_utc_from_jakarta(cutoff_ts_local)
    deltas_df = _query_location_ledger_deltas(baseline_ts_utc, cutoff_ts_utc, _LEDGER_LOCATION_POOL)

    df = baseline_df.merge(deltas_df, on='product_id', how='outer')
    df['qty'] = pd.to_numeric(df.get('qty'), errors='coerce').fillna(0.0)
    df['qty_delta'] = pd.to_numeric(df.get('qty_delta'), errors='coerce').fillna(0.0)
    df['on_hand_qty'] = df['qty'] + df['qty_delta']
    df['reserved_qty'] = 0.0
    df = df[['product_id', 'on_hand_qty', 'reserved_qty']].copy()

    lookback_start = as_of_date - timedelta(days=lookback_days - 1)
    conn = get_duckdb_connection()
    sales_df = conn.execute(
        """
        SELECT product_id, SUM(quantity) AS units_sold
        FROM fact_sales_all
        WHERE date >= ? AND date < ? + INTERVAL 1 DAY
        GROUP BY 1
        """,
        [lookback_start, as_of_date],
    ).df()

    df = df.merge(sales_df, on='product_id', how='left')
    df['units_sold'] = pd.to_numeric(df.get('units_sold'), errors='coerce').fillna(0.0)

    products_df = conn.execute(
        """
        SELECT product_id, product_name, product_category, product_brand
        FROM dim_products
        """
    ).df()
    df = df.merge(products_df, on='product_id', how='left')

    df['product_name'] = df['product_name'].fillna(df['product_id'].apply(lambda v: f'Product {v}'))
    df['product_category'] = df['product_category'].fillna('Unknown Category')
    df['product_brand'] = df['product_brand'].fillna('Unknown Brand')

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

    snapshot_date = _get_snapshot_date(start_date)
    empty_items = pd.DataFrame(columns=[
        "product_id", "product_name", "product_category", "product_brand",
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
    conn = get_duckdb_connection()
    query = """
        SELECT
            f.product_id,
            COALESCE(p.product_name, 'Product ' || f.product_id::VARCHAR) AS product_name,
            COALESCE(p.product_category, 'Unknown Category') AS product_category,
            COALESCE(p.product_brand, 'Unknown Brand') AS product_brand,
            SUM(f.revenue) AS revenue,
            SUM(f.quantity) AS quantity
        FROM fact_sales_all f
        LEFT JOIN dim_products p ON f.product_id = p.product_id
        WHERE f.date >= ? AND f.date < ? + INTERVAL 1 DAY
        GROUP BY 1, 2, 3, 4
        ORDER BY revenue DESC
    """

    return conn.execute(query, [start_date, end_date]).df()


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
