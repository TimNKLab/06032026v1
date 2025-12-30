import duckdb
import os
import logging
import threading
from datetime import date, timedelta
from typing import Dict, Optional
from functools import lru_cache
import pandas as pd

logger = logging.getLogger(__name__)


class DuckDBManager:
    _instance: Optional['DuckDBManager'] = None
    _connection: Optional[duckdb.DuckDBPyConnection] = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            with self._lock:
                if self._connection is None:
                    conn = duckdb.connect(database=':memory:')
                    self._setup_views(conn)  # Setup before assignment
                    self._connection = conn
                    self._initialized = True
        return self._connection

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_data_paths() -> tuple:
        """Cache data paths to avoid repeated env lookups."""
        data_lake = os.environ.get('DATA_LAKE_ROOT') or os.environ.get('DATA_LAKE_PATH', '/app/data-lake')
        return (
            f"{data_lake}/star-schema/fact_sales",
            f"{data_lake}/star-schema/dim_products.parquet",
            f"{data_lake}/star-schema/dim_categories.parquet",
            f"{data_lake}/star-schema/dim_brands.parquet",
        )

    def _setup_views(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Setup DuckDB views - fails fast on errors."""
        fact_path, dim_products, dim_categories, dim_brands = self._get_data_paths()

        # Use TRY_CAST and COALESCE in view definition instead of DESCRIBE
        # This handles missing columns gracefully at query time
        conn.execute(f"""
            CREATE OR REPLACE VIEW fact_sales AS
            SELECT 
                date,
                COALESCE(TRY_CAST(order_id AS BIGINT), 0) AS order_id,
                COALESCE(order_ref, '') AS order_ref,
                COALESCE(TRY_CAST(pos_config_id AS BIGINT), 0) AS pos_config_id,
                COALESCE(TRY_CAST(cashier_id AS BIGINT), 0) AS cashier_id,
                COALESCE(TRY_CAST(customer_id AS BIGINT), 0) AS customer_id,
                COALESCE(payment_method_ids, '') AS payment_method_ids,
                COALESCE(TRY_CAST(line_id AS BIGINT), 0) AS line_id,
                product_id,
                quantity,
                revenue
            FROM read_parquet('{fact_path}/*.parquet', union_by_name=True, filename=true)
        """)

        conn.execute(f"""
            CREATE OR REPLACE VIEW dim_products AS
            SELECT product_id, product_category, product_parent_category, product_brand
            FROM read_parquet('{dim_products}')
        """)

        conn.execute(f"""
            CREATE OR REPLACE VIEW dim_categories AS
            SELECT product_category, product_parent_category
            FROM read_parquet('{dim_categories}')
        """)

        conn.execute(f"""
            CREATE OR REPLACE VIEW dim_brands AS
            SELECT product_brand FROM read_parquet('{dim_brands}')
        """)

        logger.info("DuckDB views created successfully")


# Module-level connection getter
def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    return DuckDBManager().get_connection()


def query_sales_trends(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Query sales trends - optimized with single scan."""
    conn = get_duckdb_connection()

    trunc_map = {'daily': 'day', 'weekly': 'week', 'monthly': 'month'}
    if period not in trunc_map:
        raise ValueError("Period must be 'daily', 'weekly', or 'monthly'")

    query = f"""
    SELECT 
        date_trunc('{trunc_map[period]}', date) as date,
        SUM(revenue) as revenue,
        COALESCE(NULLIF(COUNT(DISTINCT order_id), 0), COUNT(*)) as transactions,
        SUM(revenue) / NULLIF(COALESCE(NULLIF(COUNT(DISTINCT order_id), 0), COUNT(*)), 0) as avg_transaction_value
    FROM fact_sales
    WHERE date >= ? AND date < ? + INTERVAL 1 DAY
    GROUP BY 1
    ORDER BY 1
    """
    return conn.execute(query, [start_date, end_date]).fetchdf()


def query_hourly_sales_pattern(target_date: date) -> pd.DataFrame:
    """Query hourly sales - pre-generates all hours in SQL."""
    conn = get_duckdb_connection()

    query = """
    WITH hours AS (SELECT unnest(range(7, 24)) as hour),
    sales AS (
        SELECT 
            EXTRACT(HOUR FROM date)::INT as hour,
            SUM(revenue) as revenue,
            COALESCE(NULLIF(COUNT(DISTINCT order_id), 0), COUNT(*)) as transactions
        FROM fact_sales
        WHERE date >= ? AND date < ? + INTERVAL 1 DAY
          AND EXTRACT(HOUR FROM date) BETWEEN 7 AND 23
        GROUP BY 1
    )
    SELECT h.hour, COALESCE(s.revenue, 0) as revenue, COALESCE(s.transactions, 0) as transactions
    FROM hours h LEFT JOIN sales s ON h.hour = s.hour
    ORDER BY h.hour
    """
    return conn.execute(query, [target_date, target_date]).fetchdf()


def query_top_products(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Query top products - single optimized query."""
    conn = get_duckdb_connection()

    query = """
    SELECT 
        'Product ' || p.product_id::VARCHAR as product_name,
        COALESCE(p.product_category, 'Unknown Category') as category,
        COALESCE(SUM(f.quantity), 0) as quantity_sold,
        COALESCE(SUM(f.revenue), 0) as total_unit_price
    FROM fact_sales f
    LEFT JOIN dim_products p ON f.product_id = p.product_id
    WHERE f.date >= ? AND f.date < ? + INTERVAL 1 DAY
    GROUP BY p.product_id, p.product_category
    ORDER BY total_unit_price DESC
    LIMIT ?
    """
    return conn.execute(query, [start_date, end_date, limit]).fetchdf()


def query_revenue_comparison(start_date: date, end_date: date) -> Dict:
    """Compare revenue - SINGLE query for both periods using FILTER."""
    conn = get_duckdb_connection()

    period_days = (end_date - start_date).days + 1
    prev_start = start_date - timedelta(days=period_days)
    prev_end = start_date - timedelta(days=1)

    # Combined query using FILTER - scans data only once
    query = """
    SELECT 
        SUM(revenue) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as cur_rev,
        COALESCE(NULLIF(COUNT(DISTINCT order_id) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY), 0),
                 COUNT(*) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY)) as cur_txn,
        SUM(revenue) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as prev_rev,
        COALESCE(NULLIF(COUNT(DISTINCT order_id) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY), 0),
                 COUNT(*) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY)) as prev_txn
    FROM fact_sales
    WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY
    """
    
    params = [
        start_date, end_date, start_date, end_date, start_date, end_date,  # current
        prev_start, prev_end, prev_start, prev_end, prev_start, prev_end,  # previous  
        prev_start, end_date  # overall filter
    ]

    row = conn.execute(query, params).fetchone()
    cur_rev, cur_txn, prev_rev, prev_txn = [v or 0 for v in row]

    cur_atv = cur_rev / cur_txn if cur_txn else 0
    prev_atv = prev_rev / prev_txn if prev_txn else 0

    def calc_delta(cur: float, prev: float) -> tuple:
        delta = cur - prev
        pct = (delta / prev * 100) if prev else 0
        return delta, pct

    rev_d, rev_p = calc_delta(cur_rev, prev_rev)
    txn_d, txn_p = calc_delta(cur_txn, prev_txn)
    atv_d, atv_p = calc_delta(cur_atv, prev_atv)

    return {
        'current': {'revenue': cur_rev, 'transactions': cur_txn, 'avg_transaction_value': cur_atv},
        'previous': {'revenue': prev_rev, 'transactions': prev_txn, 'avg_transaction_value': prev_atv},
        'deltas': {
            'revenue': rev_d, 'revenue_pct': rev_p,
            'transactions': txn_d, 'transactions_pct': txn_p,
            'avg_transaction_value': atv_d, 'avg_transaction_value_pct': atv_p
        }
    }


def query_hourly_sales_heatmap(start_date: date, end_date: date) -> pd.DataFrame:
    """Query hourly heatmap data."""
    conn = get_duckdb_connection()

    query = """
    SELECT
        date_trunc('day', date)::DATE as date,
        EXTRACT(HOUR FROM date)::INT as hour,
        SUM(revenue) as revenue
    FROM fact_sales
    WHERE date >= ? AND date < ? + INTERVAL 1 DAY
      AND EXTRACT(HOUR FROM date) BETWEEN 7 AND 23
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    return conn.execute(query, [start_date, end_date]).fetchdf()


def query_overview_summary(start_date: date, end_date: date) -> Dict:
    """Get overview summary - combined into fewer queries."""
    conn = get_duckdb_connection()

    # Single combined query for all aggregations
    query = """
    WITH base AS (
        SELECT f.revenue, f.quantity, 
               COALESCE(p.product_parent_category, 'Unknown') as parent_cat,
               COALESCE(p.product_category, 'Unknown') as cat,
               COALESCE(p.product_brand, 'Unknown') as brand
        FROM fact_sales f
        LEFT JOIN dim_products p ON f.product_id = p.product_id
        WHERE f.date >= ? AND f.date < ? + INTERVAL 1 DAY
    ),
    summary AS (SELECT SUM(revenue) as rev, SUM(quantity) as qty FROM base),
    by_cat AS (SELECT parent_cat, cat, SUM(revenue) as rev FROM base GROUP BY 1, 2),
    by_brand AS (SELECT parent_cat, cat, brand, SUM(revenue) as rev FROM base GROUP BY 1, 2, 3)
    SELECT 'summary' as type, NULL as c1, NULL as c2, NULL as c3, rev, qty FROM summary
    UNION ALL SELECT 'cat', parent_cat, cat, NULL, rev, NULL FROM by_cat
    UNION ALL SELECT 'brand', parent_cat, cat, brand, rev, NULL FROM by_brand
    """

    results = conn.execute(query, [start_date, end_date]).fetchall()

    categories_nested = {}
    brands_nested = {}
    total_rev = total_qty = 0

    for row in results:
        rtype, c1, c2, c3, rev, qty = row
        rev = float(rev or 0)
        
        if rtype == 'summary':
            total_rev, total_qty = rev, float(qty or 0)
        elif rtype == 'cat':
            categories_nested.setdefault(c1, {})[c2] = rev
        elif rtype == 'brand':
            brands_nested.setdefault(c1, {}).setdefault(c2, {})[c3] = rev

    return {
        'target_date_start': start_date,
        'target_date_end': end_date,
        'today_amount': total_rev,
        'today_qty': total_qty,
        'prev_amount': 0,
        'categories_nested': categories_nested,
        'brands_nested': brands_nested,
    }
