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
            f"{data_lake}/star-schema/fact_invoice_sales",
            f"{data_lake}/star-schema/fact_purchases",
            f"{data_lake}/star-schema/fact_inventory_moves",
            f"{data_lake}/star-schema/dim_products.parquet",
            f"{data_lake}/star-schema/dim_categories.parquet",
            f"{data_lake}/star-schema/dim_brands.parquet",
            f"{data_lake}/star-schema/dim_taxes.parquet",
        )

    def _setup_views(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Setup DuckDB views - fails fast on errors."""
        fact_path, fact_invoice_path, fact_purchases_path, fact_inventory_moves_path, dim_products, dim_categories, dim_brands, dim_taxes = self._get_data_paths()

        def _parquet_columns(parquet_path: str) -> set:
            try:
                rows = conn.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
                ).fetchall()
                # DESCRIBE returns rows like: (column_name, column_type, null, key, default, extra)
                return {r[0] for r in rows if r and r[0]}
            except Exception:
                return set()

        products_cols = _parquet_columns(dim_products)
        categories_cols = _parquet_columns(dim_categories)
        brands_cols = _parquet_columns(dim_brands)

        product_name_col = (
            "product_name" if "product_name" in products_cols
            else "name" if "name" in products_cols
            else None
        )
        product_category_col = (
            "product_category" if "product_category" in products_cols
            else "category_name" if "category_name" in products_cols
            else None
        )
        product_parent_category_col = (
            "product_parent_category" if "product_parent_category" in products_cols
            else "parent_category_name" if "parent_category_name" in products_cols
            else None
        )
        product_brand_col = (
            "product_brand" if "product_brand" in products_cols
            else "brand_name" if "brand_name" in products_cols
            else None
        )

        category_leaf_col = (
            "product_category" if "product_category" in categories_cols
            else "category_name" if "category_name" in categories_cols
            else None
        )
        category_parent_col = (
            "product_parent_category" if "product_parent_category" in categories_cols
            else "parent_category_name" if "parent_category_name" in categories_cols
            else None
        )

        brand_name_col = (
            "product_brand" if "product_brand" in brands_cols
            else "brand_name" if "brand_name" in brands_cols
            else None
        )

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
                COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue
            FROM read_parquet('{fact_path}/*.parquet', union_by_name=True, filename=true)
        """)

        conn.execute(f"""
            CREATE OR REPLACE VIEW fact_invoice_sales AS
            SELECT 
                TRY_CAST(date AS TIMESTAMP) AS date,
                COALESCE(TRY_CAST(move_id AS BIGINT), 0) AS move_id,
                COALESCE(move_name, '') AS move_name,
                COALESCE(TRY_CAST(customer_id AS BIGINT), 0) AS customer_id,
                COALESCE(customer_name, '') AS customer_name,
                COALESCE(TRY_CAST(move_line_id AS BIGINT), 0) AS move_line_id,
                COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                COALESCE(TRY_CAST(price_unit AS DOUBLE), 0) AS price_unit,
                COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                COALESCE(tax_ids_json, '[]') AS tax_ids_json,
                FALSE AS is_free_item
            FROM read_parquet('{fact_invoice_path}/*.parquet', union_by_name=True, filename=true)
        """)

        conn.execute(f"""
            CREATE OR REPLACE VIEW fact_purchases AS
            SELECT 
                TRY_CAST(date AS TIMESTAMP) AS date,
                COALESCE(TRY_CAST(move_id AS BIGINT), 0) AS move_id,
                COALESCE(move_name, '') AS move_name,
                COALESCE(TRY_CAST(vendor_id AS BIGINT), 0) AS vendor_id,
                COALESCE(vendor_name, '') AS vendor_name,
                TRY_CAST(purchase_order_id AS BIGINT) AS purchase_order_id,
                COALESCE(purchase_order_name, '') AS purchase_order_name,
                COALESCE(TRY_CAST(move_line_id AS BIGINT), 0) AS move_line_id,
                COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                COALESCE(TRY_CAST(price_unit AS DOUBLE), 0) AS price_unit,
                COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                TRY_CAST(tax_id AS BIGINT) AS tax_id,
                COALESCE(tax_name, '') AS tax_name,
                COALESCE(tax_ids_json, '[]') AS tax_ids_json,
                COALESCE(TRY_CAST(is_free_item AS BOOLEAN), FALSE) AS is_free_item
            FROM read_parquet('{fact_purchases_path}/*.parquet', union_by_name=True, filename=true)
        """)

        conn.execute(f"""
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
            FROM read_parquet('{fact_inventory_moves_path}/*.parquet', union_by_name=True, filename=true)
        """)

        conn.execute("""
            CREATE OR REPLACE VIEW fact_sales_all AS
            SELECT 
                date,
                order_id AS txn_id,
                line_id AS line_id,
                product_id,
                revenue,
                quantity
            FROM fact_sales
            UNION ALL
            SELECT 
                date,
                move_id AS txn_id,
                move_line_id AS line_id,
                product_id,
                price_unit * quantity AS revenue,
                quantity
            FROM fact_invoice_sales
        """)

        conn.execute(f"""
            CREATE OR REPLACE VIEW dim_products AS
            SELECT
                product_id,
                {f"COALESCE({product_name_col}, '')" if product_name_col else "''"} AS product_name,
                {product_category_col if product_category_col else "NULL"} AS product_category,
                {product_parent_category_col if product_parent_category_col else "NULL"} AS product_parent_category,
                {f"COALESCE({product_brand_col}, '')" if product_brand_col else "''"} AS product_brand
            FROM read_parquet('{dim_products}', union_by_name=True)
        """)

        conn.execute(f"""
            CREATE OR REPLACE VIEW dim_categories AS
            SELECT
                {category_leaf_col if category_leaf_col else "NULL"} AS product_category,
                {category_parent_col if category_parent_col else "NULL"} AS product_parent_category
            FROM read_parquet('{dim_categories}', union_by_name=True)
        """)

        conn.execute(f"""
            CREATE OR REPLACE VIEW dim_brands AS
            SELECT
                {f"COALESCE({brand_name_col}, '')" if brand_name_col else "''"} AS product_brand
            FROM read_parquet('{dim_brands}', union_by_name=True)
        """)

        if os.path.exists(dim_taxes):
            conn.execute(f"""
                CREATE OR REPLACE VIEW dim_taxes AS
                SELECT
                    COALESCE(TRY_CAST(tax_id AS BIGINT), 0) AS tax_id,
                    COALESCE(tax_name, '') AS tax_name
                FROM read_parquet('{dim_taxes}', union_by_name=True)
            """)
        else:
            conn.execute("""
                CREATE OR REPLACE VIEW dim_taxes AS
                SELECT
                    CAST(NULL AS BIGINT) AS tax_id,
                    CAST('' AS VARCHAR) AS tax_name
                WHERE FALSE
            """)

        logger.info("DuckDB views created successfully")


def query_sales_by_principal(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    conn = DuckDBManager().get_connection()
    _, _, _, _, dim_products, _, dim_brands, _ = DuckDBManager._get_data_paths()

    def _parquet_columns(parquet_path: str) -> set:
        try:
            rows = conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
            ).fetchall()
            return {r[0] for r in rows if r and r[0]}
        except Exception:
            return set()

    brands_cols = _parquet_columns(dim_brands)

    brand_name_col = (
        "brand_name" if "brand_name" in brands_cols
        else "product_brand" if "product_brand" in brands_cols
        else None
    )
    principal_name_col = (
        "principal_name" if "principal_name" in brands_cols
        else None
    )

    if brand_name_col and principal_name_col:
        principal_expr = f"COALESCE(NULLIF(TRIM(b.{principal_name_col}), ''), 'Unknown Principal')"
        brand_join = f"LOWER(TRIM(b.{brand_name_col})) = LOWER(TRIM(p.product_brand))"
    else:
        principal_expr = "'Unknown Principal'"
        brand_join = "FALSE"

    query = f"""
        WITH base AS (
            SELECT
                {principal_expr} AS principal,
                f.revenue AS revenue
            FROM fact_sales_all f
            LEFT JOIN dim_products p
                ON f.product_id = p.product_id
            LEFT JOIN read_parquet('{dim_brands}', union_by_name=True) b
                ON {brand_join}
            WHERE f.date >= ? AND f.date < ? + INTERVAL 1 DAY
        )
        SELECT
            principal,
            SUM(revenue) AS revenue
        FROM base
        GROUP BY 1
        ORDER BY revenue DESC
        LIMIT ?
    """

    try:
        return conn.execute(query, [start_date, end_date, int(limit)]).df()
    except Exception as exc:
        logger.exception("DuckDB query failed in query_sales_by_principal: %s", exc)
        return pd.DataFrame(columns=["principal", "revenue"])


# Module-level connection getter
def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    return DuckDBManager().get_connection()


def query_sales_trends(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Query sales trends - optimized with single scan."""
    conn = get_duckdb_connection()

    trunc_map = {'daily': 'day', 'weekly': 'week', 'monthly': 'month'}
    if period not in trunc_map:
        raise ValueError("Period must be 'daily', 'weekly', or 'monthly'")

    trunc_expr = trunc_map[period]

    query = f"""
    WITH date_series AS (
        SELECT date_trunc('{trunc_expr}', date) as period_start,
               date_trunc('{trunc_expr}', date) + interval '1 {trunc_expr}' as period_end
        FROM generate_series(
            date_trunc('{trunc_expr}', ?::date)::timestamp,
            date_trunc('{trunc_expr}', ?::date)::timestamp,
            interval '1 {trunc_expr}'
        ) as t(date)
    )
    SELECT 
        ds.period_start as date,
        COALESCE(SUM(fs.revenue), 0) as revenue,
        COALESCE(COUNT(DISTINCT fs.txn_id), 0) as transactions,
        COALESCE(SUM(fs.quantity), 0) as items_sold,
        CASE 
            WHEN COUNT(DISTINCT fs.txn_id) > 0 
            THEN SUM(fs.revenue) / COUNT(DISTINCT fs.txn_id) 
            ELSE 0 
        END as avg_transaction_value
    FROM date_series ds
    LEFT JOIN fact_sales_all fs ON 
        fs.date >= ds.period_start AND 
        fs.date < ds.period_end
    GROUP BY ds.period_start
    ORDER BY ds.period_start
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
            COALESCE(NULLIF(COUNT(DISTINCT txn_id), 0), COUNT(*)) as transactions
        FROM fact_sales_all
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
        COALESCE(p.product_name, 'Product ' || f.product_id::VARCHAR) as product_name,
        COALESCE(p.product_category, 'Unknown Category') as category,
        COALESCE(SUM(f.quantity), 0) as quantity_sold,
        COALESCE(SUM(f.revenue), 0) as total_unit_price
    FROM fact_sales_all f
    LEFT JOIN dim_products p ON f.product_id = p.product_id
    WHERE f.date >= ? AND f.date < ? + INTERVAL 1 DAY
    GROUP BY 1, 2
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
        -- Current period
        SUM(revenue) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as cur_rev,
        COALESCE(NULLIF(COUNT(DISTINCT txn_id) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY), 0),
                 COUNT(*) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY)) as cur_txn,
        SUM(quantity) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as cur_items,
        
        -- Previous period
        SUM(revenue) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as prev_rev,
        COALESCE(NULLIF(COUNT(DISTINCT txn_id) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY), 0),
                 COUNT(*) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY)) as prev_txn,
        SUM(quantity) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as prev_items
        
    FROM fact_sales_all
    WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY
    """
    
    params = [
        # Current period filters
        start_date, end_date,  # cur_rev
        start_date, end_date, start_date, end_date,  # cur_txn
        start_date, end_date,  # cur_items
        
        # Previous period filters
        prev_start, prev_end,  # prev_rev
        prev_start, prev_end, prev_start, prev_end,  # prev_txn
        prev_start, prev_end,  # prev_items
        
        # Overall filter
        prev_start, end_date
    ]

    row = conn.execute(query, params).fetchone()
    cur_rev, cur_txn, cur_items, prev_rev, prev_txn, prev_items = [v or 0 for v in row]

    # Calculate averages
    cur_atv = cur_rev / cur_txn if cur_txn else 0
    prev_atv = prev_rev / prev_txn if prev_txn else 0

    def calc_delta(cur: float, prev: float) -> tuple:
        delta = cur - prev
        pct = (delta / prev * 100) if prev else 0
        return delta, pct

    rev_d, rev_p = calc_delta(cur_rev, prev_rev)
    txn_d, txn_p = calc_delta(cur_txn, prev_txn)
    atv_d, atv_p = calc_delta(cur_atv, prev_atv)
    items_d, items_p = calc_delta(cur_items, prev_items)

    return {
        'current': {
            'revenue': cur_rev,
            'transactions': cur_txn,
            'items_sold': cur_items,
            'avg_transaction_value': cur_atv
        },
        'previous': {
            'revenue': prev_rev,
            'transactions': prev_txn,
            'items_sold': prev_items,
            'avg_transaction_value': prev_atv
        },
        'deltas': {
            'revenue': rev_d, 'revenue_pct': rev_p,
            'transactions': txn_d, 'transactions_pct': txn_p,
            'items_sold': items_d, 'items_sold_pct': items_p,
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
    FROM fact_sales_all
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
        FROM fact_sales_all f
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
