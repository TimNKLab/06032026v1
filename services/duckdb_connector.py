"""DuckDB in-memory connector for Dashboard queries.

Reads Parquet files directly via DuckDB :memory: connections.
No disk-backed DB, no singleton, no file-lock conflicts.
Every query gets a fresh connection with views pre-created.
Connections are automatically closed after each query via context manager.
"""
import os
import logging
import time
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Dict, Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

DATA_LAKE_ROOT = os.environ.get('DATA_LAKE_ROOT', '/data-lake')


# ---------------------------------------------------------------------------
# Connection Factory
# ---------------------------------------------------------------------------

def get_duckdb_connection():
    """Return a fresh in-memory DuckDB connection with views over Parquet."""
    conn = duckdb.connect(database=':memory:')
    conn.execute("SET threads = 4")
    conn.execute("SET memory_limit = '2GB'")
    _create_views(conn)
    return conn


@contextmanager
def _query_conn():
    """Context manager that provides a DuckDB connection and ensures it's closed."""
    conn = get_duckdb_connection()
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# View Creation
# ---------------------------------------------------------------------------

def _gp(sub):
    """Helper: build star-schema path for a given sub-directory/file."""
    return DATA_LAKE_ROOT + '/star-schema/' + sub


def _try_create_view(conn, name, sql):
    """Create a view; fallback to empty if Parquet is missing."""
    try:
        conn.execute("CREATE OR REPLACE VIEW " + name + " AS " + sql)
    except Exception as exc:
        logger.warning("[duckdb] view %s create failed, using fallback: %s", name, exc)
        conn.execute("CREATE OR REPLACE VIEW " + name + " AS SELECT 1 AS dummy WHERE FALSE")


def _load_dimension_tables(conn):
    """Load small dimension parquet files into DuckDB tables for fast joins.

    Tables (not views) give DuckDB column statistics and avoid repeated
    filesystem scans on every query.
    """
    dp = DATA_LAKE_ROOT + '/star-schema/'

    for name in ('dim_products', 'dim_categories', 'dim_brands', 'dim_taxes'):
        path = dp + name + '.parquet'
        if os.path.exists(path):
            conn.execute(
                "CREATE TABLE " + name +
                " AS SELECT * FROM read_parquet('" + path + "', union_by_name=True)"
            )
        else:
            conn.execute("CREATE TABLE " + name + " AS SELECT 1 AS dummy WHERE FALSE")


def _create_views(conn):
    """Create all DuckDB views over Parquet files on a fresh connection."""
    t0 = time.time()
    _load_dimension_tables(conn)

    # --- Fact views ---
    _try_create_view(conn, "fact_sales",
        "SELECT"
        " TRY_CAST(date AS DATE) AS date,"
        " COALESCE(TRY_CAST(order_id AS BIGINT), 0) AS order_id,"
        " COALESCE(order_ref, '') AS order_ref,"
        " COALESCE(TRY_CAST(pos_config_id AS BIGINT), 0) AS pos_config_id,"
        " COALESCE(TRY_CAST(cashier_id AS BIGINT), 0) AS cashier_id,"
        " COALESCE(TRY_CAST(customer_id AS BIGINT), 0) AS customer_id,"
        " COALESCE(payment_method_ids, '') AS payment_method_ids,"
        " COALESCE(TRY_CAST(line_id AS BIGINT), 0) AS line_id,"
        " COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,"
        " COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,"
        " COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue"
        " FROM read_parquet('" + _gp('fact_sales') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)")

    _try_create_view(conn, "fact_invoice_sales",
        "SELECT"
        " TRY_CAST(date AS TIMESTAMP) AS date,"
        " COALESCE(TRY_CAST(move_id AS BIGINT), 0) AS move_id,"
        " COALESCE(move_name, '') AS move_name,"
        " COALESCE(TRY_CAST(customer_id AS BIGINT), 0) AS customer_id,"
        " COALESCE(customer_name, '') AS customer_name,"
        " COALESCE(TRY_CAST(move_line_id AS BIGINT), 0) AS move_line_id,"
        " COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,"
        " COALESCE(TRY_CAST(price_unit AS DOUBLE), 0) AS price_unit,"
        " COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,"
        " COALESCE(TRY_CAST(tax_id AS BIGINT), 0) AS tax_id,"
        " COALESCE(tax_ids_json, '[]') AS tax_ids_json,"
        " FALSE AS is_free_item"
        " FROM read_parquet('" + _gp('fact_invoice_sales') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)")

    _try_create_view(conn, "fact_purchases",
        "SELECT"
        " TRY_CAST(date AS TIMESTAMP) AS date,"
        " COALESCE(TRY_CAST(move_id AS BIGINT), 0) AS move_id,"
        " COALESCE(move_name, '') AS move_name,"
        " COALESCE(TRY_CAST(vendor_id AS BIGINT), 0) AS vendor_id,"
        " COALESCE(vendor_name, '') AS vendor_name,"
        " TRY_CAST(purchase_order_id AS BIGINT) AS purchase_order_id,"
        " COALESCE(purchase_order_name, '') AS purchase_order_name,"
        " COALESCE(TRY_CAST(move_line_id AS BIGINT), 0) AS move_line_id,"
        " COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,"
        " COALESCE(TRY_CAST(price_unit AS DOUBLE), 0) AS price_unit,"
        " COALESCE(TRY_CAST(actual_price AS DOUBLE), 0) AS actual_price,"
        " COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,"
        " TRY_CAST(tax_id AS BIGINT) AS tax_id,"
        " COALESCE(tax_name, '') AS tax_name,"
        " COALESCE(tax_ids_json, '[]') AS tax_ids_json,"
        " COALESCE(TRY_CAST(is_free_item AS BOOLEAN), FALSE) AS is_free_item"
        " FROM read_parquet('" + _gp('fact_purchases') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)")

    _try_create_view(conn, "fact_inventory_moves",
        "SELECT"
        " TRY_CAST(date AS TIMESTAMP) AS movement_date,"
        " COALESCE(TRY_CAST(move_id AS BIGINT), 0) AS move_id,"
        " TRY_CAST(move_line_id AS BIGINT) AS move_line_id,"
        " TRY_CAST(product_id AS BIGINT) AS product_id,"
        " COALESCE(product_name, '') AS product_name,"
        " COALESCE(product_brand, '') AS product_brand,"
        " TRY_CAST(location_src_id AS BIGINT) AS location_src_id,"
        " COALESCE(location_src_name, '') AS location_src_name,"
        " TRY_CAST(location_dest_id AS BIGINT) AS location_dest_id,"
        " COALESCE(location_dest_name, '') AS location_dest_name,"
        " COALESCE(TRY_CAST(qty_moved AS DOUBLE), 0) AS qty_moved,"
        " TRY_CAST(uom_id AS BIGINT) AS uom_id,"
        " COALESCE(uom_name, '') AS uom_name,"
        " COALESCE(uom_category, '') AS uom_category,"
        " COALESCE(movement_type, '') AS movement_type,"
        " COALESCE(TRY_CAST(inventory_adjustment_flag AS BOOLEAN), FALSE) AS inventory_adjustment_flag,"
        " TRY_CAST(manufacturing_order_id AS BIGINT) AS manufacturing_order_id,"
        " TRY_CAST(picking_id AS BIGINT) AS picking_id,"
        " COALESCE(picking_type_code, '') AS picking_type_code,"
        " COALESCE(reference, '') AS reference,"
        " COALESCE(origin_reference, '') AS origin_reference,"
        " TRY_CAST(source_partner_id AS BIGINT) AS source_partner_id,"
        " COALESCE(source_partner_name, '') AS source_partner_name,"
        " TRY_CAST(destination_partner_id AS BIGINT) AS destination_partner_id,"
        " COALESCE(destination_partner_name, '') AS destination_partner_name,"
        " TRY_CAST(created_by_user AS BIGINT) AS created_by_user,"
        " TRY_CAST(create_date AS TIMESTAMP) AS create_date"
        " FROM read_parquet('" + _gp('fact_inventory_moves') + "/*.parquet',"
        " union_by_name=True, filename=true)")

    _try_create_view(conn, "fact_stock_on_hand_snapshot",
        "SELECT"
        " TRY_CAST(snapshot_date AS DATE) AS snapshot_date,"
        " COALESCE(TRY_CAST(quant_id AS BIGINT), 0) AS quant_id,"
        " COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,"
        " TRY_CAST(location_id AS BIGINT) AS location_id,"
        " TRY_CAST(lot_id AS BIGINT) AS lot_id,"
        " TRY_CAST(owner_id AS BIGINT) AS owner_id,"
        " TRY_CAST(company_id AS BIGINT) AS company_id,"
        " COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,"
        " COALESCE(TRY_CAST(reserved_quantity AS DOUBLE), 0) AS reserved_quantity"
        " FROM read_parquet('" + _gp('fact_stock_on_hand_snapshot') + "/*.parquet',"
        " union_by_name=True, filename=true)")

    # --- Cost / profit views ---
    _try_create_view(conn, "fact_product_cost_events",
        "SELECT"
        " TRY_CAST(date AS DATE) AS date,"
        " COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,"
        " COALESCE(TRY_CAST(cost_unit_tax_in AS DOUBLE), 0) AS cost_unit_tax_in,"
        " COALESCE(TRY_CAST(source_move_id AS BIGINT), 0) AS source_move_id,"
        " COALESCE(TRY_CAST(source_tax_id AS BIGINT), 0) AS source_tax_id"
        " FROM read_parquet('" + _gp('fact_product_cost_events') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)")

    _try_create_view(conn, "fact_product_cost_latest_daily",
        "SELECT"
        " TRY_CAST(date AS DATE) AS date,"
        " COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,"
        " COALESCE(TRY_CAST(cost_unit_tax_in AS DOUBLE), 0) AS cost_unit_tax_in,"
        " COALESCE(TRY_CAST(source_move_id AS BIGINT), 0) AS source_move_id,"
        " COALESCE(TRY_CAST(source_tax_id AS BIGINT), 0) AS source_tax_id"
        " FROM read_parquet('" + _gp('fact_product_cost_latest_daily') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)")

    _try_create_view(conn, "fact_product_beginning_costs",
        "SELECT"
        " COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,"
        " COALESCE(TRY_CAST(cost_unit_tax_in AS DOUBLE), 0) AS cost_unit_tax_in,"
        " COALESCE(TRY_CAST(source_tax_id AS BIGINT), 0) AS source_tax_id,"
        " COALESCE(TRY_CAST(effective_date AS DATE), DATE '2025-02-10') AS effective_date,"
        " COALESCE(TRY_CAST(is_active AS BOOLEAN), TRUE) AS is_active,"
        " COALESCE(notes, '') AS notes"
        " FROM read_parquet('" + _gp('fact_product_beginning_costs') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)"
        " WHERE COALESCE(TRY_CAST(is_active AS BOOLEAN), TRUE) = TRUE")

    _try_create_view(conn, "fact_product_legacy_costs",
        "SELECT"
        " COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,"
        " COALESCE(TRY_CAST(cost_unit_tax_in AS DOUBLE), 0) AS cost_unit_tax_in,"
        " COALESCE(TRY_CAST(source_tax_id AS BIGINT), 0) AS source_tax_id,"
        " COALESCE(TRY_CAST(effective_date AS DATE), DATE '2025-02-10') AS effective_date,"
        " COALESCE(TRY_CAST(cost_source AS VARCHAR), 'unknown') AS cost_source,"
        " COALESCE(TRY_CAST(priority AS INTEGER), 3) AS priority,"
        " COALESCE(TRY_CAST(is_active AS BOOLEAN), TRUE) AS is_active,"
        " COALESCE(TRY_CAST(created_at AS TIMESTAMP), TIMESTAMP '2025-02-10 00:00:00') AS created_at,"
        " COALESCE(notes, '') AS notes"
        " FROM read_parquet('" + _gp('fact_product_legacy_costs') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)"
        " WHERE COALESCE(TRY_CAST(is_active AS BOOLEAN), TRUE) = TRUE")

    _try_create_view(conn, "fact_product_costs_unified",
        "WITH latest_costs AS ("
        " SELECT product_id, cost_unit_tax_in, source_tax_id,"
        " 'latest_purchase' AS cost_source, 1 AS priority, date AS effective_date"
        " FROM read_parquet('" + _gp('fact_product_cost_latest_daily') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)"
        " WHERE cost_unit_tax_in > 0"
        "),"
        " legacy_costs AS ("
        " SELECT product_id, cost_unit_tax_in, source_tax_id, cost_source, priority, effective_date"
        " FROM fact_product_legacy_costs"
        " WHERE is_active = TRUE AND cost_unit_tax_in > 0"
        "),"
        " all_costs AS (SELECT * FROM latest_costs UNION ALL SELECT * FROM legacy_costs),"
        " ranked_costs AS ("
        " SELECT product_id, cost_unit_tax_in, source_tax_id, cost_source, effective_date,"
        " ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY priority ASC, effective_date DESC) AS rn"
        " FROM all_costs"
        ")"
        " SELECT product_id, cost_unit_tax_in, source_tax_id, cost_source, effective_date"
        " FROM ranked_costs WHERE rn = 1")

    _try_create_view(conn, "fact_sales_lines_profit",
        "SELECT"
        " TRY_CAST(date AS DATE) AS date,"
        " COALESCE(TRY_CAST(txn_id AS BIGINT), 0) AS txn_id,"
        " COALESCE(TRY_CAST(line_id AS BIGINT), 0) AS line_id,"
        " COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,"
        " COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,"
        " COALESCE(TRY_CAST(revenue_tax_in AS DOUBLE), 0) AS revenue_tax_in,"
        " COALESCE(TRY_CAST(cost_unit_tax_in AS DOUBLE), 0) AS cost_unit_tax_in,"
        " COALESCE(TRY_CAST(cogs_tax_in AS DOUBLE), 0) AS cogs_tax_in,"
        " COALESCE(TRY_CAST(gross_profit AS DOUBLE), 0) AS gross_profit,"
        " COALESCE(TRY_CAST(source_cost_move_id AS BIGINT), 0) AS source_cost_move_id,"
        " COALESCE(TRY_CAST(source_cost_tax_id AS BIGINT), 0) AS source_cost_tax_id"
        " FROM read_parquet('" + _gp('fact_sales_lines_profit') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)")

    # --- Aggregate views (fast path) ---
    _try_create_view(conn, "agg_profit_daily",
        "SELECT"
        " COALESCE(TRY_CAST(date AS DATE),"
        " MAKE_DATE(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER))) AS date,"
        " COALESCE(TRY_CAST(revenue_tax_in AS DOUBLE), 0) AS revenue_tax_in,"
        " COALESCE(TRY_CAST(cogs_tax_in AS DOUBLE), 0) AS cogs_tax_in,"
        " COALESCE(TRY_CAST(gross_profit AS DOUBLE), 0) AS gross_profit,"
        " COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,"
        " COALESCE(TRY_CAST(transactions AS BIGINT), 0) AS transactions,"
        " COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines"
        " FROM read_parquet('" + _gp('agg_profit_daily') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)")

    _try_create_view(conn, "agg_profit_daily_by_product",
        "SELECT"
        " COALESCE(TRY_CAST(date AS DATE),"
        " MAKE_DATE(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER))) AS date,"
        " COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,"
        " COALESCE(TRY_CAST(revenue_tax_in AS DOUBLE), 0) AS revenue_tax_in,"
        " COALESCE(TRY_CAST(cogs_tax_in AS DOUBLE), 0) AS cogs_tax_in,"
        " COALESCE(TRY_CAST(gross_profit AS DOUBLE), 0) AS gross_profit,"
        " COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,"
        " COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines"
        " FROM read_parquet('" + _gp('agg_profit_daily_by_product') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)")

    _try_create_view(conn, "agg_sales_daily",
        "SELECT"
        " COALESCE(TRY_CAST(date AS DATE),"
        " MAKE_DATE(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER))) AS date,"
        " COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,"
        " COALESCE(TRY_CAST(transactions AS BIGINT), 0) AS transactions,"
        " COALESCE(TRY_CAST(items_sold AS DOUBLE), 0) AS items_sold,"
        " COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines"
        " FROM read_parquet('" + _gp('agg_sales_daily') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)")

    _try_create_view(conn, "agg_sales_daily_by_product",
        "SELECT"
        " COALESCE(TRY_CAST(date AS DATE),"
        " MAKE_DATE(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER))) AS date,"
        " COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,"
        " COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,"
        " COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,"
        " COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines"
        " FROM read_parquet('" + _gp('agg_sales_daily_by_product') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)")

    _try_create_view(conn, "agg_sales_daily_by_principal",
        "SELECT"
        " COALESCE(TRY_CAST(date AS DATE),"
        " MAKE_DATE(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER))) AS date,"
        " COALESCE(principal, 'Unknown') AS principal,"
        " COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,"
        " COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,"
        " COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines"
        " FROM read_parquet('" + _gp('agg_sales_daily_by_principal') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)")

    # --- Combined sales view (POS + invoice, excluding cancelled) ---
    _try_create_view(conn, "fact_sales_all",
        "SELECT"
        " MAKE_DATE(year, CAST(month AS INTEGER), CAST(day AS INTEGER)) AS date,"
        " order_id AS txn_id, line_id AS line_id, product_id,"
        " revenue, quantity, year, month, day, order_ref"
        " FROM read_parquet('" + _gp('fact_sales') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)"
        " WHERE COALESCE(order_ref, '') != '/' OR order_ref IS NULL"
        " UNION ALL"
        " SELECT"
        " MAKE_DATE(year, CAST(month AS INTEGER), CAST(day AS INTEGER)) AS date,"
        " move_id AS txn_id, move_line_id AS line_id, product_id,"
        " price_unit * quantity AS revenue, quantity, year, month, day, move_name AS order_ref"
        " FROM read_parquet('" + _gp('fact_invoice_sales') + "/*.parquet',"
        " union_by_name=True, hive_partitioning=1, filename=true)"
        " WHERE COALESCE(move_name, '') != '/' OR move_name IS NULL")

    logger.info("[duckdb] all views created in %.3fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Query Functions (Dashboard-facing API)
# ---------------------------------------------------------------------------

def query_sales_by_principal(start_date, end_date, limit=20):
    with _query_conn() as conn:
        q = ("SELECT principal, SUM(revenue) AS revenue"
             " FROM agg_sales_daily_by_principal"
             " WHERE date >= ? AND date < ? + INTERVAL 1 DAY"
             " GROUP BY principal ORDER BY revenue DESC LIMIT ?")
        t0 = time.time()
        try:
            result = conn.execute(q, [start_date, end_date, int(limit)]).fetchdf()
            logger.info("[TIMING] query_sales_by_principal: %.3fs", time.time() - t0)
            return result
        except Exception as exc:
            logger.error("query_sales_by_principal failed: %s", exc)
            return pd.DataFrame(columns=["principal", "revenue"])


def query_sales_trends(start_date, end_date, period='daily'):
    with _query_conn() as conn:
        trunc_map = {'daily': 'day', 'weekly': 'week', 'monthly': 'month'}
        if period not in trunc_map:
            raise ValueError("Period must be 'daily', 'weekly', or 'monthly'")
        te = trunc_map[period]

        q = ("WITH date_series AS ("
             " SELECT date_trunc('" + te + "', date) AS period_start,"
             " date_trunc('" + te + "', date) + INTERVAL '1 " + te + "' AS period_end"
             " FROM generate_series("
             " date_trunc('" + te + "', ?::DATE)::TIMESTAMP,"
             " date_trunc('" + te + "', ?::DATE)::TIMESTAMP,"
             " INTERVAL '1 " + te + "'"
             " ) AS t(date)"
             ")"
             " SELECT ds.period_start AS date,"
             " COALESCE(SUM(a.revenue), 0) AS revenue,"
             " COALESCE(SUM(a.transactions), 0) AS transactions,"
             " COALESCE(SUM(a.items_sold), 0) AS items_sold,"
             " CASE WHEN SUM(a.transactions) > 0 THEN SUM(a.revenue)/SUM(a.transactions) ELSE 0 END"
             " AS avg_transaction_value"
             " FROM date_series ds"
             " LEFT JOIN agg_sales_daily a ON a.date >= ds.period_start AND a.date < ds.period_end"
             " GROUP BY ds.period_start ORDER BY ds.period_start")

        t0 = time.time()
        result = conn.execute(q, [start_date, end_date]).fetchdf()
        logger.info("[TIMING] query_sales_trends: %.3fs", time.time() - t0)
        return result


def query_hourly_sales_pattern(target_date):
    with _query_conn() as conn:
        q = ("WITH hours AS (SELECT UNNEST(RANGE(7, 24)) AS hour),"
             " sales AS ("
             " SELECT EXTRACT(HOUR FROM date)::INT AS hour,"
             " SUM(revenue) AS revenue,"
             " COALESCE(NULLIF(COUNT(DISTINCT txn_id), 0), COUNT(*)) AS transactions"
             " FROM fact_sales_all"
             " WHERE date >= ? AND date < ? + INTERVAL 1 DAY"
             " AND EXTRACT(HOUR FROM date) BETWEEN 7 AND 23"
             " GROUP BY 1"
             " )"
             " SELECT h.hour, COALESCE(s.revenue, 0) AS revenue, COALESCE(s.transactions, 0) AS transactions"
             " FROM hours h LEFT JOIN sales s ON h.hour = s.hour"
             " ORDER BY h.hour")
        t0 = time.time()
        result = conn.execute(q, [target_date, target_date]).fetchdf()
        logger.info("[TIMING] query_hourly_sales_pattern: %.3fs", time.time() - t0)
        return result


def query_top_products(start_date, end_date, limit=20):
    with _query_conn() as conn:
        q = ("WITH product_agg AS ("
             " SELECT product_id, SUM(quantity) AS quantity_sold, SUM(revenue) AS total_unit_price"
             " FROM agg_sales_daily_by_product"
             " WHERE date >= ? AND date < ? + INTERVAL 1 DAY"
             " GROUP BY product_id ORDER BY total_unit_price DESC LIMIT ?"
             ")"
             " SELECT"
             " COALESCE(p.product_name, 'Product ' || s.product_id::VARCHAR) AS product_name,"
             " COALESCE(p.product_category, 'Unknown Category') AS category,"
             " s.quantity_sold, s.total_unit_price"
             " FROM product_agg s LEFT JOIN dim_products p ON s.product_id = p.product_id"
             " ORDER BY s.total_unit_price DESC")
        t0 = time.time()
        result = conn.execute(q, [start_date, end_date, limit]).fetchdf()
        logger.info("[TIMING] query_top_products: %.3fs", time.time() - t0)
        return result


def query_revenue_comparison(start_date, end_date):
    with _query_conn() as conn:
        period_days = (end_date - start_date).days + 1
        prev_start = start_date - timedelta(days=period_days)
        prev_end = start_date - timedelta(days=1)

        q = ("SELECT"
             " SUM(revenue) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) AS curr_rev,"
             " SUM(transactions) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) AS curr_txn,"
             " SUM(items_sold) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) AS curr_items,"
             " SUM(revenue) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) AS prev_rev,"
             " SUM(transactions) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) AS prev_txn,"
             " SUM(items_sold) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) AS prev_items"
             " FROM agg_sales_daily"
             " WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY")

        params = [
            start_date, end_date, start_date, end_date, start_date, end_date,
            prev_start, prev_end, prev_start, prev_end, prev_start, prev_end,
            prev_start, end_date
        ]
        t0 = time.time()
        row = conn.execute(q, params).fetchone()
        logger.info("[TIMING] query_revenue_comparison: %.3fs", time.time() - t0)
        cur_rev, cur_txn, cur_items, prev_rev, prev_txn, prev_items = [v or 0 for v in row]
        cur_atv = cur_rev / cur_txn if cur_txn else 0
        prev_atv = prev_rev / prev_txn if prev_txn else 0

        def _d(c, p):
            delta = c - p
            pct = (delta / p * 100) if p else 0
            return delta, pct

        return {
            'current': {'revenue': cur_rev, 'transactions': cur_txn,
                        'items_sold': cur_items, 'avg_transaction_value': cur_atv},
            'previous': {'revenue': prev_rev, 'transactions': prev_txn,
                         'items_sold': prev_items, 'avg_transaction_value': prev_atv},
            'deltas': {
                'revenue': _d(cur_rev, prev_rev),
                'revenue_pct': _d(cur_rev, prev_rev)[1],
                'transactions': _d(cur_txn, prev_txn),
                'transactions_pct': _d(cur_txn, prev_txn)[1],
                'items_sold': _d(cur_items, prev_items),
                'items_sold_pct': _d(cur_items, prev_items)[1],
                'avg_transaction_value': _d(cur_atv, prev_atv),
                'avg_transaction_value_pct': _d(cur_atv, prev_atv)[1],
            },
        }


def query_hourly_sales_heatmap(start_date, end_date):
    with _query_conn() as conn:
        q = ("SELECT date_trunc('day', date)::DATE AS date, EXTRACT(HOUR FROM date)::INT AS hour,"
             " SUM(revenue) AS revenue"
             " FROM fact_sales_all"
             " WHERE date >= ? AND date < ? + INTERVAL 1 DAY"
             " AND EXTRACT(HOUR FROM date) BETWEEN 7 AND 23"
             " GROUP BY 1, 2 ORDER BY 1, 2")
        t0 = time.time()
        result = conn.execute(q, [start_date, end_date]).fetchdf()
        logger.info("[TIMING] query_hourly_sales_heatmap: %.3fs", time.time() - t0)
        return result


def query_overview_summary(start_date, end_date):
    with _query_conn() as conn:
        q = ("WITH base AS ("
             " SELECT a.product_id, a.revenue, a.quantity,"
             " COALESCE(p.product_parent_category, 'Unknown') AS parent_cat,"
             " COALESCE(p.product_category, 'Unknown') AS cat,"
             " COALESCE(p.product_brand, 'Unknown') AS brand"
             " FROM agg_sales_daily_by_product a"
             " LEFT JOIN dim_products p ON a.product_id = p.product_id"
             " WHERE a.date >= ? AND a.date < ? + INTERVAL 1 DAY"
             "),"
             " summary AS (SELECT SUM(revenue) AS rev, SUM(quantity) AS qty FROM base),"
             " by_cat AS (SELECT parent_cat, cat, SUM(revenue) AS rev FROM base GROUP BY 1, 2),"
             " by_brand AS (SELECT parent_cat, cat, brand, SUM(revenue) AS rev FROM base GROUP BY 1, 2, 3)"
             " SELECT 'summary' AS type, NULL AS c1, NULL AS c2, NULL AS c3, rev, qty FROM summary"
             " UNION ALL SELECT 'cat', parent_cat, cat, NULL, rev, NULL FROM by_cat"
             " UNION ALL SELECT 'brand', parent_cat, cat, brand, rev, NULL FROM by_brand")
        t0 = time.time()
        rows = conn.execute(q, [start_date, end_date]).fetchall()
        logger.info("[TIMING] query_overview_summary: %.3fs", time.time() - t0)

        cat_nested = {}
        brand_nested = {}
        total_rev = 0.0
        total_qty = 0.0
        for rtype, c1, c2, c3, rev, qty in rows:
            rev = float(rev or 0)
            if rtype == 'summary':
                total_rev, total_qty = rev, float(qty or 0)
            elif rtype == 'cat':
                cat_nested.setdefault(c1, {})[c2] = rev
            elif rtype == 'brand':
                brand_nested.setdefault(c1, {}).setdefault(c2, {})[c3] = rev

        return {
            'target_date_start': start_date, 'target_date_end': end_date,
            'today_amount': total_rev, 'today_qty': total_qty,
            'prev_amount': 0.0,
            'categories_nested': cat_nested, 'brands_nested': brand_nested,
        }


def query_profit_summary(start_date, end_date):
    with _query_conn() as conn:
        q = ("SELECT"
             " SUM(revenue_tax_in) AS revenue,"
             " SUM(cogs_tax_in) AS cogs,"
             " SUM(gross_profit) AS gross_profit,"
             " SUM(quantity) AS quantity,"
             " SUM(transactions) AS transactions,"
             " SUM(lines) AS lines,"
             " CASE WHEN SUM(transactions) > 0 THEN SUM(revenue_tax_in)/SUM(transactions) ELSE 0 END"
             " AS avg_transaction_value,"
             " CASE WHEN SUM(revenue_tax_in) > 0 THEN SUM(gross_profit)/SUM(revenue_tax_in)*100 ELSE 0 END"
             " AS gross_margin_pct"
             " FROM agg_profit_daily"
             " WHERE date >= ? AND date < ? + INTERVAL 1 DAY")
        t0 = time.time()
        row = conn.execute(q, [start_date, end_date]).fetchone()
        logger.info("[TIMING] query_profit_summary: %.3fs", time.time() - t0)
        revenue, cogs, gross_profit, quantity, transactions, lines, atv, margin_pct = [v or 0 for v in row]
        return {
            'revenue': float(revenue), 'cogs': float(cogs),
            'gross_profit': float(gross_profit), 'quantity': float(quantity),
            'transactions': int(transactions), 'lines': int(lines),
            'avg_transaction_value': float(atv), 'gross_margin_pct': float(margin_pct),
        }


def query_profit_trends(start_date, end_date, period='daily'):
    with _query_conn() as conn:
        te = 'day' if period == 'daily' else 'week' if period == 'weekly' else 'month'
        q = ("WITH date_series AS ("
             " SELECT date_trunc('" + te + "', date) + INTERVAL '1 " + te + "' AS period_end"
             " FROM generate_series("
             " date_trunc('" + te + "', ?::DATE)::TIMESTAMP,"
             " date_trunc('" + te + "', ?::DATE)::TIMESTAMP,"
             " INTERVAL '1 " + te + "'"
             " ) AS t(date)"
             ")"
             " SELECT"
             " ds.period_end AS date,"
             " COALESCE(SUM(a.revenue_tax_in), 0) AS revenue,"
             " COALESCE(SUM(a.cogs_tax_in), 0) AS cogs,"
             " COALESCE(SUM(a.gross_profit), 0) AS gross_profit,"
             " COALESCE(SUM(a.quantity), 0) AS items_sold,"
             " COALESCE(SUM(a.transactions), 0) AS transactions,"
             " COALESCE(SUM(a.lines), 0) AS lines,"
             " CASE WHEN SUM(a.transactions) > 0 THEN SUM(a.revenue_tax_in)/SUM(a.transactions) ELSE 0 END"
             " AS avg_transaction_value,"
             " CASE WHEN SUM(a.revenue_tax_in) > 0 THEN SUM(a.gross_profit)/SUM(a.revenue_tax_in)*100 ELSE 0 END"
             " AS gross_margin_pct"
             " FROM date_series ds"
             " LEFT JOIN agg_profit_daily a"
             " ON a.date >= ds.period_end - INTERVAL '1 " + te + "' AND a.date < ds.period_end"
             " GROUP BY ds.period_end ORDER BY ds.period_end")
        t0 = time.time()
        result = conn.execute(q, [start_date, end_date]).fetchdf()
        logger.info("[TIMING] query_profit_trends: %.3fs", time.time() - t0)
        return result


def query_profit_by_product(start_date, end_date, limit=20):
    with _query_conn() as conn:
        q = ("WITH product_agg AS ("
             " SELECT product_id,"
             " SUM(revenue_tax_in) AS total_revenue, SUM(cogs_tax_in) AS total_cogs,"
             " SUM(gross_profit) AS total_profit, SUM(quantity) AS total_quantity,"
             " SUM(lines) AS total_lines"
             " FROM agg_profit_daily_by_product"
             " WHERE date >= ? AND date < ? + INTERVAL 1 DAY"
             " GROUP BY product_id ORDER BY total_profit DESC LIMIT ?"
             ")"
             " SELECT s.product_id,"
             " COALESCE(p.product_name, 'Product ' || s.product_id::VARCHAR) AS product_name,"
             " COALESCE(p.product_category, 'Unknown Category') AS category,"
             " s.total_revenue, s.total_cogs, s.total_profit,"
             " s.total_quantity, s.total_lines,"
             " CASE WHEN s.total_revenue > 0 THEN s.total_profit/s.total_revenue*100 ELSE 0 END"
             " AS profit_margin_pct"
             " FROM product_agg s LEFT JOIN dim_products p ON s.product_id = p.product_id"
             " ORDER BY s.total_profit DESC")
        t0 = time.time()
        result = conn.execute(q, [start_date, end_date, limit]).fetchdf()
        logger.info("[TIMING] query_profit_by_product: %.3fs", time.time() - t0)
        return result


def query_profit_drilldown(start_date, end_date, product_id=None):
    with _query_conn() as conn:
        if product_id:
            w = "WHERE date >= ? AND date < date(?, '+1 day') AND product_id = ?"
            params = [start_date, end_date, product_id]
        else:
            w = "WHERE date >= ? AND date < date(?, '+1 day')"
            params = [start_date, end_date]
        q = ("SELECT date, txn_id, line_id, product_id, quantity,"
             " revenue_tax_in, cost_unit_tax_in, cogs_tax_in, gross_profit,"
             " CASE WHEN revenue_tax_in > 0 THEN gross_profit*100.0/revenue_tax_in ELSE 0 END"
             " AS profit_margin_pct"
             " FROM fact_sales_lines_profit " + w +
             " ORDER BY date, gross_profit DESC")
        t0 = time.time()
        result = conn.execute(q, params).fetchdf()
        logger.info("[TIMING] query_profit_drilldown: %.3fs", time.time() - t0)
        return result


def query_profit_revenue_by_category(start_date, end_date):
    with _query_conn() as conn:
        q = ("WITH product_agg AS ("
             " SELECT a.product_id, SUM(a.revenue_tax_in) AS revenue_tax_in"
             " FROM agg_profit_daily_by_product a"
             " WHERE a.date >= ? AND a.date < ? + INTERVAL 1 DAY"
             " GROUP BY a.product_id"
             ")"
             " SELECT COALESCE(p.product_parent_category, 'Unknown') AS parent_cat,"
             " COALESCE(p.product_category, 'Unknown') AS cat,"
             " SUM(s.revenue_tax_in) AS revenue"
             " FROM product_agg s LEFT JOIN dim_products p ON s.product_id = p.product_id"
             " GROUP BY parent_cat, cat ORDER BY revenue DESC")
        t0 = time.time()
        rows = conn.execute(q, [start_date, end_date]).fetchall()
        logger.info("[TIMING] query_profit_revenue_by_category: %.3fs", time.time() - t0)
        result = {}
        for parent_cat, cat, revenue in rows:
            result.setdefault(str(parent_cat), {})[str(cat)] = float(revenue or 0)
        return result


def query_inventory_snapshot(snapshot_date):
    with _query_conn() as conn:
        path = DATA_LAKE_ROOT + '/star-schema/fact_stock_on_hand_snapshot/*.parquet'
        q = ("SELECT product_id, SUM(quantity) AS qty_on_hand"
             " FROM read_parquet('" + path + "', hive_partitioning=1)"
             " WHERE snapshot_date = ?"
             " GROUP BY product_id LIMIT 5000")
        t0 = time.time()
        df = conn.execute(q, [snapshot_date]).fetchdf()
        logger.info("[TIMING] query_inventory_snapshot: %.3fs", time.time() - t0)
        return df


def query_sales_by_product_duckdb(start_date, end_date):
    with _query_conn() as conn:
        path = DATA_LAKE_ROOT + '/star-schema/agg_sales_daily_by_product/*.parquet'
        q = ("SELECT product_id, SUM(quantity) AS units_sold, SUM(revenue) AS revenue"
             " FROM read_parquet('" + path + "', hive_partitioning=1)"
             " WHERE date >= ? AND date <= ?"
             " GROUP BY product_id LIMIT 5000")
        t0 = time.time()
        df = conn.execute(q, [start_date, end_date]).fetchdf()
        logger.info("[TIMING] query_sales_by_product_duckdb: %.3fs", time.time() - t0)
        return df
