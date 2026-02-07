from datetime import date
from typing import Dict, Optional
import pandas as pd
import time
from .cache import cache
from .duckdb_connector import get_duckdb_connection


@cache.memoize()
def query_profit_trends(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Query profit trends - optimized for daily totals with optional drill-down."""
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
        COALESCE(SUM(ap.revenue_tax_in), 0) as revenue,
        COALESCE(SUM(ap.cogs_tax_in), 0) as cogs,
        COALESCE(SUM(ap.gross_profit), 0) as gross_profit,
        COALESCE(SUM(ap.quantity), 0) as items_sold,
        COALESCE(SUM(ap.transactions), 0) as transactions,
        COALESCE(SUM(ap.lines), 0) as lines,
        CASE 
            WHEN SUM(ap.transactions) > 0 
            THEN SUM(ap.revenue_tax_in) / SUM(ap.transactions) 
            ELSE 0 
        END as avg_transaction_value,
        CASE 
            WHEN SUM(ap.revenue_tax_in) > 0 
            THEN SUM(ap.gross_profit) / SUM(ap.revenue_tax_in) * 100 
            ELSE 0 
        END as gross_margin_pct
    FROM date_series ds
    LEFT JOIN agg_profit_daily ap ON 
        ap.date >= ds.period_start AND 
        ap.date < ds.period_end
    GROUP BY ds.period_start
    ORDER BY ds.period_start
    """
    
    query_start = time.time()
    result = conn.execute(query, [start_date, end_date]).fetchdf()
    print(f"[TIMING] query_profit_trends: {time.time() - query_start:.3f}s")
    return result


@cache.memoize()
def query_profit_by_product(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Query top products by profit - uses aggregate table for performance."""
    conn = get_duckdb_connection()

    query = """
    WITH product_profit AS (
        SELECT 
            product_id,
            SUM(revenue_tax_in) as total_revenue,
            SUM(cogs_tax_in) as total_cogs,
            SUM(gross_profit) as total_profit,
            SUM(quantity) as total_quantity,
            SUM(lines) as total_lines
        FROM agg_profit_daily_by_product
        WHERE date >= ? AND date < ? + INTERVAL 1 DAY
        GROUP BY product_id
        ORDER BY total_profit DESC
        LIMIT ?
    )
    SELECT 
        COALESCE(p.product_name, 'Product ' || s.product_id::VARCHAR) as product_name,
        COALESCE(p.product_category, 'Unknown Category') as category,
        s.total_revenue,
        s.total_cogs,
        s.total_profit,
        s.total_quantity,
        s.total_lines,
        CASE 
            WHEN s.total_revenue > 0 
            THEN s.total_profit / s.total_revenue * 100 
            ELSE 0 
        END as profit_margin_pct
    FROM product_profit s
    LEFT JOIN dim_products p ON s.product_id = p.product_id
    ORDER BY s.total_profit DESC
    """
    
    query_start = time.time()
    result = conn.execute(query, [start_date, end_date, limit]).fetchdf()
    print(f"[TIMING] query_profit_by_product: {time.time() - query_start:.3f}s")
    return result


@cache.memoize()
def query_profit_summary(start_date: date, end_date: date) -> Dict:
    """Get profit summary - single query for all key metrics."""
    conn = get_duckdb_connection()

    query = """
    SELECT 
        SUM(revenue_tax_in) as revenue,
        SUM(cogs_tax_in) as cogs,
        SUM(gross_profit) as gross_profit,
        SUM(quantity) as quantity,
        SUM(transactions) as transactions,
        SUM(lines) as lines,
        CASE 
            WHEN SUM(transactions) > 0 
            THEN SUM(revenue_tax_in) / SUM(transactions) 
            ELSE 0 
        END as avg_transaction_value,
        CASE 
            WHEN SUM(revenue_tax_in) > 0 
            THEN SUM(gross_profit) / SUM(revenue_tax_in) * 100 
            ELSE 0 
        END as gross_margin_pct
    FROM agg_profit_daily
    WHERE date >= ? AND date < ? + INTERVAL 1 DAY
    """
    
    query_start = time.time()
    row = conn.execute(query, [start_date, end_date]).fetchone()
    print(f"[TIMING] query_profit_summary: {time.time() - query_start:.3f}s")
    
    revenue, cogs, gross_profit, quantity, transactions, lines, atv, margin_pct = [
        v or 0 for v in row
    ]

    return {
        'revenue': float(revenue),
        'cogs': float(cogs),
        'gross_profit': float(gross_profit),
        'quantity': float(quantity),
        'transactions': int(transactions),
        'lines': int(lines),
        'avg_transaction_value': float(atv),
        'gross_margin_pct': float(margin_pct)
    }


def query_profit_drilldown(start_date: date, end_date: date, product_id: Optional[int] = None) -> pd.DataFrame:
    """Drill-down to line-level profit details - use sparingly for detailed analysis."""
    conn = get_duckdb_connection()

    if product_id:
        where_clause = "WHERE date >= ? AND date < ? + INTERVAL 1 DAY AND product_id = ?"
        params = [start_date, end_date, product_id]
    else:
        where_clause = "WHERE date >= ? AND date < ? + INTERVAL 1 DAY"
        params = [start_date, end_date]

    query = f"""
    SELECT 
        date,
        txn_id,
        line_id,
        product_id,
        quantity,
        revenue_tax_in,
        cost_unit_tax_in,
        cogs_tax_in,
        gross_profit,
        CASE 
            WHEN revenue_tax_in > 0 
            THEN gross_profit / revenue_tax_in * 100 
            ELSE 0 
        END as profit_margin_pct
    FROM fact_sales_lines_profit
    {where_clause}
    ORDER BY date, gross_profit DESC
    """
    
    query_start = time.time()
    result = conn.execute(query, params).fetchdf()
    print(f"[TIMING] query_profit_drilldown: {time.time() - query_start:.3f}s")
    return result
