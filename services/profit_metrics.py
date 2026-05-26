from datetime import date
from typing import Dict, Optional
import pandas as pd
import time
import sqlite3
from functools import lru_cache
from .sqlite_manager import SQLiteManager
from .versioned_cache import versioned_cache


@versioned_cache(ttl=3600, key_prefix="profit_trends")
@lru_cache(maxsize=32)
def query_profit_trends(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Query profit trends - uses SQLite materialized view."""
    manager = SQLiteManager()
    
    # Generate date series in Python (SQLite doesn't have generate_series)
    if period == 'daily':
        dates = pd.date_range(start=start_date, end=end_date, freq="D").date.tolist()
    elif period == 'weekly':
        dates = pd.date_range(start=start_date, end=end_date, freq="W-MON").date.tolist()
    elif period == 'monthly':
        dates = pd.date_range(start=start_date, end=end_date, freq="MS").date.tolist()
    else:
        raise ValueError("Period must be 'daily', 'weekly', or 'monthly'")
    
    query = """
    SELECT 
        ? as date,
        COALESCE(SUM(mv.revenue_tax_in), 0) as revenue,
        COALESCE(SUM(mv.cogs_tax_in), 0) as cogs,
        COALESCE(SUM(mv.gross_profit), 0) as gross_profit,
        COALESCE(SUM(mv.quantity), 0) as items_sold,
        COALESCE(SUM(mv.transactions), 0) as transactions,
        COALESCE(SUM(mv.lines), 0) as lines,
        CASE 
            WHEN SUM(mv.transactions) > 0 
            THEN SUM(mv.revenue_tax_in) / SUM(mv.transactions) 
            ELSE 0 
        END as avg_transaction_value,
        CASE 
            WHEN SUM(mv.revenue_tax_in) > 0 
            THEN SUM(mv.gross_profit) / SUM(mv.revenue_tax_in) * 100 
            ELSE 0 
        END as gross_margin_pct
    FROM mv_profit_daily mv
    WHERE date(mv.date) = ?
    """
    
    query_start = time.time()
    results = []
    with manager.reader_conn() as conn:
        for d in dates:
            result = pd.read_sql_query(query, conn, params=[d, d])
            results.append(result)
    
    df = pd.concat(results, ignore_index=True)
    print(f"[TIMING] query_profit_trends: {time.time() - query_start:.3f}s")
    return df


@lru_cache(maxsize=32)
def query_profit_by_product(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Query top products by profit - uses SQLite materialized view."""
    # Note: This still uses DuckDB for product dimension join
    # In the hybrid architecture, we keep DuckDB for complex joins
    from .duckdb_connector import get_duckdb_connection
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


@versioned_cache(ttl=3600, key_prefix="profit_summary")
@lru_cache(maxsize=32)
def query_profit_summary(start_date: date, end_date: date) -> Dict:
    """Get profit summary - uses SQLite materialized view."""
    manager = SQLiteManager()
    
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
    FROM mv_profit_daily
    WHERE date(mv.date) >= ? AND date(mv.date) <= ?
    """
    
    query_start = time.time()
    with manager.reader_conn() as conn:
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


@lru_cache(maxsize=32)
def query_profit_revenue_by_category(start_date: date, end_date: date) -> Dict[str, Dict[str, float]]:
    ensure_duckdb_view_groups({"overview", "dims"})
    conn = get_duckdb_connection()

    query = """
    SELECT
        COALESCE(NULLIF(TRIM(p.product_parent_category), ''), 'Unknown') as parent_category,
        COALESCE(NULLIF(TRIM(p.product_category), ''), 'Unknown') as category,
        SUM(a.revenue_tax_in) as revenue_tax_in
    FROM agg_profit_daily_by_product a
    LEFT JOIN dim_products p ON a.product_id = p.product_id
    WHERE a.date >= ? AND a.date < ? + INTERVAL 1 DAY
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    query_start = time.time()
    rows = conn.execute(query, [start_date, end_date]).fetchall()
    print(f"[TIMING] query_profit_revenue_by_category: {time.time() - query_start:.3f}s")

    nested: Dict[str, Dict[str, float]] = {}
    for parent, child, amt in rows:
        parent = parent or 'Unknown'
        child = child or 'Unknown'
        nested.setdefault(parent, {})[child] = float(amt or 0)

    return nested


def clear_profit_caches() -> None:
    """Clear all cached profit query functions to force fresh reads after ETL/MV updates."""
    query_profit_summary.cache_clear()
    query_profit_revenue_by_category.cache_clear()
    query_profit_trends.cache_clear()
    query_profit_by_product.cache_clear()
    # Also clear versioned Redis cache entries
    try:
        from .cache import cache
        cache.delete_many([
            k for k in (cache._cache.keys() if hasattr(cache, '_cache') else [])
            if 'profit' in str(k)
        ])
    except Exception:
        pass


def query_profit_drilldown(start_date: date, end_date: date, product_id: Optional[int] = None) -> pd.DataFrame:
    """Drill-down to line-level profit details - use sparingly for detailed analysis."""
    ensure_duckdb_view_groups({"profit_detail"})
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
