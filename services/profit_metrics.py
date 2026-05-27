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
    WHERE mv.date = ?
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
    """Query top products by profit - uses SQLite materialized view.
    
    Note: Simplified version without dimension joins. Product names/categories
    would require SQLite dimension tables (mv_dim_products) which are not yet implemented.
    Returns product_id instead of product_name for now.
    """
    manager = SQLiteManager()
    
    query = """
    SELECT 
        product_id,
        SUM(revenue_tax_in) as total_revenue,
        SUM(cogs_tax_in) as total_cogs,
        SUM(gross_profit) as total_profit,
        SUM(quantity) as total_quantity,
        SUM(lines) as total_lines,
        CASE 
            WHEN SUM(revenue_tax_in) > 0 
            THEN SUM(gross_profit) / SUM(revenue_tax_in) * 100 
            ELSE 0 
        END as profit_margin_pct
    FROM mv_profit_daily
    WHERE date >= ? AND date <= ?
    GROUP BY product_id
    ORDER BY total_profit DESC
    LIMIT ?
    """
    
    query_start = time.time()
    with manager.reader_conn() as conn:
        result = pd.read_sql_query(query, conn, params=[start_date, end_date, limit])
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
    WHERE mv.date >= ? AND mv.date <= ?
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
    """Query profit revenue by category - uses SQLite materialized view.
    
    Note: Simplified version without dimension joins. Category breakdown
    would require SQLite dimension tables (mv_dim_products) which are not yet implemented.
    Returns aggregated profit by product_id for now.
    """
    manager = SQLiteManager()
    
    query = """
    SELECT 
        product_id,
        SUM(revenue_tax_in) as revenue_tax_in
    FROM mv_profit_daily
    WHERE date >= ? AND date <= ?
    GROUP BY product_id
    ORDER BY revenue_tax_in DESC
    """

    query_start = time.time()
    with manager.reader_conn() as conn:
        rows = conn.execute(query, [start_date, end_date]).fetchall()
    print(f"[TIMING] query_profit_revenue_by_category: {time.time() - query_start:.3f}s")

    # Simplified: return as flat dict by product_id
    result: Dict[str, float] = {}
    for product_id, revenue in rows:
        result[str(product_id)] = float(revenue or 0)

    return result


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
    """Drill-down to line-level profit details - use sparingly for detailed analysis.
    
    Note: This function still uses DuckDB for detailed drill-down queries.
    Would require SQLite MV (mv_fact_sales_lines_profit) for full migration.
    Kept on DuckDB for now as it's a detailed analysis query, not a primary dashboard query.
    """
    from .duckdb_connector import get_duckdb_connection, ensure_duckdb_view_groups
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
