from datetime import date
from typing import Dict
import pandas as pd
import time

from services.sqlite_manager import SQLiteManager


def get_sales_trends_data(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """
    Get revenue trend data for the specified date range and period using SQLite.
    
    Args:
        start_date: Start date for the analysis
        end_date: End date for the analysis
        period: 'daily', 'weekly', or 'monthly' aggregation
    
    Returns:
        DataFrame with columns: date, revenue, transactions, avg_transaction_value
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    
    try:
        manager = SQLiteManager()
        
        with manager.reader_conn() as conn:
            # Generate date series in Python (SQLite doesn't have generate_series)
            dates = pd.date_range(start=start_date, end=end_date, freq="D").date.tolist()
            
            query = """
            SELECT 
                ? as date,
                COALESCE(SUM(mv.revenue), 0) as revenue,
                COALESCE(SUM(mv.transactions), 0) as transactions,
                COALESCE(SUM(mv.items_sold), 0) as items_sold,
                COALESCE(SUM(mv.lines), 0) as lines
            FROM mv_sales_daily mv
            WHERE date(mv.date) = ?
            """
            
            query_start = time.time()
            results = []
            for d in dates:
                result = pd.read_sql_query(query, conn, params=[d, d])
                results.append(result)
            
            df = pd.concat(results, ignore_index=True)
            print(f"[TIMING] query_sales_trends: {time.time() - query_start:.3f}s")
            return df
    except Exception as e:
        print(f"SQLite query failed in get_sales_trends_data: {e}")
        return pd.DataFrame(columns=['date', 'revenue', 'transactions', 'avg_transaction_value'])

def get_daily_transaction_counts(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Return transactions aggregated per day between start_date and end_date.
    """
    trends_df = get_sales_trends_data(start_date, end_date, period='daily')
    if trends_df.empty:
        return pd.DataFrame(columns=['date', 'transactions'])
    return trends_df[['date', 'transactions']].copy()


def get_revenue_comparison(start_date: date, end_date: date) -> Dict:
    """
    Compare revenue between current period and previous period of same length using SQLite.
    
    Args:
        start_date: Current period start date
        end_date: Current period end date
    
    Returns:
        Dict with current and previous period metrics
    """
    try:
        manager = SQLiteManager()
        
        with manager.reader_conn() as conn:
            # Current period
            current_query = """
            SELECT 
                COALESCE(SUM(revenue), 0) as revenue,
                COALESCE(SUM(transactions), 0) as transactions,
                COALESCE(SUM(items_sold), 0) as items_sold
            FROM mv_sales_daily
            WHERE date BETWEEN ? AND ?
            """
            current_df = pd.read_sql_query(current_query, conn, params=[start_date, end_date])
            
            # Previous period (same length, immediately before)
            days_diff = (end_date - start_date).days + 1
            prev_start = start_date - pd.Timedelta(days=days_diff)
            prev_end = start_date - pd.Timedelta(days=1)
            
            prev_query = """
            SELECT 
                COALESCE(SUM(revenue), 0) as revenue,
                COALESCE(SUM(transactions), 0) as transactions,
                COALESCE(SUM(items_sold), 0) as items_sold
            FROM mv_sales_daily
            WHERE date BETWEEN ? AND ?
            """
            prev_df = pd.read_sql_query(prev_query, conn, params=[prev_start, prev_end])
            
            current = current_df.iloc[0].to_dict()
            previous = prev_df.iloc[0].to_dict()
            
            # Calculate deltas
            current_atv = current['transactions'] if current['transactions'] > 0 else 1
            prev_atv = previous['transactions'] if previous['transactions'] > 0 else 1
            
            deltas = {
                'revenue': current['revenue'] - previous['revenue'],
                'revenue_pct': ((current['revenue'] / previous['revenue'] - 1) * 100) if previous['revenue'] > 0 else 0,
                'transactions': current['transactions'] - previous['transactions'],
                'transactions_pct': ((current['transactions'] / previous['transactions'] - 1) * 100) if previous['transactions'] > 0 else 0,
                'items_sold': current['items_sold'] - previous['items_sold'],
                'items_sold_pct': ((current['items_sold'] / previous['items_sold'] - 1) * 100) if previous['items_sold'] > 0 else 0,
                'avg_transaction_value': (current['revenue'] / current_atv) - (previous['revenue'] / prev_atv),
                'avg_transaction_value_pct': 0  # Simplified
            }
            
            return {
                'current': current,
                'previous': previous,
                'deltas': deltas
            }
    except Exception as e:
        print(f"SQLite query failed in get_revenue_comparison: {e}")
        return {
            'current': {'revenue': 0, 'transactions': 0, 'items_sold': 0, 'avg_transaction_value': 0},
            'previous': {'revenue': 0, 'transactions': 0, 'items_sold': 0, 'avg_transaction_value': 0},
            'deltas': {'revenue': 0, 'revenue_pct': 0, 'transactions': 0, 'transactions_pct': 0,
                      'items_sold': 0, 'items_sold_pct': 0, 'avg_transaction_value': 0, 'avg_transaction_value_pct': 0}
        }

def get_hourly_sales_pattern(target_date: date) -> pd.DataFrame:
    """
    Get hourly sales pattern for a specific date using SQLite.
    Times are converted to Bangkok timezone (UTC+7) and filtered to store hours (7:00-23:00).
    
    Args:
        target_date: Date to analyze
    
    Returns:
        DataFrame with hourly revenue and transaction counts for active hours only
    """
    try:
        manager = SQLiteManager()
        
        with manager.reader_conn() as conn:
            # Hourly data not materialized in SQLite MVs yet - return empty
            print(f"Hourly sales pattern not yet implemented in SQLite MVs")
            return pd.DataFrame(columns=['hour', 'revenue', 'transactions'])
    except Exception as e:
        print(f"SQLite query failed in get_hourly_sales_pattern: {e}")
        return pd.DataFrame(columns=['hour', 'revenue', 'transactions'])

def get_hourly_sales_heatmap_data(start_date: date, end_date: date) -> pd.DataFrame:
    """Get hourly sales heatmap data across a date range using SQLite (single query)."""
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    try:
        manager = SQLiteManager()
        
        with manager.reader_conn() as conn:
            # Hourly data not materialized in SQLite MVs yet - return empty
            print(f"Hourly sales heatmap not yet implemented in SQLite MVs")
            return pd.DataFrame(columns=['date', 'hour', 'revenue'])
    except Exception as e:
        print(f"SQLite query failed in get_hourly_sales_heatmap_data: {e}")
        return pd.DataFrame(columns=['date', 'hour', 'revenue'])


def get_top_products(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """
    Get top selling products by revenue for the specified date range using SQLite.
    
    Args:
        start_date: Start date
        end_date: End date
        limit: Number of top products to return (default 20)
    
    Returns:
        DataFrame with top products metrics including name, category, quantity, and total revenue
    """
    try:
        manager = SQLiteManager()
        
        with manager.reader_conn() as conn:
            query = """
            SELECT 
                product_id,
                SUM(revenue) as total_revenue,
                SUM(quantity) as quantity_sold
            FROM mv_sales_by_product
            WHERE date BETWEEN ? AND ?
            GROUP BY product_id
            ORDER BY total_revenue DESC
            LIMIT ?
            """
            df = pd.read_sql_query(query, conn, params=[start_date, end_date, limit])
            return df
    except Exception as e:
        print(f"SQLite query failed in get_top_products: {e}")
        return pd.DataFrame(columns=['product_id', 'total_revenue', 'quantity_sold'])


def get_sales_by_principal(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Aggregate sales revenue by principal.

    Principal is derived from brand via dim_brands.parquet (brand -> principal_name).
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    try:
        manager = SQLiteManager()
        
        with manager.reader_conn() as conn:
            query = """
            SELECT 
                principal,
                SUM(revenue) as revenue
            FROM mv_sales_by_principal
            WHERE date BETWEEN ? AND ?
            GROUP BY principal
            ORDER BY revenue DESC
            LIMIT ?
            """
            df = pd.read_sql_query(query, conn, params=[start_date, end_date, limit])
            return df
    except Exception as e:
        print(f"SQLite query failed in get_sales_by_principal: {e}")
        return pd.DataFrame(columns=['principal', 'revenue'])
