from datetime import date
from typing import Dict
import pandas as pd

from services.duckdb_connector import query_sales_trends, query_hourly_sales_pattern, query_hourly_sales_heatmap, query_top_products, query_revenue_comparison, query_sales_by_principal


def get_sales_trends_data(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """
    Get revenue trend data for the specified date range and period using DuckDB.
    
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
        # Use DuckDB for faster queries
        return query_sales_trends(start_date, end_date, period)
    except Exception as e:
        print(f"DuckDB query failed in get_sales_trends_data: {e}")
        # Return empty DataFrame - no live Odoo queries
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
    Compare revenue between current period and previous period of same length using DuckDB.
    
    Args:
        start_date: Current period start date
        end_date: Current period end date
    
    Returns:
        Dict with current and previous period metrics
    """
    try:
        # Use DuckDB for faster queries
        return query_revenue_comparison(start_date, end_date)
    except Exception as e:
        print(f"DuckDB query failed in get_revenue_comparison: {e}")
        # Return empty structure - no live Odoo queries
        return {
            'current': {'revenue': 0, 'transactions': 0, 'items_sold': 0, 'avg_transaction_value': 0},
            'previous': {'revenue': 0, 'transactions': 0, 'items_sold': 0, 'avg_transaction_value': 0},
            'deltas': {'revenue': 0, 'revenue_pct': 0, 'transactions': 0, 'transactions_pct': 0,
                      'items_sold': 0, 'items_sold_pct': 0, 'avg_transaction_value': 0, 'avg_transaction_value_pct': 0}
        }

def get_hourly_sales_pattern(target_date: date) -> pd.DataFrame:
    """
    Get hourly sales pattern for a specific date using DuckDB.
    Times are converted to Bangkok timezone (UTC+7) and filtered to store hours (7:00-23:00).
    
    Args:
        target_date: Date to analyze
    
    Returns:
        DataFrame with hourly revenue and transaction counts for active hours only
    """
    try:
        # Use DuckDB for faster queries
        return query_hourly_sales_pattern(target_date)
    except Exception as e:
        print(f"DuckDB query failed in get_hourly_sales_pattern: {e}")
        # Return empty DataFrame - no live Odoo queries
        return pd.DataFrame(columns=['hour', 'revenue', 'transactions'])

def get_hourly_sales_heatmap_data(start_date: date, end_date: date) -> pd.DataFrame:
    """Get hourly sales heatmap data across a date range using DuckDB (single query)."""
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    try:
        return query_hourly_sales_heatmap(start_date, end_date)
    except Exception as e:
        print(f"DuckDB query failed in get_hourly_sales_heatmap_data: {e}")
        # Return empty DataFrame - no live Odoo queries
        return pd.DataFrame(columns=['date', 'hour', 'revenue'])


def get_top_products(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """
    Get top selling products by revenue for the specified date range using DuckDB.
    
    Args:
        start_date: Start date
        end_date: End date
        limit: Number of top products to return (default 20)
    
    Returns:
        DataFrame with top products metrics including name, category, quantity, and total revenue
    """
    try:
        # Use DuckDB for faster queries
        return query_top_products(start_date, end_date, limit)
    except Exception as e:
        print(f"DuckDB query failed in get_top_products: {e}")
        return pd.DataFrame(columns=['product_name', 'category', 'quantity_sold', 'total_unit_price'])


def get_sales_by_principal(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Aggregate sales revenue by principal.

    Principal is derived from brand via dim_brands.parquet (brand -> principal_name).
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    try:
        return query_sales_by_principal(start_date, end_date, limit)
    except Exception as e:
        print(f"DuckDB query failed in get_sales_by_principal: {e}")
        return pd.DataFrame(columns=['principal', 'revenue'])
