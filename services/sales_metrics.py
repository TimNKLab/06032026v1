from datetime import date, timedelta
from typing import Dict, List, Tuple
import pandas as pd
import polars as pl

from services.pos_data import get_pos_order_lines_batched, create_fact_dataframe


def get_sales_trends_data(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """
    Get revenue trend data for the specified date range and period using Polars.
    
    Args:
        start_date: Start date for the analysis
        end_date: End date for the analysis
        period: 'daily', 'weekly', or 'monthly' aggregation
    
    Returns:
        DataFrame with columns: date, revenue, transactions, avg_transaction_value
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    
    # Get POS data for the date range
    lines = get_pos_order_lines_batched(start_date, end_date)
    if not lines:
        return pd.DataFrame(columns=['date', 'revenue', 'transactions', 'avg_transaction_value'])
    
    # Create pandas DataFrame first, then convert to Polars
    df_pandas = create_fact_dataframe(lines)
    if df_pandas.empty:
        return pd.DataFrame(columns=['date', 'revenue', 'transactions', 'avg_transaction_value'])
    
    try:
        df = pl.from_pandas(df_pandas)
    except Exception as e:
        print(f"Polars conversion failed in get_sales_trends_data: {e}, falling back to pandas")
        return _get_sales_trends_data_pandas_fallback(df_pandas, start_date, end_date, period)
    
    # Add period grouping using Polars expressions
    if period == 'daily':
        df = df.with_columns(
            pl.col('order_date').dt.date().alias('date_group')
        )
    elif period == 'weekly':
        df = df.with_columns(
            pl.col('order_date').dt.truncate('1w').alias('date_group')
        )
    elif period == 'monthly':
        df = df.with_columns(
            pl.col('order_date').dt.truncate('1mo').alias('date_group')
        )
    else:
        raise ValueError("Period must be 'daily', 'weekly', or 'monthly'")
    
    # Aggregate using Polars (much faster than pandas)
    trends = (
        df
        .group_by('date_group')
        .agg([
            pl.col('price_subtotal_incl').sum().alias('revenue'),
            pl.col('order_date').n_unique().alias('transactions')
        ])
        .sort('date_group')
        .with_columns(
            (pl.col('revenue') / pl.col('transactions').replace(0, 1)).alias('avg_transaction_value')
        )
        .rename({'date_group': 'date'})
    )
    
    # Ensure we have all dates in the range (fill missing dates with zeros)
    if period == 'daily':
        all_dates = pl.date_range(start_date, end_date, interval='1d', eager=True).alias('date')
        trends = all_dates.join(trends, on='date', how='left').fill_null(0)
    
    # Convert back to pandas for Plotly compatibility
    return trends.to_pandas()

def _get_sales_trends_data_pandas_fallback(df, start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Pandas fallback for sales trends if Polars conversion fails."""
    # Add period grouping using pandas
    if period == 'daily':
        df['date_group'] = df['order_date'].dt.date
    elif period == 'weekly':
        df['date_group'] = df['order_date'].dt.to_period('W').dt.start_time
    elif period == 'monthly':
        df['date_group'] = df['order_date'].dt.to_period('M').dt.start_time
    else:
        raise ValueError("Period must be 'daily', 'weekly', or 'monthly'")
    
    # Aggregate metrics using pandas
    trends = (
        df.groupby('date_group')
        .agg({
            'price_subtotal_incl': 'sum',
            'order_date': 'nunique'
        })
        .reset_index()
        .rename(columns={
            'price_subtotal_incl': 'revenue',
            'order_date': 'transactions'
        })
    )
    
    # Calculate average transaction value
    trends['avg_transaction_value'] = trends['revenue'] / trends['transactions'].replace(0, 1)
    
    # Convert date_group to datetime for plotting
    trends['date'] = pd.to_datetime(trends['date_group'])
    
    # Ensure we have all dates in the range (fill missing dates with zeros)
    if period == 'daily':
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        trends = trends.set_index('date').reindex(all_dates).fillna(0).reset_index()
        trends = trends.rename(columns={'index': 'date'})
    
    return trends[['date', 'revenue', 'transactions', 'avg_transaction_value']]


def get_revenue_comparison(start_date: date, end_date: date) -> Dict:
    """
    Compare revenue between current period and previous period of same length.
    
    Args:
        start_date: Current period start date
        end_date: Current period end date
    
    Returns:
        Dict with current and previous period metrics
    """
    # Current period data
    current_trends = get_sales_trends_data(start_date, end_date, 'daily')
    current_revenue = current_trends['revenue'].sum()
    current_transactions = current_trends['transactions'].sum()
    current_avg_atv = current_revenue / current_transactions if current_transactions > 0 else 0
    
    # Previous period (same length)
    period_length = (end_date - start_date).days + 1
    prev_start = start_date - timedelta(days=period_length)
    prev_end = start_date - timedelta(days=1)
    
    prev_trends = get_sales_trends_data(prev_start, prev_end, 'daily')
    prev_revenue = prev_trends['revenue'].sum()
    prev_transactions = prev_trends['transactions'].sum()
    prev_avg_atv = prev_revenue / prev_transactions if prev_transactions > 0 else 0
    
    # Calculate deltas
    revenue_delta = current_revenue - prev_revenue
    revenue_delta_pct = (revenue_delta / prev_revenue * 100) if prev_revenue > 0 else 0
    
    transactions_delta = current_transactions - prev_transactions
    transactions_delta_pct = (transactions_delta / prev_transactions * 100) if prev_transactions > 0 else 0
    
    atv_delta = current_avg_atv - prev_avg_atv
    atv_delta_pct = (atv_delta / prev_avg_atv * 100) if prev_avg_atv > 0 else 0
    
    return {
        'current': {
            'revenue': current_revenue,
            'transactions': current_transactions,
            'avg_transaction_value': current_avg_atv,
        },
        'previous': {
            'revenue': prev_revenue,
            'transactions': prev_transactions,
            'avg_transaction_value': prev_avg_atv,
        },
        'deltas': {
            'revenue': revenue_delta,
            'revenue_pct': revenue_delta_pct,
            'transactions': transactions_delta,
            'transactions_pct': transactions_delta_pct,
            'avg_transaction_value': atv_delta,
            'avg_transaction_value_pct': atv_delta_pct,
        }
    }


def get_hourly_sales_pattern(target_date: date) -> pd.DataFrame:
    """
    Get hourly sales pattern for a specific date using Polars.
    
    Args:
        target_date: Date to analyze
    
    Returns:
        DataFrame with hourly revenue and transaction counts
    """
    lines = get_pos_order_lines_batched(target_date, target_date)
    if not lines:
        return pd.DataFrame(columns=['hour', 'revenue', 'transactions'])
    
    df_pandas = create_fact_dataframe(lines)
    df = pl.from_pandas(df_pandas)
    
    # Extract hour using Polars
    df = df.with_columns(
        pl.col('order_date').dt.hour().alias('hour')
    )
    
    # Aggregate by hour using Polars
    hourly = (
        df
        .group_by('hour')
        .agg([
            pl.col('price_subtotal_incl').sum().alias('revenue'),
            pl.col('order_date').n_unique().alias('transactions')
        ])
        .sort('hour')
    )
    
    # Fill missing hours (0-23) with zeros
    all_hours = pl.DataFrame({'hour': range(24)})
    hourly = all_hours.join(hourly, on='hour', how='left').fill_null(0)
    
    return hourly.to_pandas()


def get_top_products(start_date: date, end_date: date, limit: int = 10) -> pd.DataFrame:
    """
    Get top selling products by revenue for the specified date range using Polars.
    
    Args:
        start_date: Start date
        end_date: End date
        limit: Number of top products to return
    
    Returns:
        DataFrame with top products metrics
    """
    lines = get_pos_order_lines_batched(start_date, end_date)
    if not lines:
        return pd.DataFrame(columns=['product_name', 'revenue', 'quantity', 'avg_price'])
    
    df_pandas = create_fact_dataframe(lines)
    df = pl.from_pandas(df_pandas)
    
    # Group by product_id and aggregate using Polars
    top_products = (
        df
        .group_by('product_id')
        .agg([
            pl.col('price_subtotal_incl').sum().alias('revenue'),
            pl.col('qty').sum().alias('quantity')
        ])
        .with_columns(
            (pl.col('revenue') / pl.col('quantity').replace(0, 1)).alias('avg_price')
        )
        .sort('revenue', descending=True)
        .limit(limit)
    )
    
    # Convert product_id to string for display
    top_products = top_products.with_columns(
        ('Product ' + pl.col('product_id').cast(str)).alias('product_name')
    )
    
    return top_products.to_pandas()
