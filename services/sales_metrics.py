from datetime import date, timedelta
from typing import Dict, List, Tuple
import pandas as pd
import polars as pl
import pytz

from services.pos_data import get_pos_order_lines_batched, create_fact_dataframe
from services.duckdb_connector import query_sales_trends, query_hourly_sales_pattern, query_hourly_sales_heatmap, query_top_products, query_revenue_comparison


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
        print(f"DuckDB query failed in get_sales_trends_data: {e}, falling back to Odoo")
        return _get_sales_trends_data_odoo_fallback(start_date, end_date, period)

def get_daily_transaction_counts(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Return transactions aggregated per day between start_date and end_date.
    """
    trends_df = get_sales_trends_data(start_date, end_date, period='daily')
    if trends_df.empty:
        return pd.DataFrame(columns=['date', 'transactions'])
    return trends_df[['date', 'transactions']].copy()

def _get_sales_trends_data_odoo_fallback(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Odoo fallback for sales trends if DuckDB fails."""
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
        print(f"Polars conversion failed in fallback: {e}, using pandas")
        return _get_sales_trends_data_pandas_fallback(df_pandas, start_date, end_date, period)

    revenue_col = 'price_subtotal_incl'
    
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
            pl.col(revenue_col).sum().alias('revenue'),
            pl.col('order_date').n_unique().alias('transactions'),
            pl.col('qty').sum().alias('items_sold')
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
        trends = all_dates.join(trends, on='date', how='left')
        # Fill null values with 0 for numeric columns only
        numeric_cols = ['revenue', 'transactions', 'items_sold', 'avg_transaction_value']
        trends = trends.with_columns([
            pl.col(col).fill_null(0) for col in numeric_cols if col in trends.columns
        ])
    
    # Convert back to pandas for Plotly compatibility
    result = trends.to_pandas()
    
    # Ensure all required columns exist in the result
    required_cols = ['date', 'revenue', 'transactions', 'items_sold', 'avg_transaction_value']
    for col in required_cols:
        if col not in result.columns:
            result[col] = 0
            
    return result[required_cols]

def _get_sales_trends_data_pandas_fallback(df, start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Pandas fallback for sales trends if Polars conversion fails."""
    revenue_col = 'price_subtotal_incl'
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
            revenue_col: 'sum',
            'order_date': 'nunique',
            'qty': 'sum'
        })
        .reset_index()
        .rename(columns={
            revenue_col: 'revenue',
            'order_date': 'transactions',
            'qty': 'items_sold'
        })
    )
    
    # Calculate average transaction value
    trends['avg_transaction_value'] = trends['revenue'] / trends['transactions'].replace(0, 1)
    
    # Convert date_group to datetime for plotting
    trends['date'] = pd.to_datetime(trends['date_group'])
    
    # Ensure we have all dates in the range (fill missing dates with zeros)
    if period == 'daily':
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        trends = trends.set_index('date').reindex(all_dates).reset_index()
        trends = trends.rename(columns={'index': 'date'})
        # Fill null values with 0 for numeric columns only
        numeric_cols = ['revenue', 'transactions', 'items_sold', 'avg_transaction_value']
        trends[numeric_cols] = trends[numeric_cols].fillna(0)
    
    # Ensure all required columns exist in the result
    required_cols = ['date', 'revenue', 'transactions', 'items_sold', 'avg_transaction_value']
    for col in required_cols:
        if col not in trends.columns:
            trends[col] = 0
            
    return trends[required_cols]


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
        print(f"DuckDB query failed in get_revenue_comparison: {e}, falling back to Odoo")
        return _get_revenue_comparison_odoo_fallback(start_date, end_date)


def _get_revenue_comparison_odoo_fallback(start_date: date, end_date: date) -> Dict:
    """Odoo fallback for revenue comparison if DuckDB fails."""
    # Current period data
    current_trends = get_sales_trends_data(start_date, end_date, 'daily')
    current_revenue = current_trends['revenue'].sum()
    current_transactions = current_trends['transactions'].sum()
    current_items_sold = current_trends['items_sold'].sum()
    current_avg_atv = current_revenue / current_transactions if current_transactions > 0 else 0
    
    # Previous period (same length)
    period_length = (end_date - start_date).days + 1
    prev_start = start_date - timedelta(days=period_length)
    prev_end = start_date - timedelta(days=1)
    
    prev_trends = get_sales_trends_data(prev_start, prev_end, 'daily')
    prev_revenue = prev_trends['revenue'].sum()
    prev_transactions = prev_trends['transactions'].sum()
    prev_items_sold = prev_trends['items_sold'].sum()
    prev_avg_atv = prev_revenue / prev_transactions if prev_transactions > 0 else 0
    
    # Calculate deltas
    revenue_delta = current_revenue - prev_revenue
    revenue_delta_pct = (revenue_delta / prev_revenue * 100) if prev_revenue > 0 else 0
    
    transactions_delta = current_transactions - prev_transactions
    transactions_delta_pct = (transactions_delta / prev_transactions * 100) if prev_transactions > 0 else 0
    
    items_delta = current_items_sold - prev_items_sold
    items_delta_pct = (items_delta / prev_items_sold * 100) if prev_items_sold > 0 else 0
    
    atv_delta = current_avg_atv - prev_avg_atv
    atv_delta_pct = (atv_delta / prev_avg_atv * 100) if prev_avg_atv > 0 else 0
    
    return {
        'current': {
            'revenue': current_revenue,
            'transactions': current_transactions,
            'items_sold': current_items_sold,
            'avg_transaction_value': current_avg_atv,
        },
        'previous': {
            'revenue': prev_revenue,
            'transactions': prev_transactions,
            'items_sold': prev_items_sold,
            'avg_transaction_value': prev_avg_atv,
        },
        'deltas': {
            'revenue': revenue_delta,
            'revenue_pct': revenue_delta_pct,
            'transactions': transactions_delta,
            'transactions_pct': transactions_delta_pct,
            'items_sold': items_delta,
            'items_sold_pct': items_delta_pct,
            'avg_transaction_value': atv_delta,
            'avg_transaction_value_pct': atv_delta_pct,
        }
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
        print(f"DuckDB query failed in get_hourly_sales_pattern: {e}, falling back to Odoo")
        return _get_hourly_sales_pattern_odoo_fallback(target_date)

def get_hourly_sales_heatmap_data(start_date: date, end_date: date) -> pd.DataFrame:
    """Get hourly sales heatmap data across a date range using DuckDB (single query)."""
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    try:
        return query_hourly_sales_heatmap(start_date, end_date)
    except Exception as e:
        print(f"DuckDB query failed in get_hourly_sales_heatmap_data: {e}, falling back to per-day fetch")

    all_data = []
    current_date = start_date
    while current_date <= end_date:
        hourly_data = get_hourly_sales_pattern(current_date)
        if not hourly_data.empty:
            hourly_data['date'] = current_date
            all_data.append(hourly_data)
        current_date += timedelta(days=1)

    if not all_data:
        return pd.DataFrame(columns=['date', 'hour', 'revenue'])

    combined = pd.concat(all_data, ignore_index=True)
    if 'revenue' not in combined.columns:
        return pd.DataFrame(columns=['date', 'hour', 'revenue'])

    return combined[['date', 'hour', 'revenue']]


def _get_hourly_sales_pattern_odoo_fallback(target_date: date) -> pd.DataFrame:
    """Odoo fallback for hourly sales pattern if DuckDB fails."""
    lines = get_pos_order_lines_batched(target_date, target_date)
    if not lines:
        return pd.DataFrame(columns=['hour', 'revenue', 'transactions'])
    
    df_pandas = create_fact_dataframe(lines)
    if df_pandas.empty:
        return pd.DataFrame(columns=['hour', 'revenue', 'transactions'])
    
    try:
        # Validate DataFrame before Polars conversion
        revenue_col = 'price_subtotal_incl'
        required_columns = ['order_date', revenue_col]
        missing_cols = [col for col in required_columns if col not in df_pandas.columns]
        if missing_cols:
            print(f"Missing required columns: {missing_cols}")
            return pd.DataFrame(columns=['hour', 'revenue', 'transactions'])
        
        # Convert to Bangkok timezone (UTC+7)
        bangkok_tz = pytz.timezone('Asia/Bangkok')
        # Ensure order_date is datetime and localize to UTC first, then convert to Bangkok
        df_pandas['order_date'] = pd.to_datetime(df_pandas['order_date'], errors='coerce')
        # If timezone naive, assume UTC
        if df_pandas['order_date'].dt.tz is None:
            df_pandas['order_date'] = df_pandas['order_date'].dt.tz_localize('UTC')
        df_pandas['order_date'] = df_pandas['order_date'].dt.tz_convert('Asia/Bangkok')
        
        # Drop rows with invalid dates
        df_pandas = df_pandas.dropna(subset=['order_date'])
        
        if df_pandas.empty:
            return pd.DataFrame(columns=['hour', 'revenue', 'transactions'])
        
        # Clean data for Polars conversion - remove problematic columns
        # Only keep columns we actually need for hourly analysis
        columns_to_keep = ['order_date', revenue_col]
        if 'order_id' in df_pandas.columns:
            columns_to_keep.append('order_id')
        
        df_clean = df_pandas[columns_to_keep].copy()
        
        # Ensure numeric columns are properly typed
        df_clean[revenue_col] = pd.to_numeric(df_clean[revenue_col], errors='coerce').fillna(0)
        
        # Convert to Polars with explicit schema handling
        df = pl.from_pandas(df_clean)
        
        # Extract hour using Polars (Bangkok time)
        df = df.with_columns(
            pl.col('order_date').dt.hour().alias('hour')
        )
        
        # Filter to store active hours (7:00-23:00)
        df = df.filter((pl.col('hour') >= 7) & (pl.col('hour') <= 23))
        
        # Aggregate by hour using Polars
        hourly = (
            df
            .group_by('hour')
            .agg([
                pl.col(revenue_col).sum().alias('revenue'),
                pl.col('order_date').n_unique().alias('transactions')
            ])
            .sort('hour')
        )
        
        # Fill missing active hours (7-23) with zeros
        active_hours = pl.DataFrame({'hour': range(7, 24)})
        hourly = active_hours.join(hourly, on='hour', how='left').fill_null(0)
        
        return hourly.to_pandas()
    
    except Exception as e:
        print(f"Error in Polars processing for hourly pattern: {e}")
        # Fallback to pandas processing
        return _get_hourly_sales_pattern_pandas_fallback(df_pandas)

def _get_hourly_sales_pattern_pandas_fallback(df_pandas) -> pd.DataFrame:
    """Pandas fallback for hourly sales pattern if Polars conversion fails."""
    revenue_col = 'price_subtotal_incl'

    # Convert to Bangkok timezone (UTC+7)
    df_pandas['order_date'] = pd.to_datetime(df_pandas['order_date'], errors='coerce')
    # If timezone naive, assume UTC
    if df_pandas['order_date'].dt.tz is None:
        df_pandas['order_date'] = df_pandas['order_date'].dt.tz_localize('UTC')
    df_pandas['order_date'] = df_pandas['order_date'].dt.tz_convert('Asia/Bangkok')
    
    # Drop rows with invalid dates
    df_pandas = df_pandas.dropna(subset=['order_date'])
    
    if df_pandas.empty:
        return pd.DataFrame(columns=['hour', 'revenue', 'transactions'])
    
    # Extract hour
    df_pandas['hour'] = df_pandas['order_date'].dt.hour
    
    # Filter to store active hours (7:00-23:00)
    df_pandas = df_pandas[(df_pandas['hour'] >= 7) & (df_pandas['hour'] <= 23)]
    
    if df_pandas.empty:
        # Return empty DataFrame with active hours
        return pd.DataFrame({'hour': range(7, 24), 'revenue': 0, 'transactions': 0})
    
    # Aggregate by hour
    hourly = (
        df_pandas.groupby('hour')
        .agg({
            revenue_col: 'sum',
            'order_date': 'nunique'
        })
        .reset_index()
        .rename(columns={
            revenue_col: 'revenue',
            'order_date': 'transactions'
        })
    )
    
    # Sort by hour (use pandas sort_values)
    hourly = hourly.sort_values('hour')
    
    # Fill missing active hours (7-23) with zeros
    active_hours = pd.DataFrame({'hour': range(7, 24)})
    hourly = active_hours.merge(hourly, on='hour', how='left').fillna(0)
    
    return hourly


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


def _get_top_products_odoo_fallback(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Odoo fallback for top products if DuckDB fails."""
    lines = get_pos_order_lines_batched(start_date, end_date)
    if not lines:
        return pd.DataFrame(columns=['product_name', 'category', 'quantity_sold', 'total_unit_price'])
    
    df_pandas = create_fact_dataframe(lines)
    if df_pandas.empty:
        return pd.DataFrame(columns=['product_name', 'category', 'quantity_sold', 'total_unit_price'])
    
    try:
        # Validate DataFrame before Polars conversion
        revenue_col = 'price_subtotal_incl'
        required_columns = ['product_id', revenue_col, 'qty']
        missing_cols = [col for col in required_columns if col not in df_pandas.columns]
        if missing_cols:
            print(f"Missing required columns for top products: {missing_cols}")
            return pd.DataFrame(columns=['product_name', 'category', 'quantity_sold', 'total_unit_price'])
        
        # Clean data for Polars conversion - only keep columns we need
        columns_to_keep = ['product_id', revenue_col, 'qty', 'product_category']
        # Add product_name if available
        if 'product_id' in df_pandas.columns:
            columns_to_keep.append('product_id')
        
        df_clean = df_pandas[columns_to_keep].copy()
        
        # Ensure numeric columns are properly typed
        df_clean[revenue_col] = pd.to_numeric(df_clean[revenue_col], errors='coerce').fillna(0)
        df_clean['qty'] = pd.to_numeric(df_clean['qty'], errors='coerce').fillna(0)
        
        # Convert to Polars with explicit schema handling
        df = pl.from_pandas(df_clean)
        
        # Group by product_id and aggregate using Polars
        top_products = (
            df
            .group_by(['product_id', 'product_category'])
            .agg([
                pl.col(revenue_col).sum().alias('total_unit_price'),
                pl.col('qty').sum().alias('quantity_sold')
            ])
            .sort('total_unit_price', descending=True)
            .limit(limit)
        )
        
        # Create product name from product_id
        top_products = top_products.with_columns(
            ('Product ' + pl.col('product_id').cast(str)).alias('product_name')
        )
        
        # Handle missing categories
        top_products = top_products.with_columns(
            pl.col('product_category').fill_null('Unknown Category').alias('category')
        )
        
        # Select and reorder columns for final output
        result = top_products.select([
            'product_name',
            'category', 
            'quantity_sold',
            'total_unit_price'
        ])
        
        return result.to_pandas()
    
    except Exception as e:
        print(f"Error in Polars processing for top products: {e}")
        # Fallback to pandas processing
        return _get_top_products_pandas_fallback(df_pandas, limit)

def _get_top_products_pandas_fallback(df_pandas, limit: int = 20) -> pd.DataFrame:
    """Pandas fallback for top products if Polars conversion fails."""
    revenue_col = 'price_subtotal_incl'

    # Group by product_id and category using pandas
    group_cols = ['product_id']
    if 'product_category' in df_pandas.columns:
        group_cols.append('product_category')
    
    top_products = (
        df_pandas.groupby(group_cols)
        .agg({
            revenue_col: 'sum',
            'qty': 'sum'
        })
        .reset_index()
        .rename(columns={
            revenue_col: 'total_unit_price',
            'qty': 'quantity_sold'
        })
    )
    
    # Handle missing category
    if 'product_category' not in top_products.columns:
        top_products['product_category'] = 'Unknown Category'
    
    # Sort by total revenue and limit
    top_products = top_products.sort_values('total_unit_price', ascending=False).head(limit)
    
    # Create product name
    top_products['product_name'] = 'Product ' + top_products['product_id'].astype(str)
    
    # Rename category column for consistency
    top_products = top_products.rename(columns={'product_category': 'category'})
    
    return top_products[['product_name', 'category', 'quantity_sold', 'total_unit_price']]
