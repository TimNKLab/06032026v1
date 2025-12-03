from datetime import date, timedelta
from typing import Dict, Tuple
import pandas as pd
import polars as pl

from services.pos_data import get_pos_order_lines_batched, create_fact_dataframe


def _extract_many2one_name(value):
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        return value[1]
    return value or 'Unknown'


def _summaries_for_dataframe(df):
    """Optimized aggregation using Polars DataFrame instead of pandas."""
    if df.empty:
        return 0, 0, {}
    
    # Convert to Polars for faster aggregation with proper schema handling
    try:
        df_pl = pl.from_pandas(df)
    except Exception as e:
        # Fallback to pandas if Polars conversion fails
        print(f"Polars conversion failed: {e}, falling back to pandas")
        return _summaries_for_dataframe_pandas_fallback(df)
    
    # Total aggregations using Polars
    total_amount = df_pl['price_subtotal_incl'].sum()
    total_qty = df_pl['qty'].sum()
    
    # Category hierarchy aggregation using Polars
    category_summary = (
        df_pl
        .group_by(['product_parent_category', 'product_category'])
        .agg([
            pl.col('price_subtotal_incl').sum().alias('revenue')
        ])
    )
    
    # Convert to nested dict format
    hierarchy = {}
    for row in category_summary.iter_rows():
        parent = row[0] or 'Unknown'
        child = row[1] or 'Unknown'
        amount = row[2]
        
        child_map = hierarchy.setdefault(parent, {})
        child_map[child] = child_map.get(child, 0) + amount
    
    return float(total_amount), float(total_qty), hierarchy

def _summaries_for_dataframe_pandas_fallback(df):
    """Fallback pandas aggregation if Polars conversion fails."""
    if df.empty:
        return 0, 0, {}
    
    # Total aggregations
    total_amount = df['price_subtotal_incl'].sum()
    total_qty = df['qty'].sum()
    
    # Category hierarchy aggregation
    category_summary = (
        df.groupby(['product_parent_category', 'product_category'])
        .agg({'price_subtotal_incl': 'sum'})
        .reset_index()
    )
    
    # Convert to nested dict format
    hierarchy = {}
    for _, row in category_summary.iterrows():
        parent = row['product_parent_category'] or 'Unknown'
        child = row['product_category'] or 'Unknown'
        amount = row['price_subtotal_incl']
        
        child_map = hierarchy.setdefault(parent, {})
        child_map[child] = child_map.get(child, 0) + amount
    
    return total_amount, total_qty, hierarchy

def _summaries_for_lines(lines):
    """Fallback function for backward compatibility."""
    if not lines:
        return 0, 0, {}
    
    df = create_fact_dataframe(lines)
    return _summaries_for_dataframe(df)


def get_total_overview_summary(target_date_start: date, target_date_end: date = None) -> Dict:
    if not isinstance(target_date_start, date):
        target_date_start = date.today()
    if target_date_end is None:
        target_date_end = target_date_start

    # Use optimized batch fetching for date ranges
    if target_date_start == target_date_end:
        # Single day - use existing function
        from services.pos_data import get_pos_order_lines_for_date
        today_lines = get_pos_order_lines_for_date(target_date_start)
        prev_lines = get_pos_order_lines_for_date(target_date_start - timedelta(days=1))
    else:
        # Date range - use batch fetching
        today_lines = get_pos_order_lines_batched(target_date_start, target_date_end)
        
        # For delta, compare to previous period of same length
        period_length = (target_date_end - target_date_start).days + 1
        prev_start = target_date_start - timedelta(days=period_length)
        prev_end = target_date_start - timedelta(days=1)
        prev_lines = get_pos_order_lines_batched(prev_start, prev_end)

    # Use DataFrame-based aggregation for better performance
    today_df = create_fact_dataframe(today_lines)
    prev_df = create_fact_dataframe(prev_lines)
    
    today_amount, today_qty, today_categories = _summaries_for_dataframe(today_df)
    prev_amount, *_ = _summaries_for_dataframe(prev_df)

    return {
        'target_date_start': target_date_start,
        'target_date_end': target_date_end,
        'today_amount': float(today_amount),
        'today_qty': float(today_qty),
        'prev_amount': float(prev_amount),
        'categories_nested': today_categories,
    }
