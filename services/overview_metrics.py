from datetime import date, timedelta
from typing import Dict, Tuple
import pandas as pd
import polars as pl

from services.pos_data import get_pos_order_lines_batched, create_fact_dataframe
from services.duckdb_connector import query_overview_summary


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

    revenue_col = 'price_subtotal_incl'

    # Total aggregations using Polars
    total_amount = df_pl[revenue_col].sum()
    total_qty = df_pl['qty'].sum()
    
    # Category hierarchy aggregation using Polars
    category_summary = (
        df_pl
        .group_by(['product_parent_category', 'product_category'])
        .agg([
            pl.col(revenue_col).sum().alias('revenue')
        ])
    )
    
    # Brand hierarchy aggregation using Polars (if brand data exists)
    brand_summary = None
    if 'product_brand' in df_pl.columns:
        brand_summary = (
            df_pl
            .group_by(['product_parent_category', 'product_category', 'product_brand'])
            .agg([
                pl.col(revenue_col).sum().alias('revenue')
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
    
    # Add brand data if available
    if brand_summary is not None:
        brand_hierarchy = {}
        for row in brand_summary.iter_rows():
            parent = row[0] or 'Unknown'
            child = row[1] or 'Unknown'
            brand = row[2] or 'Unknown'
            amount = row[3]
            
            brand_map = brand_hierarchy.setdefault(parent, {})
            child_map = brand_map.setdefault(child, {})
            child_map[brand] = child_map.get(brand, 0) + amount
        
        return float(total_amount), float(total_qty), hierarchy, brand_hierarchy
    
    return float(total_amount), float(total_qty), hierarchy

def _summaries_for_dataframe_pandas_fallback(df):
    """Fallback pandas aggregation if Polars conversion fails."""
    if df.empty:
        return 0, 0, {}

    revenue_col = 'price_subtotal_incl'
    
    # Total aggregations
    total_amount = df[revenue_col].sum()
    total_qty = df['qty'].sum()
    
    # Category hierarchy aggregation
    category_summary = (
        df.groupby(['product_parent_category', 'product_category'])
        .agg({revenue_col: 'sum'})
        .reset_index()
    )
    
    # Brand hierarchy aggregation (if brand data exists)
    brand_summary = None
    if 'product_brand' in df.columns:
        brand_summary = (
            df.groupby(['product_parent_category', 'product_category', 'product_brand'])
            .agg({revenue_col: 'sum'})
            .reset_index()
        )
    
    # Convert to nested dict format
    hierarchy = {}
    for _, row in category_summary.iterrows():
        parent = row['product_parent_category'] or 'Unknown'
        child = row['product_category'] or 'Unknown'
        amount = row[revenue_col]
        
        child_map = hierarchy.setdefault(parent, {})
        child_map[child] = child_map.get(child, 0) + amount
    
    # Add brand data if available
    if brand_summary is not None:
        brand_hierarchy = {}
        for _, row in brand_summary.iterrows():
            parent = row['product_parent_category'] or 'Unknown'
            child = row['product_category'] or 'Unknown'
            brand = row['product_brand'] or 'Unknown'
            amount = row[revenue_col]
            
            brand_map = brand_hierarchy.setdefault(parent, {})
            child_map = brand_map.setdefault(child, {})
            child_map[brand] = child_map.get(brand, 0) + amount
        
        return total_amount, total_qty, hierarchy, brand_hierarchy
    
    return total_amount, total_qty, hierarchy

def _get_total_overview_summary_odoo_fallback(target_date_start: date, target_date_end: date) -> Dict:
    """Odoo fallback for overview summary if DuckDB fails."""
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
    
    result = _summaries_for_dataframe(today_df)
    
    # Handle both return formats (with and without brand data)
    if len(result) == 4:
        today_amount, today_qty, today_categories, today_brands = result
    else:
        today_amount, today_qty, today_categories = result
        today_brands = {}
    
    prev_amount, *_ = _summaries_for_dataframe(prev_df)

    return {
        'target_date_start': target_date_start,
        'target_date_end': target_date_end,
        'today_amount': float(today_amount),
        'today_qty': float(today_qty),
        'prev_amount': float(prev_amount),
        'categories_nested': today_categories,
        'brands_nested': today_brands,
    }


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

    try:
        # Use DuckDB for faster queries
        return query_overview_summary(target_date_start, target_date_end)
    except Exception as e:
        print(f"DuckDB query failed in get_total_overview_summary: {e}, falling back to Odoo")
        return _get_total_overview_summary_odoo_fallback(target_date_start, target_date_end)
