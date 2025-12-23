import duckdb
import os
from datetime import date, timedelta
from typing import Dict, List, Optional
import pandas as pd

class DuckDBManager:
    _instance = None
    _connection = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_connection(self):
        if self._connection is None:
            self._connection = duckdb.connect(':memory:', access_mode='READ_ONLY')
            self._setup_views()
        return self._connection
    
    def _setup_views(self):
        """Setup DuckDB views for star schema tables."""
        conn = self._connection
        
        # Paths to star schema data
        data_lake_path = os.environ.get('DATA_LAKE_PATH', '/app/data-lake')
        fact_path = f"{data_lake_path}/star-schema/fact_sales"
        dim_products_path = f"{data_lake_path}/star-schema/dim_products.parquet"
        dim_categories_path = f"{data_lake_path}/star-schema/dim_categories.parquet"
        dim_brands_path = f"{data_lake_path}/star-schema/dim_brands.parquet"
        
        # Create views for fact and dimension tables
        try:
            # Fact sales view (partitioned by date)
            conn.execute(f"""
                CREATE OR REPLACE VIEW fact_sales AS
                SELECT 
                    date,
                    product_id,
                    quantity,
                    revenue
                FROM read_parquet('{fact_path}/*.parquet')
            """)
            
            # Dim products view (single file)
            conn.execute(f"""
                CREATE OR REPLACE VIEW dim_products AS
                SELECT 
                    product_id,
                    product_category,
                    product_parent_category,
                    product_brand
                FROM read_parquet('{dim_products_path}')
            """)
            
            # Dim categories view (single file)
            conn.execute(f"""
                CREATE OR REPLACE VIEW dim_categories AS
                SELECT 
                    product_category,
                    product_parent_category
                FROM read_parquet('{dim_categories_path}')
            """)
            
            # Dim brands view (single file)
            conn.execute(f"""
                CREATE OR REPLACE VIEW dim_brands AS
                SELECT 
                    product_brand
                FROM read_parquet('{dim_brands_path}')
            """)
            
            print("DuckDB views created successfully")
            
        except Exception as e:
            print(f"Error setting up DuckDB views: {e}")
            # Fallback to empty views
            for view_name in ['fact_sales', 'dim_products', 'dim_categories', 'dim_brands']:
                conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * WHERE 1=0")

def get_duckdb_connection():
    """Get a reusable DuckDB connection."""
    return DuckDBManager().get_connection()

def query_sales_trends(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Query sales trends using DuckDB."""
    conn = get_duckdb_connection()
    
    # Date formatting for DuckDB
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Period grouping
    if period == 'daily':
        date_expr = "date"
    elif period == 'weekly':
        date_expr = "date_trunc('week', date)"
    elif period == 'monthly':
        date_expr = "date_trunc('month', date)"
    else:
        raise ValueError("Period must be 'daily', 'weekly', or 'monthly'")
    
    query = f"""
    SELECT 
        {date_expr} as date,
        SUM(revenue) as revenue,
        COUNT(DISTINCT date) as transactions,
        SUM(revenue) / COUNT(DISTINCT date) as avg_transaction_value
    FROM fact_sales
    WHERE date BETWEEN '{start_str}' AND '{end_str}'
    GROUP BY {date_expr}
    ORDER BY date
    """
    
    try:
        result = conn.execute(query).fetchdf()
        return result
    except Exception as e:
        print(f"Error querying sales trends: {e}")
        return pd.DataFrame(columns=['date', 'revenue', 'transactions', 'avg_transaction_value'])

def query_hourly_sales_pattern(target_date: date) -> pd.DataFrame:
    """Query hourly sales pattern using DuckDB."""
    conn = get_duckdb_connection()
    
    date_str = target_date.strftime('%Y-%m-%d')
    
    query = f"""
    SELECT 
        EXTRACT(HOUR FROM date) as hour,
        SUM(revenue) as revenue,
        COUNT(DISTINCT date) as transactions
    FROM fact_sales
    WHERE DATE(date) = '{date_str}'
      AND EXTRACT(HOUR FROM date) BETWEEN 7 AND 23
    GROUP BY EXTRACT(HOUR FROM date)
    ORDER BY hour
    """
    
    try:
        result = conn.execute(query).fetchdf()
        
        # Fill missing hours (7-23) with zeros
        all_hours = pd.DataFrame({'hour': range(7, 24)})
        result = all_hours.merge(result, on='hour', how='left').fillna(0)
        
        return result
    except Exception as e:
        print(f"Error querying hourly sales pattern: {e}")
        return pd.DataFrame(columns=['hour', 'revenue', 'transactions'])

def query_top_products(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Query top products by revenue using DuckDB."""
    conn = get_duckdb_connection()
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    query = f"""
    SELECT 
        p.product_id,
        p.product_brand as brand,
        p.product_category as category,
        COALESCE(SUM(f.quantity), 0) as quantity_sold,
        COALESCE(SUM(f.revenue), 0) as total_revenue
    FROM fact_sales f
    LEFT JOIN dim_products p ON f.product_id = p.product_id
    WHERE f.date BETWEEN '{start_str}' AND '{end_str}'
    GROUP BY p.product_id, p.product_brand, p.product_category
    ORDER BY total_revenue DESC
    LIMIT {limit}
    """
    
    try:
        result = conn.execute(query).fetchdf()
        
        # Generate product names
        result['product_name'] = 'Product ' + result['product_id'].astype(str)
        
        # Handle missing categories
        result['category'] = result['category'].fillna('Unknown Category')
        
        return result[['product_name', 'category', 'quantity_sold', 'total_revenue']]
    except Exception as e:
        print(f"Error querying top products: {e}")
        return pd.DataFrame(columns=['product_name', 'category', 'quantity_sold', 'total_revenue'])

def query_revenue_comparison(start_date: date, end_date: date) -> Dict:
    """Compare revenue between current and previous periods."""
    conn = get_duckdb_connection()
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Current period
    current_query = f"""
    SELECT 
        SUM(revenue) as revenue,
        COUNT(DISTINCT date) as transactions
    FROM fact_sales
    WHERE date BETWEEN '{start_str}' AND '{end_str}'
    """
    
    # Previous period (same length)
    period_length = (end_date - start_date).days + 1
    prev_start = start_date - timedelta(days=period_length)
    prev_end = start_date - timedelta(days=1)
    
    prev_start_str = prev_start.strftime('%Y-%m-%d')
    prev_end_str = prev_end.strftime('%Y-%m-%d')
    
    previous_query = f"""
    SELECT 
        SUM(revenue) as revenue,
        COUNT(DISTINCT date) as transactions
    FROM fact_sales
    WHERE date BETWEEN '{prev_start_str}' AND '{prev_end_str}'
    """
    
    try:
        current_result = conn.execute(current_query).fetchone()
        previous_result = conn.execute(previous_query).fetchone()
        
        current_revenue, current_transactions = current_result or (0, 0)
        prev_revenue, prev_transactions = previous_result or (0, 0)
        
        current_avg_atv = current_revenue / current_transactions if current_transactions > 0 else 0
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
    except Exception as e:
        print(f"Error querying revenue comparison: {e}")
        return {
            'current': {'revenue': 0, 'transactions': 0, 'avg_transaction_value': 0},
            'previous': {'revenue': 0, 'transactions': 0, 'avg_transaction_value': 0},
            'deltas': {'revenue': 0, 'revenue_pct': 0, 'transactions': 0, 'transactions_pct': 0, 'avg_transaction_value': 0, 'avg_transaction_value_pct': 0}
        }

def query_overview_summary(start_date: date, end_date: date) -> Dict:
    """Get overview summary using DuckDB."""
    conn = get_duckdb_connection()
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Current period summary
    summary_query = f"""
    SELECT 
        SUM(f.revenue) as revenue,
        SUM(f.quantity) as quantity
    FROM fact_sales f
    WHERE f.date BETWEEN '{start_str}' AND '{end_str}'
    """
    
    # Category hierarchy
    category_query = f"""
    SELECT 
        p.product_parent_category,
        p.product_category,
        SUM(f.revenue) as revenue
    FROM fact_sales f
    LEFT JOIN dim_products p ON f.product_id = p.product_id
    WHERE f.date BETWEEN '{start_str}' AND '{end_str}'
    GROUP BY p.product_parent_category, p.product_category
    """
    
    # Brand hierarchy
    brand_query = f"""
    SELECT 
        p.product_parent_category,
        p.product_category,
        p.product_brand,
        SUM(f.revenue) as revenue
    FROM fact_sales f
    LEFT JOIN dim_products p ON f.product_id = p.product_id
    WHERE f.date BETWEEN '{start_str}' AND '{end_str}'
    GROUP BY p.product_parent_category, p.product_category, p.product_brand
    """
    
    try:
        # Get summary
        summary_result = conn.execute(summary_query).fetchone()
        current_revenue, current_quantity = summary_result or (0, 0)
        
        # Get categories
        category_results = conn.execute(category_query).fetchdf()
        categories_nested = {}
        for _, row in category_results.iterrows():
            parent = row['product_parent_category'] or 'Unknown'
            child = row['product_category'] or 'Unknown'
            amount = row['revenue']
            
            child_map = categories_nested.setdefault(parent, {})
            child_map[child] = child_map.get(child, 0) + amount
        
        # Get brands
        brand_results = conn.execute(brand_query).fetchdf()
        brands_nested = {}
        for _, row in brand_results.iterrows():
            parent = row['product_parent_category'] or 'Unknown'
            child = row['product_category'] or 'Unknown'
            brand = row['product_brand'] or 'Unknown'
            amount = row['revenue']
            
            brand_map = brands_nested.setdefault(parent, {})
            child_map = brand_map.setdefault(child, {})
            child_map[brand] = child_map.get(brand, 0) + amount
        
        return {
            'target_date_start': start_date,
            'target_date_end': end_date,
            'today_amount': float(current_revenue),
            'today_qty': float(current_quantity),
            'prev_amount': 0,  # Would need additional query for previous period
            'categories_nested': categories_nested,
            'brands_nested': brands_nested,
        }
    except Exception as e:
        print(f"Error querying overview summary: {e}")
        return {
            'target_date_start': start_date,
            'target_date_end': end_date,
            'today_amount': 0,
            'today_qty': 0,
            'prev_amount': 0,
            'categories_nested': {},
            'brands_nested': {},
        }
