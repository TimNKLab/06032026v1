from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple
import pandas as pd
import gc

from odoorpc_connector import get_odoo_connection, retry_odoo

@retry_odoo(max_retries=3, delay=1)
def get_pos_order_lines_for_date(target_date):
    """Fetch POS order lines for the given date (00:00 -> +1 day) with category info."""
    if target_date is None:
        return []

    odoo = get_odoo_connection()
    if odoo is None or 'pos.order.line' not in odoo.env:
        return []

    start_dt = datetime.combine(target_date, datetime.min.time())
    end_dt = start_dt + timedelta(days=1)
    domain = [
        ('x_studio_order_date', '>=', start_dt.strftime('%Y-%m-%d %H:%M:%S')),
        ('x_studio_order_date', '<', end_dt.strftime('%Y-%m-%d %H:%M:%S')),
    ]
    # Optimized field selection - only fetch what we need
    fields = [
        'x_studio_order_date',
        'product_id',
        'qty',
        'price_subtotal_incl',
    ]

    try:
        PosOrderLine = odoo.env['pos.order.line']
        lines = PosOrderLine.search_read(domain, fields)
    except Exception as exc:
        print(f"Error fetching pos.order.line data: {exc}")
        return []

    # Batch fetch product categories to reduce API calls
    product_ids = {
        line['product_id'][0]
        for line in lines
        if isinstance(line.get('product_id'), (list, tuple)) and line['product_id']
    }

    category_by_product = {}
    if product_ids and 'product.product' in odoo.env:
        try:
            Product = odoo.env['product.product']
            products = Product.read(list(product_ids), ['categ_id'])
            category_by_product = {
                prod['id']: prod.get('categ_id')
                for prod in products
            }
        except Exception as exc:
            print(f"Error fetching product categories: {exc}")

    # Process lines in chunks to manage memory
    processed_lines = []
    chunk_size = 1000
    
    for i in range(0, len(lines), chunk_size):
        chunk = lines[i:i + chunk_size]
        processed_chunk = _process_lines_chunk(chunk, category_by_product)
        processed_lines.extend(processed_chunk)
        
        # Clear memory
        del chunk
        gc.collect()

    return processed_lines

def _process_lines_chunk(lines_chunk, category_by_product):
    """Process a chunk of POS lines to add category information."""
    for line in lines_chunk:
        product = line.get('product_id')
        product_id = product[0] if isinstance(product, (list, tuple)) and product else None
        categ_value = category_by_product.get(product_id)
        line['product_categ_id'] = categ_value

        # Odoo stores Many2one as (id, "Parent/Child"). Extract human-friendly parts.
        categ_name = None
        if isinstance(categ_value, (list, tuple)) and len(categ_value) >= 2:
            categ_name = categ_value[1]
        elif isinstance(categ_value, str):
            categ_name = categ_value

        parent_category = None
        leaf_category = None
        if isinstance(categ_name, str):
            segments = [segment.strip() for segment in categ_name.split('/') if segment.strip()]
            if segments:
                parent_category = segments[0]
                leaf_category = segments[-1]

        line['product_parent_category'] = parent_category
        line['product_category'] = leaf_category

    return lines_chunk

def get_pos_order_lines_batched(start_date, end_date, batch_days=7):
    """Fetch POS order lines in batches to handle large date ranges efficiently."""
    all_lines = []
    current = start_date
    
    while current <= end_date:
        batch_end = min(current + timedelta(days=batch_days-1), end_date)
        batch_lines = get_pos_order_lines_for_date_range(current, batch_end)
        all_lines.extend(batch_lines)
        current += timedelta(days=batch_days)
    
    return all_lines

def get_pos_order_lines_for_date_range(start_date, end_date):
    """Fetch POS order lines for a date range (more efficient than multiple single-day calls)."""
    if start_date is None or end_date is None:
        return []

    odoo = get_odoo_connection()
    if odoo is None or 'pos.order.line' not in odoo.env:
        return []

    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    
    domain = [
        ('x_studio_order_date', '>=', start_dt.strftime('%Y-%m-%d %H:%M:%S')),
        ('x_studio_order_date', '<=', end_dt.strftime('%Y-%m-%d %H:%M:%S')),
    ]
    
    fields = [
        'x_studio_order_date',
        'product_id',
        'qty',
        'price_subtotal_incl',
    ]

    try:
        PosOrderLine = odoo.env['pos.order.line']
        lines = PosOrderLine.search_read(domain, fields)
    except Exception as exc:
        print(f"Error fetching pos.order.line data for range: {exc}")
        return []

    # Process categories (same as single-day function)
    product_ids = {
        line['product_id'][0]
        for line in lines
        if isinstance(line.get('product_id'), (list, tuple)) and line['product_id']
    }

    category_by_product = {}
    if product_ids and 'product.product' in odoo.env:
        try:
            Product = odoo.env['product.product']
            products = Product.read(list(product_ids), ['categ_id'])
            category_by_product = {
                prod['id']: prod.get('categ_id')
                for prod in products
            }
        except Exception as exc:
            print(f"Error fetching product categories: {exc}")

    return _process_lines_chunk(lines, category_by_product)

def create_fact_dataframe(lines):
    """Create a pandas DataFrame simulating a fact table for optimized processing."""
    if not lines:
        return pd.DataFrame()
    
    df = pd.DataFrame(lines)
    
    # Extract date components for faster filtering
    df['order_date'] = pd.to_datetime(df['x_studio_order_date'])
    df['date_key'] = df['order_date'].dt.strftime('%Y%m%d').astype(int)
    
    # Extract product info
    df['product_id'] = df['product_id'].apply(lambda x: x[0] if isinstance(x, (list, tuple)) else None)
    
    # Convert to appropriate data types
    df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0)
    df['price_subtotal_incl'] = pd.to_numeric(df['price_subtotal_incl'], errors='coerce').fillna(0)
    
    return df
