from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple
import pandas as pd
import gc
import json

from odoorpc_connector import get_odoo_connection, retry_odoo

def _extract_m2o_id(value):
    if isinstance(value, (list, tuple)) and value:
        return value[0]
    if isinstance(value, int):
        return value
    return None

def _extract_o2m_ids(value) -> List[int]:
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [v for v in value if isinstance(v, int)]
    return []

@retry_odoo(max_retries=3, delay=1)
def get_pos_order_lines_for_date(target_date):
    """Fetch POS order lines derived from pos.order for the given date (00:00 -> +1 day)."""
    if target_date is None:
        return []

    odoo = get_odoo_connection()
    if odoo is None or 'pos.order' not in odoo.env or 'pos.order.line' not in odoo.env:
        return []

    start_dt = datetime.combine(target_date, datetime.min.time())
    end_dt = start_dt + timedelta(days=1)
    domain = [
        ('date_order', '>=', start_dt.strftime('%Y-%m-%d %H:%M:%S')),
        ('date_order', '<', end_dt.strftime('%Y-%m-%d %H:%M:%S')),
    ]

    order_fields = [
        'date_order',
        'config_id',
        'employee_id',
        'partner_id',
        'name',
        'amount_total',
        'lines',
        'payments_id',
    ]

    try:
        PosOrder = odoo.env['pos.order']
        orders = PosOrder.search_read(domain, order_fields)
    except Exception as exc:
        print(f"Error fetching pos.order data: {exc}")
        return []

    if not orders:
        return []

    order_ids = set()
    line_ids = set()
    payment_ids = set()
    payment_id_to_order_id = {}

    for order in orders:
        oid = order.get('id')
        if isinstance(oid, int):
            order_ids.add(oid)
        for lid in _extract_o2m_ids(order.get('lines')):
            line_ids.add(lid)
        for pid in _extract_o2m_ids(order.get('payments_id')):
            payment_ids.add(pid)
            if isinstance(oid, int):
                payment_id_to_order_id[pid] = oid

    if not line_ids:
        return []

    # Read lines
    try:
        PosOrderLine = odoo.env['pos.order.line']
        line_fields = ['id', 'order_id', 'product_id', 'qty', 'price_subtotal_incl', 'x_studio_discount_amount']
        line_recs = PosOrderLine.read(list(line_ids), line_fields)
    except Exception as exc:
        print(f"Error reading pos.order.line records: {exc}")
        return []

    lines_by_order = {}
    product_ids = set()
    for line in line_recs:
        oid = _extract_m2o_id(line.get('order_id'))
        if not isinstance(oid, int):
            continue
        lines_by_order.setdefault(oid, []).append(line)
        pid = _extract_m2o_id(line.get('product_id'))
        if isinstance(pid, int):
            product_ids.add(pid)

    # Read payments
    payment_method_ids_by_order = {}
    if payment_ids and 'pos.payment' in odoo.env:
        try:
            PaymentModel = odoo.env['pos.payment']
            pay_recs = PaymentModel.read(list(payment_ids), ['id', 'amount', 'payment_method_id'])
            for pay in pay_recs:
                pay_id = pay.get('id')
                if not isinstance(pay_id, int):
                    continue
                oid = payment_id_to_order_id.get(pay_id)
                if not isinstance(oid, int):
                    continue
                try:
                    amount = float(pay.get('amount') or 0)
                except Exception:
                    amount = 0.0
                if amount <= 0:
                    continue
                mid = _extract_m2o_id(pay.get('payment_method_id'))
                if isinstance(mid, int):
                    payment_method_ids_by_order.setdefault(oid, []).append(mid)
        except Exception as exc:
            print(f"Error reading pos.payment records: {exc}")

    payment_method_ids_json_by_order = {}
    for oid in order_ids:
        mids = payment_method_ids_by_order.get(oid, [])
        payment_method_ids_json_by_order[oid] = json.dumps(sorted(set([m for m in mids if isinstance(m, int)])))

    # Batch fetch product categories and brands to reduce API calls
    category_by_product = {}
    brand_by_product = {}
    if product_ids and 'product.product' in odoo.env:
        try:
            Product = odoo.env['product.product']
            # Fetch both category and brand information
            products = Product.read(list(product_ids), ['categ_id', 'x_studio_brand_id'])
            category_by_product = {
                prod['id']: prod.get('categ_id')
                for prod in products
            }
            brand_by_product = {
                prod['id']: prod.get('x_studio_brand_id')
                for prod in products
            }
        except Exception as exc:
            print(f"Error fetching product categories/brands: {exc}")

    processed_lines = []
    for order in orders:
        oid = order.get('id')
        if not isinstance(oid, int):
            continue

        order_lines = lines_by_order.get(oid, [])
        if not order_lines:
            continue

        for line in order_lines:
            product = line.get('product_id')
            product_id = product[0] if isinstance(product, (list, tuple)) and product else None
            if product_id is None:
                continue

            try:
                qty = float(line.get('qty') or 0)
            except Exception:
                qty = 0.0
            try:
                price_subtotal_incl = float(line.get('price_subtotal_incl') or 0)
            except Exception:
                price_subtotal_incl = 0.0
            try:
                discount_amount = float(line.get('x_studio_discount_amount') or 0)
            except Exception:
                discount_amount = 0.0

            processed_lines.append({
                'order_date': order.get('date_order'),
                'order_id': oid,
                'order_ref': order.get('name'),
                'pos_config_id': _extract_m2o_id(order.get('config_id')),
                'cashier_id': _extract_m2o_id(order.get('employee_id')),
                'customer_id': _extract_m2o_id(order.get('partner_id')),
                'amount_total': float(order.get('amount_total') or 0),
                'payment_method_ids': payment_method_ids_json_by_order.get(oid, '[]'),
                'line_id': line.get('id'),
                'product_id': product_id,
                'qty': qty,
                'price_subtotal_incl': price_subtotal_incl,
                'discount_amount': discount_amount,
                'product_categ_id': category_by_product.get(product_id),
                'x_studio_brand_id': brand_by_product.get(product_id),
            })

    # Enrich with parent/category and brand name fields
    processed_lines = _process_lines_chunk(processed_lines, category_by_product, brand_by_product)
    return processed_lines

def _process_lines_chunk(lines_chunk, category_by_product, brand_by_product=None):
    """Process a chunk of POS lines to add category and brand information."""
    if brand_by_product is None:
        brand_by_product = {}
        
    for line in lines_chunk:
        product = line.get('product_id')
        if isinstance(product, int):
            product_id = product
        else:
            product_id = product[0] if isinstance(product, (list, tuple)) and product else None
        categ_value = category_by_product.get(product_id)
        brand_value = brand_by_product.get(product_id)
        
        line['product_categ_id'] = categ_value
        line['x_studio_brand_id'] = brand_value

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
        
        # Extract brand name
        brand_name = None
        if isinstance(brand_value, (list, tuple)) and len(brand_value) >= 2:
            brand_name = brand_value[1]
        elif isinstance(brand_value, str):
            brand_name = brand_value
        
        line['product_brand'] = brand_name or 'Unknown'

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

    all_lines = []
    current = start_date
    while current <= end_date:
        day_lines = get_pos_order_lines_for_date(current)
        if day_lines:
            all_lines.extend(day_lines)
        current += timedelta(days=1)
    return all_lines

def create_fact_dataframe(lines):
    """Create a pandas DataFrame simulating a fact table for optimized processing."""
    if not lines:
        return pd.DataFrame()
    
    df = pd.DataFrame(lines)
    
    # Extract date components for faster filtering
    if 'order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    elif 'x_studio_order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['x_studio_order_date'], errors='coerce')
    else:
        df['order_date'] = pd.NaT
    df['date_key'] = pd.to_numeric(df['order_date'].dt.strftime('%Y%m%d'), errors='coerce').fillna(0).astype(int)
    
    # Extract product info
    if 'product_id' in df.columns:
        df['product_id'] = df['product_id'].apply(lambda x: x[0] if isinstance(x, (list, tuple)) else x)

    if 'order_id' in df.columns:
        df['order_id'] = df['order_id'].apply(lambda x: x[0] if isinstance(x, (list, tuple)) else x)
    
    # Convert to appropriate data types
    df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0)
    df['price_subtotal_incl'] = pd.to_numeric(df.get('price_subtotal_incl', 0), errors='coerce').fillna(0)
    df['discount_amount'] = pd.to_numeric(df.get('discount_amount', 0), errors='coerce').fillna(0)
    
    return df
