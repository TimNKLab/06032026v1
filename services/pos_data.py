from datetime import datetime, timedelta

from odoorpc_connector import get_odoo_connection


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
    fields = [
        'x_studio_order_date',
        'product_id',
        'x_studio_brand_id',
        'qty',
        'price_unit',
        'price_subtotal_incl',
    ]

    try:
        PosOrderLine = odoo.env['pos.order.line']
        lines = PosOrderLine.search_read(domain, fields)
    except Exception as exc:
        print(f"Error fetching pos.order.line data: {exc}")
        return []

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

    for line in lines:
        product = line.get('product_id')
        product_id = product[0] if isinstance(product, (list, tuple)) and product else None
        line['product_categ_id'] = category_by_product.get(product_id)

    return lines
