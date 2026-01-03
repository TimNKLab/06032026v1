"""POS order lines extraction implementation."""
import json
from datetime import datetime, date
from typing import Dict, List, Set, Any

from etl.odoo_helpers import (
    batch_ids, safe_extract_m2o, safe_float, extract_o2m_ids,
    get_model_fields, read_all_records,
)
from etl.cache import get_redis_client, cache_get, cache_set
from etl.config import CACHE_TTL


def batch_read_products(odoo, product_ids: Set[int]) -> Dict[int, Dict]:
    """Batch read products with bulk caching."""
    if not product_ids:
        return {}

    redis = get_redis_client()
    cache_keys = [f'etl:cache:product:{pid}' for pid in product_ids]

    # Bulk cache lookup
    try:
        cached_values = redis.mget(cache_keys)
        cached_data = {}
        uncached_ids = set()

        for pid, cached in zip(product_ids, cached_values):
            if cached:
                cached_data[pid] = json.loads(cached)
            else:
                uncached_ids.add(pid)
    except Exception:
        uncached_ids = product_ids
        cached_data = {}

    if not uncached_ids:
        return cached_data

    # Fetch uncached products
    Product = odoo.env['product.product']
    fields = ['name', 'categ_id', 'x_studio_brand_id']

    product_data = {}
    cache_pipeline = redis.pipeline()

    for batch in batch_ids(uncached_ids):
        try:
            products = Product.read(batch, fields)
            for prod in products:
                categ_value = prod.get('categ_id')
                categ_name = safe_extract_m2o(categ_value, get_id=False)

                parent_category = None
                leaf_category = None
                if isinstance(categ_name, str):
                    segments = [s.strip() for s in categ_name.split('/') if s.strip()]
                    if segments:
                        parent_category = segments[0]
                        leaf_category = segments[-1]

                brand_value = prod.get('x_studio_brand_id')

                prod_info = {
                    'name': prod.get('name'),
                    'category': leaf_category,
                    'parent_category': parent_category,
                    'brand_name': safe_extract_m2o(brand_value, get_id=False) or 'Unknown',
                    'brand_id': safe_extract_m2o(brand_value, get_id=True),
                }

                product_data[prod['id']] = prod_info

                cache_pipeline.setex(
                    f'etl:cache:product:{prod["id"]}',
                    CACHE_TTL,
                    json.dumps(prod_info)
                )

        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error reading product batch: {e}")

    # Execute bulk cache insert
    try:
        cache_pipeline.execute()
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Cache bulk insert error: {e}")

    return {**cached_data, **product_data}


def extract_pos_order_lines_impl(target_date: str) -> Dict[str, Any]:
    """Extract POS order lines with optimized batched API calls."""
    from etl.odoo_pool import get_pooled_odoo_connection
    import logging
    logger = logging.getLogger(__name__)
    
    target_dt = date.fromisoformat(target_date)

    with get_pooled_odoo_connection() as odoo:
        start_dt = datetime.combine(target_dt, datetime.min.time())
        end_dt = start_dt.replace(hour=23, minute=59, second=59)

        PosOrder = odoo.env['pos.order']
        PosOrderLine = odoo.env['pos.order.line']

        if PosOrder is None or PosOrderLine is None:
            logger.warning("Missing required Odoo models: pos.order and/or pos.order.line")
            return {'lines': [], 'target_date': target_date}

        domain = [
            ('date_order', '>=', start_dt.strftime('%Y-%m-%d %H:%M:%S')),
            ('date_order', '<=', end_dt.strftime('%Y-%m-%d %H:%M:%S')),
        ]

        order_fields = [
            'date_order', 'config_id', 'employee_id', 'partner_id',
            'name', 'amount_total', 'lines', 'payment_ids',
        ]

        orders = PosOrder.search_read(domain, order_fields)

        if not orders:
            logger.info(f"No pos.order found for {target_date}")
            return {'lines': [], 'target_date': target_date}

        order_ids: Set[int] = set()
        line_ids: Set[int] = set()
        payment_ids: Set[int] = set()
        payment_id_to_order_id: Dict[int, int] = {}

        for order in orders:
            if isinstance(order.get('id'), int):
                order_ids.add(order['id'])

            for lid in extract_o2m_ids(order.get('lines')):
                line_ids.add(lid)

            for pid in extract_o2m_ids(order.get('payment_ids')):
                payment_ids.add(pid)
                if isinstance(order.get('id'), int):
                    payment_id_to_order_id[pid] = order['id']

        if not line_ids:
            logger.info(f"No pos.order.line IDs found for {target_date}")
            return {'lines': [], 'target_date': target_date}

        # Read all lines
        line_fields = ['id', 'order_id', 'product_id', 'qty', 'price_subtotal_incl', 'x_studio_discount_amount']
        lines_by_order: Dict[int, List[Dict[str, Any]]] = {}
        product_ids: Set[int] = set()

        for batch in batch_ids(line_ids):
            try:
                line_recs = PosOrderLine.read(batch, line_fields)
            except Exception as e:
                logger.error(f"Error reading pos.order.line batch: {e}")
                continue

            for line in line_recs:
                order_id = safe_extract_m2o(line.get('order_id'))
                if not isinstance(order_id, int):
                    continue
                lines_by_order.setdefault(order_id, []).append(line)

                product_id = safe_extract_m2o(line.get('product_id'))
                if isinstance(product_id, int):
                    product_ids.add(product_id)

        if not product_ids:
            return {'lines': [], 'target_date': target_date}

        # Read payments
        payment_method_ids_by_order: Dict[int, List[int]] = {}
        PaymentModel = odoo.env['pos.payment']
        if payment_ids and PaymentModel is not None:
            payment_fields = ['id', 'amount', 'payment_method_id']
            for batch in batch_ids(payment_ids):
                try:
                    payment_recs = PaymentModel.read(batch, payment_fields)
                except Exception as e:
                    logger.error(f"Error reading pos.payment batch: {e}")
                    continue

                for pay in payment_recs:
                    pay_id = pay.get('id')
                    if not isinstance(pay_id, int):
                        continue
                    order_id = payment_id_to_order_id.get(pay_id)
                    if not isinstance(order_id, int):
                        continue
                    amount = safe_float(pay.get('amount'))
                    if amount <= 0:
                        continue
                    method_id = safe_extract_m2o(pay.get('payment_method_id'))
                    if isinstance(method_id, int):
                        payment_method_ids_by_order.setdefault(order_id, []).append(method_id)

        payment_method_ids_json_by_order: Dict[int, str] = {}
        for oid in order_ids:
            method_ids = payment_method_ids_by_order.get(oid, [])
            method_ids = sorted(set([m for m in method_ids if isinstance(m, int)]))
            payment_method_ids_json_by_order[oid] = json.dumps(method_ids)

        # Batch read product enrichment
        product_data = batch_read_products(odoo, product_ids)

        # Produce line-grain rows
        processed_lines: List[Dict[str, Any]] = []
        for order in orders:
            order_id = order.get('id')
            if not isinstance(order_id, int):
                continue

            order_lines = lines_by_order.get(order_id, [])
            if not order_lines:
                continue

            pos_config_id = safe_extract_m2o(order.get('config_id'))
            cashier_id = safe_extract_m2o(order.get('employee_id'))
            customer_id = safe_extract_m2o(order.get('partner_id'))
            order_ref = order.get('name')
            amount_total = safe_float(order.get('amount_total'))
            payment_method_ids = payment_method_ids_json_by_order.get(order_id, '[]')

            for line in order_lines:
                product_id = safe_extract_m2o(line.get('product_id'))
                if not isinstance(product_id, int):
                    continue

                product = product_data.get(product_id, {})

                processed_lines.append({
                    'order_date': order.get('date_order'),
                    'order_id': order_id,
                    'order_ref': order_ref,
                    'pos_config_id': pos_config_id,
                    'cashier_id': cashier_id,
                    'customer_id': customer_id,
                    'amount_total': amount_total,
                    'payment_method_ids': payment_method_ids,
                    'line_id': line.get('id'),
                    'product_id': product_id,
                    'qty': safe_float(line.get('qty')),
                    'price_subtotal_incl': safe_float(line.get('price_subtotal_incl')),
                    'discount_amount': safe_float(line.get('x_studio_discount_amount')),
                    'product_brand': product.get('brand_name', 'Unknown'),
                    'product_brand_id': product.get('brand_id'),
                    'product_name': product.get('name'),
                    'product_category': product.get('category'),
                    'product_parent_category': product.get('parent_category'),
                })

        return {
            'lines': processed_lines,
            'target_date': target_date,
            'count': len(processed_lines)
        }
