"""Stock quant snapshot extraction."""

import logging
from datetime import date
from typing import Any, Dict, List

from etl.odoo_helpers import get_model_fields, safe_extract_m2o, safe_float
from etl.odoo_pool import get_pooled_odoo_connection

logger = logging.getLogger(__name__)


def extract_stock_quants_impl(target_date: str) -> Dict[str, Any]:
    """Extract stock.quant snapshot for the target date."""
    target_dt = date.fromisoformat(target_date)

    with get_pooled_odoo_connection() as odoo:
        try:
            Quant = odoo.env['stock.quant']
        except Exception:
            logger.warning("Missing required Odoo model: stock.quant")
            return {'lines': [], 'target_date': target_date, 'count': 0}

        candidates = [
            'id', 'product_id', 'location_id', 'quantity', 'reserved_quantity',
            'lot_id', 'owner_id', 'company_id'
        ]
        fields = get_model_fields(odoo, 'stock.quant', candidates)
        if 'id' not in fields:
            fields = ['id'] + [f for f in fields if f != 'id']

        domain = []
        if 'quantity' in fields:
            domain.append(('quantity', '!=', 0))

        try:
            quants = Quant.search_read(domain, fields)
        except Exception as exc:
            logger.error(f"Error fetching stock.quant data: {exc}")
            return {'lines': [], 'target_date': target_date, 'count': 0}

        if not quants:
            logger.info(f"No stock.quant data found for snapshot {target_date}")
            return {'lines': [], 'target_date': target_date, 'count': 0}

        processed: List[Dict[str, Any]] = []
        for qt in quants:
            quant_id = qt.get('id')
            if not isinstance(quant_id, int):
                continue

            processed.append({
                'quant_id': quant_id,
                'snapshot_date': target_dt.isoformat(),
                'product_id': safe_extract_m2o(qt.get('product_id')),
                'location_id': safe_extract_m2o(qt.get('location_id')),
                'lot_id': safe_extract_m2o(qt.get('lot_id')),
                'owner_id': safe_extract_m2o(qt.get('owner_id')),
                'company_id': safe_extract_m2o(qt.get('company_id')),
                'quantity': safe_float(qt.get('quantity')),
                'reserved_quantity': safe_float(qt.get('reserved_quantity')),
            })

        return {
            'lines': processed,
            'target_date': target_date,
            'count': len(processed),
        }
