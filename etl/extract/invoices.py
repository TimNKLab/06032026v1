"""Invoice extraction implementations."""

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from etl.odoo_helpers import batch_ids, extract_o2m_ids, get_model_fields, safe_extract_m2o, safe_float
from etl.odoo_pool import get_pooled_odoo_connection

logger = logging.getLogger(__name__)


def extract_account_move_lines_impl(target_date: str, move_type: str, partner_role: str) -> Dict[str, Any]:
    """Shared extractor for account.move invoice lines."""
    target_dt = date.fromisoformat(target_date)

    with get_pooled_odoo_connection() as odoo:
        start_dt = datetime.combine(target_dt, datetime.min.time())
        end_dt = start_dt.replace(hour=23, minute=59, second=59)

        Move = odoo.env['account.move']
        MoveLine = odoo.env['account.move.line']
        if Move is None or MoveLine is None:
            logger.warning("Missing required Odoo models: account.move and/or account.move.line")
            return {'lines': [], 'target_date': target_date}

        domain = [
            ('date', '>=', start_dt.strftime('%Y-%m-%d')),
            ('date', '<=', end_dt.strftime('%Y-%m-%d')),
            ('move_type', '=', move_type),
            ('state', '=', 'posted'),
        ]

        move_fields = ['id', 'date', 'name', 'partner_id', 'invoice_line_ids']
        moves = Move.search_read(domain, move_fields)
        if not moves:
            logger.info(f"No account.move ({move_type}) found for {target_date}")
            return {'lines': [], 'target_date': target_date}

        line_ids: Set[int] = set()
        move_partner: Dict[int, Tuple[Optional[int], Optional[str]]] = {}
        move_dates: Dict[int, str] = {}
        move_names: Dict[int, str] = {}

        for mv in moves:
            mid = mv.get('id')
            if isinstance(mid, int):
                move_dates[mid] = mv.get('date')
                move_names[mid] = mv.get('name')
                partner_id = safe_extract_m2o(mv.get('partner_id'), get_id=True)
                partner_name = safe_extract_m2o(mv.get('partner_id'), get_id=False)
                move_partner[mid] = (partner_id, partner_name)
            for lid in extract_o2m_ids(mv.get('invoice_line_ids')):
                line_ids.add(lid)

        if not line_ids:
            logger.info(f"No invoice lines found for {target_date} ({move_type})")
            return {'lines': [], 'target_date': target_date}

        line_candidates = ['id', 'move_id', 'product_id', 'price_unit', 'quantity', 'tax_ids']
        if move_type == 'in_invoice':
            line_candidates.append('purchase_order_id')

        line_fields = get_model_fields(odoo, 'account.move.line', line_candidates)
        if 'id' not in line_fields:
            line_fields = ['id'] + [f for f in line_fields if f != 'id']

        processed_lines: List[Dict[str, Any]] = []

        for batch in batch_ids(line_ids):
            try:
                lines = MoveLine.read(batch, line_fields)
            except Exception as exc:
                logger.error(f"Error reading account.move.line batch: {exc}")
                continue

            for ln in lines:
                move_id_val = safe_extract_m2o(ln.get('move_id'))
                if not isinstance(move_id_val, int):
                    continue

                product_id = safe_extract_m2o(ln.get('product_id'))
                price_unit = safe_float(ln.get('price_unit'))
                quantity = safe_float(ln.get('quantity'))

                tax_ids = extract_o2m_ids(ln.get('tax_ids'))
                tax_ids_list = sorted(set([tid for tid in tax_ids if isinstance(tid, int)]))
                tax_id = tax_ids_list[0] if tax_ids_list else None
                tax_ids_json = json.dumps(tax_ids_list)

                partner_id, partner_name = move_partner.get(move_id_val, (None, None))
                purchase_order_id = None
                purchase_order_name = None
                if 'purchase_order_id' in line_fields:
                    purchase_order_id = safe_extract_m2o(ln.get('purchase_order_id'), get_id=True)
                    purchase_order_name = safe_extract_m2o(ln.get('purchase_order_id'), get_id=False)

                processed_lines.append({
                    'move_id': move_id_val,
                    'move_name': move_names.get(move_id_val),
                    'move_date': move_dates.get(move_id_val),
                    f'{partner_role}_id': partner_id,
                    f'{partner_role}_name': partner_name,
                    'purchase_order_id': purchase_order_id,
                    'purchase_order_name': purchase_order_name,
                    'move_line_id': ln.get('id'),
                    'product_id': product_id,
                    'price_unit': price_unit,
                    'quantity': quantity,
                    'tax_id': tax_id,
                    'tax_ids_json': tax_ids_json,
                })

        return {
            'lines': processed_lines,
            'target_date': target_date,
            'count': len(processed_lines),
            'move_type': move_type,
        }


def extract_sales_invoice_lines_impl(target_date: str) -> Dict[str, Any]:
    """Extract posted customer invoices (out_invoice) lines."""
    return extract_account_move_lines_impl(target_date, move_type='out_invoice', partner_role='customer')


def extract_purchase_invoice_lines_impl(target_date: str) -> Dict[str, Any]:
    """Extract posted vendor bills (in_invoice) lines."""
    return extract_account_move_lines_impl(target_date, move_type='in_invoice', partner_role='vendor')
