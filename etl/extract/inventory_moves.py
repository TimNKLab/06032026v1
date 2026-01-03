"""Inventory moves extraction implementation."""

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set

from etl.odoo_helpers import batch_ids, get_model_fields, safe_extract_m2o, safe_float
from etl.odoo_pool import get_pooled_odoo_connection

logger = logging.getLogger(__name__)


def _locations_internal_usage(odoo, location_ids: Set[int]) -> Dict[int, Dict[str, Any]]:
    """Get location usage info."""
    if not location_ids:
        return {}
    Location = odoo.env['stock.location']
    if Location is None:
        return {}

    fields = get_model_fields(odoo, 'stock.location', ['id', 'usage', 'scrap_location', 'name'])
    if 'id' not in fields:
        fields = ['id'] + [f for f in fields if f != 'id']

    results: Dict[int, Dict[str, Any]] = {}
    for batch in batch_ids(location_ids):
        try:
            recs = Location.read(batch, fields)
        except Exception as exc:
            logger.error(f"Error reading stock.location batch: {exc}")
            continue
        for rec in recs:
            lid = rec.get('id')
            if isinstance(lid, int):
                results[lid] = rec
    return results


def _picking_type_code_to_movement_type(code: Optional[str]) -> Optional[str]:
    """Convert picking type code to movement type."""
    if not isinstance(code, str):
        return None
    mapping = {
        'incoming': 'receipt',
        'outgoing': 'delivery',
        'internal': 'internal_transfer',
    }
    return mapping.get(code)


def extract_inventory_moves_impl(target_date: str) -> Dict[str, Any]:
    """Extract executed inventory moves (stock.move.line) for a target date."""

    target_dt = date.fromisoformat(target_date)

    with get_pooled_odoo_connection() as odoo:
        MoveLine = odoo.env['stock.move.line']
        Move = odoo.env['stock.move']
        Picking = odoo.env['stock.picking']
        PickingType = odoo.env['stock.picking.type']

        if MoveLine is None or Move is None:
            logger.warning("Missing required Odoo models: stock.move.line and/or stock.move")
            return {'lines': [], 'target_date': target_date, 'count': 0}

        start_dt = datetime.combine(target_dt, datetime.min.time())
        end_dt = start_dt.replace(hour=23, minute=59, second=59)

        date_field = 'date'
        try:
            meta = MoveLine.fields_get(['date', 'date_done'])
            if 'date' in meta:
                date_field = 'date'
            elif 'date_done' in meta:
                date_field = 'date_done'
        except Exception:
            date_field = 'date'

        domain = [
            (date_field, '>=', start_dt.strftime('%Y-%m-%d %H:%M:%S')),
            (date_field, '<=', end_dt.strftime('%Y-%m-%d %H:%M:%S')),
            ('qty_done', '!=', 0),
        ]
        try:
            domain.append(('move_id.state', '=', 'done'))
        except Exception:
            pass

        line_candidates = [
            'id', 'move_id', date_field, 'product_id', 'location_id',
            'location_dest_id', 'qty_done', 'product_uom_id', 'lot_id',
            'owner_id', 'picking_id', 'create_uid', 'create_date',
        ]
        line_fields = get_model_fields(odoo, 'stock.move.line', line_candidates)
        if 'id' not in line_fields:
            line_fields = ['id'] + [f for f in line_fields if f != 'id']

        try:
            move_lines = MoveLine.search_read(domain, line_fields)
        except Exception as exc:
            logger.error(f"Error fetching stock.move.line data: {exc}")
            return {'lines': [], 'target_date': target_date, 'count': 0}

        if not move_lines:
            logger.info(f"No stock.move.line found for {target_date}")
            return {'lines': [], 'target_date': target_date, 'count': 0}

        move_ids: Set[int] = set()
        picking_ids: Set[int] = set()
        location_ids: Set[int] = set()
        for ml in move_lines:
            mid = safe_extract_m2o(ml.get('move_id'))
            if isinstance(mid, int):
                move_ids.add(mid)
            pid = safe_extract_m2o(ml.get('picking_id'))
            if isinstance(pid, int):
                picking_ids.add(pid)
            src = safe_extract_m2o(ml.get('location_id'))
            dst = safe_extract_m2o(ml.get('location_dest_id'))
            if isinstance(src, int):
                location_ids.add(src)
            if isinstance(dst, int):
                location_ids.add(dst)

        move_candidates = [
            'id', 'name', 'reference', 'picking_id', 'picking_type_id',
            'origin', 'company_id', 'create_uid', 'create_date',
            'inventory_id', 'raw_material_production_id', 'production_id',
        ]
        move_fields = get_model_fields(odoo, 'stock.move', move_candidates)
        if 'id' not in move_fields:
            move_fields = ['id'] + [f for f in move_fields if f != 'id']

        moves_by_id: Dict[int, Dict[str, Any]] = {}
        if move_ids:
            for batch in batch_ids(move_ids):
                try:
                    recs = Move.read(batch, move_fields)
                except Exception as exc:
                    logger.error(f"Error reading stock.move batch: {exc}")
                    continue
                for rec in recs:
                    rid = rec.get('id')
                    if isinstance(rid, int):
                        moves_by_id[rid] = rec

        pickings_by_id: Dict[int, Dict[str, Any]] = {}
        if Picking is not None and picking_ids:
            picking_candidates = [
                'id', 'name', 'partner_id', 'picking_type_id', 'origin', 'company_id',
            ]
            picking_fields = get_model_fields(odoo, 'stock.picking', picking_candidates)
            if 'id' not in picking_fields:
                picking_fields = ['id'] + [f for f in picking_fields if f != 'id']

            for batch in batch_ids(picking_ids):
                try:
                    recs = Picking.read(batch, picking_fields)
                except Exception as exc:
                    logger.error(f"Error reading stock.picking batch: {exc}")
                    continue
                for rec in recs:
                    rid = rec.get('id')
                    if isinstance(rid, int):
                        pickings_by_id[rid] = rec

        picking_type_ids: Set[int] = set()
        for mv in moves_by_id.values():
            ptid = safe_extract_m2o(mv.get('picking_type_id'))
            if isinstance(ptid, int):
                picking_type_ids.add(ptid)
        for pk in pickings_by_id.values():
            ptid = safe_extract_m2o(pk.get('picking_type_id'))
            if isinstance(ptid, int):
                picking_type_ids.add(ptid)

        picking_type_by_id: Dict[int, Dict[str, Any]] = {}
        if PickingType is not None and picking_type_ids:
            pt_candidates = ['id', 'code', 'name']
            pt_fields = get_model_fields(odoo, 'stock.picking.type', pt_candidates)
            if 'id' not in pt_fields:
                pt_fields = ['id'] + [f for f in pt_fields if f != 'id']
            for batch in batch_ids(picking_type_ids):
                try:
                    recs = PickingType.read(batch, pt_fields)
                except Exception as exc:
                    logger.error(f"Error reading stock.picking.type batch: {exc}")
                    continue
                for rec in recs:
                    rid = rec.get('id')
                    if isinstance(rid, int):
                        picking_type_by_id[rid] = rec

        locations_by_id = _locations_internal_usage(odoo, location_ids)

        processed: List[Dict[str, Any]] = []
        for ml in move_lines:
            ml_id = ml.get('id')
            if not isinstance(ml_id, int):
                continue

            move_id = safe_extract_m2o(ml.get('move_id'))
            if not isinstance(move_id, int):
                continue

            mv = moves_by_id.get(move_id, {})
            picking_id = safe_extract_m2o(ml.get('picking_id'))
            if not isinstance(picking_id, int):
                picking_id = safe_extract_m2o(mv.get('picking_id'))

            picking = pickings_by_id.get(picking_id, {}) if isinstance(picking_id, int) else {}

            src_id = safe_extract_m2o(ml.get('location_id'))
            dst_id = safe_extract_m2o(ml.get('location_dest_id'))

            qty_done = safe_float(ml.get('qty_done'))
            if qty_done == 0:
                continue

            src_usage = None
            dst_usage = None
            src_scrap = False
            dst_scrap = False

            if isinstance(src_id, int):
                loc = locations_by_id.get(src_id, {})
                src_usage = loc.get('usage')
                src_scrap = bool(loc.get('scrap_location') or False)
            if isinstance(dst_id, int):
                loc = locations_by_id.get(dst_id, {})
                dst_usage = loc.get('usage')
                dst_scrap = bool(loc.get('scrap_location') or False)

            src_internal = src_usage == 'internal'
            dst_internal = dst_usage == 'internal'

            qty_moved = qty_done
            if src_internal and not dst_internal:
                qty_moved = -abs(qty_done)
            elif not src_internal and dst_internal:
                qty_moved = abs(qty_done)

            picking_type_id = safe_extract_m2o(picking.get('picking_type_id'))
            if not isinstance(picking_type_id, int):
                picking_type_id = safe_extract_m2o(mv.get('picking_type_id'))

            picking_type_code = None
            if isinstance(picking_type_id, int):
                picking_type_code = picking_type_by_id.get(picking_type_id, {}).get('code')

            movement_type = _picking_type_code_to_movement_type(picking_type_code)
            inventory_adjustment_flag = False

            picking_type_name = None
            if isinstance(picking_type_id, int):
                picking_type_name = picking_type_by_id.get(picking_type_id, {}).get('name')

            manufacturing_order_id = None
            raw_mo = safe_extract_m2o(mv.get('raw_material_production_id'))
            prod_mo = safe_extract_m2o(mv.get('production_id'))
            if isinstance(raw_mo, int):
                movement_type = 'manufacturing_consumption'
                manufacturing_order_id = raw_mo
            elif isinstance(prod_mo, int):
                movement_type = 'manufacturing_output'
                manufacturing_order_id = prod_mo

            if dst_scrap:
                movement_type = 'scrap'
            elif (src_usage == 'inventory' or dst_usage == 'inventory') and movement_type not in {
                'manufacturing_consumption', 'manufacturing_output',
            }:
                movement_type = 'adjustment'
                inventory_adjustment_flag = True

            if isinstance(picking_type_name, str) and 'return' in picking_type_name.lower():
                if movement_type == 'receipt':
                    movement_type = 'return_from_customer'
                elif movement_type == 'delivery':
                    movement_type = 'return_to_vendor'

            origin_reference = picking.get('origin') or mv.get('origin')
            reference = picking.get('name') or mv.get('reference') or mv.get('name') or origin_reference

            company_id = safe_extract_m2o(picking.get('company_id'))
            if not isinstance(company_id, int):
                company_id = safe_extract_m2o(mv.get('company_id'))

            partner_id = safe_extract_m2o(picking.get('partner_id'))

            source_partner_id = None
            destination_partner_id = None
            if isinstance(partner_id, int):
                if not src_internal and dst_internal:
                    source_partner_id = partner_id
                elif src_internal and not dst_internal:
                    destination_partner_id = partner_id

            create_uid = safe_extract_m2o(ml.get('create_uid'))
            if not isinstance(create_uid, int):
                create_uid = safe_extract_m2o(mv.get('create_uid'))

            create_date_val = ml.get('create_date') or mv.get('create_date')

            processed.append({
                'move_id': move_id,
                'move_line_id': ml_id,
                'movement_date': ml.get(date_field),
                'product_id': safe_extract_m2o(ml.get('product_id')),
                'location_src_id': src_id,
                'location_dest_id': dst_id,
                'qty_moved': qty_moved,
                'uom_id': safe_extract_m2o(ml.get('product_uom_id')),
                'movement_type': movement_type,
                'picking_id': picking_id,
                'picking_type_code': picking_type_code,
                'reference': reference,
                'origin_reference': origin_reference,
                'company_id': company_id,
                'lot_id': safe_extract_m2o(ml.get('lot_id')),
                'owner_id': safe_extract_m2o(ml.get('owner_id')),
                'source_partner_id': source_partner_id,
                'destination_partner_id': destination_partner_id,
                'cost_per_unit': None,
                'inventory_adjustment_flag': inventory_adjustment_flag,
                'manufacturing_order_id': manufacturing_order_id,
                'created_by_user': create_uid,
                'create_date': create_date_val,
            })

        return {
            'lines': processed,
            'target_date': target_date,
            'count': len(processed),
        }
