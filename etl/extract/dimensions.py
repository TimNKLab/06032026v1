"""Dimension extraction: Build/update dimension parquet files used for enrichment.

No Celery decorators. No framework dependencies beyond stdlib, Polars, and Odoo RPC.
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

import polars as pl

from etl.config import (
    DIM_COMPANIES_FILE,
    DIM_LOCATIONS_FILE,
    DIM_LOTS_FILE,
    DIM_PARTNERS_FILE,
    DIM_PRODUCTS_FILE,
    DIM_UOMS_FILE,
    DIM_USERS_FILE,
)
from etl.io_parquet import atomic_write_parquet
from etl.odoo_pool import get_pooled_odoo_connection
from etl.odoo_helpers import get_model_fields, read_all_records, safe_extract_m2o

logger = logging.getLogger(__name__)


class DimensionLoader:
    """Caches and manages dimension loading."""
    _cache = {}

    @classmethod
    def clear_cache(cls):
        cls._cache.clear()


def refresh_dimensions_incremental(targets: Optional[List[str]] = None) -> Dict[str, Any]:
    """Build/update dimension parquet files used for enrichment."""
    try:
        target_list = targets or [
            'products', 'locations', 'uoms', 'partners', 'users', 'companies', 'lots',
        ]

        results: Dict[str, int] = {}
        now = datetime.now()

        # Clear dimension cache before refresh
        DimensionLoader.clear_cache()

        with get_pooled_odoo_connection() as odoo:
            if 'products' in target_list:
                Product = odoo.env['product.product']
                if Product is not None:
                    fields = get_model_fields(
                        odoo,
                        'product.product',
                        ['id', 'name', 'categ_id', 'x_studio_brand_id', 'barcode', 'default_code'],
                    )
                    records = read_all_records(odoo, 'product.product', fields)
                    rows = []
                    for prod in records:
                        pid = prod.get('id')
                        if not isinstance(pid, int):
                            continue
                        categ_val = prod.get('categ_id')
                        categ_name = safe_extract_m2o(categ_val, get_id=False)
                        parent_category = None
                        leaf_category = None
                        if isinstance(categ_name, str):
                            parts = [p.strip() for p in categ_name.split('/') if p.strip()]
                            if parts:
                                parent_category = parts[0]
                                leaf_category = parts[-1]
                        brand_val = prod.get('x_studio_brand_id')
                        rows.append({
                            'product_id': pid,
                            'product_name': prod.get('name'),
                            'product_category': leaf_category,
                            'product_parent_category': parent_category,
                            'product_brand': safe_extract_m2o(brand_val, get_id=False) or 'Unknown',
                            'product_brand_id': safe_extract_m2o(brand_val, get_id=True),
                            'product_barcode': prod.get('barcode'),
                            'product_sku': prod.get('default_code'),
                        })
                    if rows:
                        atomic_write_parquet(pl.DataFrame(rows), DIM_PRODUCTS_FILE)
                    results['products'] = len(rows)

            if 'locations' in target_list and 'stock.location' in odoo.env:
                fields = get_model_fields(odoo, 'stock.location', ['id', 'complete_name', 'name', 'usage', 'scrap_location'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = read_all_records(odoo, 'stock.location', fields)
                rows = []
                for loc in records:
                    lid = loc.get('id')
                    if not isinstance(lid, int):
                        continue
                    rows.append({
                        'location_id': lid,
                        'location_name': loc.get('complete_name') or loc.get('name'),
                        'location_usage': loc.get('usage'),
                        'scrap_location': bool(loc.get('scrap_location') or False),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_LOCATIONS_FILE)
                results['locations'] = len(rows)

            if 'uoms' in target_list and 'uom.uom' in odoo.env:
                fields = get_model_fields(odoo, 'uom.uom', ['id', 'name', 'category_id'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = read_all_records(odoo, 'uom.uom', fields)
                rows = []
                for uom in records:
                    uid = uom.get('id')
                    if not isinstance(uid, int):
                        continue
                    rows.append({
                        'uom_id': uid,
                        'uom_name': uom.get('name'),
                        'uom_category': safe_extract_m2o(uom.get('category_id'), get_id=False),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_UOMS_FILE)
                results['uoms'] = len(rows)

            if 'partners' in target_list and 'res.partner' in odoo.env:
                fields = get_model_fields(odoo, 'res.partner', ['id', 'name', 'ref', 'email', 'phone', 'is_company'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = read_all_records(odoo, 'res.partner', fields)
                rows = []
                for partner in records:
                    pid = partner.get('id')
                    if not isinstance(pid, int):
                        continue
                    rows.append({
                        'partner_id': pid,
                        'partner_name': partner.get('name'),
                        'partner_ref': partner.get('ref'),
                        'partner_email': partner.get('email'),
                        'partner_phone': partner.get('phone'),
                        'is_company': bool(partner.get('is_company') or False),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_PARTNERS_FILE)
                results['partners'] = len(rows)

            if 'users' in target_list and 'res.users' in odoo.env:
                fields = get_model_fields(odoo, 'res.users', ['id', 'name', 'partner_id', 'login'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = read_all_records(odoo, 'res.users', fields)
                rows = []
                for user in records:
                    uid = user.get('id')
                    if not isinstance(uid, int):
                        continue
                    rows.append({
                        'user_id': uid,
                        'user_name': user.get('name'),
                        'user_login': user.get('login'),
                        'partner_id': safe_extract_m2o(user.get('partner_id'), get_id=True),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_USERS_FILE)
                results['users'] = len(rows)

            if 'companies' in target_list and 'res.company' in odoo.env:
                fields = get_model_fields(odoo, 'res.company', ['id', 'name', 'partner_id'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = read_all_records(odoo, 'res.company', fields)
                rows = []
                for comp in records:
                    cid = comp.get('id')
                    if not isinstance(cid, int):
                        continue
                    rows.append({
                        'company_id': cid,
                        'company_name': comp.get('name'),
                        'partner_id': safe_extract_m2o(comp.get('partner_id'), get_id=True),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_COMPANIES_FILE)
                results['companies'] = len(rows)

            if 'lots' in target_list and 'stock.lot' in odoo.env:
                fields = get_model_fields(odoo, 'stock.lot', ['id', 'name', 'product_id'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = read_all_records(odoo, 'stock.lot', fields)
                rows = []
                for lot in records:
                    lid = lot.get('id')
                    if not isinstance(lid, int):
                        continue
                    rows.append({
                        'lot_id': lid,
                        'lot_name': lot.get('name'),
                        'product_id': safe_extract_m2o(lot.get('product_id'), get_id=True),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_LOTS_FILE)
                results['lots'] = len(rows)

        for dim in results.keys():
            ETLMetadata.set_dimension_last_sync(dim, now)

        return {'updated': True, 'targets': results}
    except Exception as exc:
        logger.error(f"Error refreshing dimensions: {exc}", exc_info=True)
        return {'updated': False, 'error': str(exc)}
