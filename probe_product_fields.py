"""
Utility script to verify that the new product fields required by the ETL
(id, name, barcode, list price, pricelist fixed price, UoM, category, brand)
are accessible from the current Odoo environment.

Usage (with Odoo environment variables configured):

    python scripts\probe_product_fields.py --product-id 123

If --product-id is omitted, the script fetches the first available product
and prints the fields for inspection.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Iterable, Optional

from odoorpc import ODOO

from odoorpc_connector import get_odoo_connection, retry_odoo


PRODUCT_FIELDS = [
    "name",
    "barcode",
    "list_price",
    "uom_id",
    "categ_id",
    "x_studio_pricelist_rules_ids",
    "product_variant_id",
    "x_studio_brand_id",
]
PRODUCT_TEMPLATE_FIELDS = ["x_studio_brand_id"]
PRICELIST_FIELDS = ["fixed_price"]


class OdooFieldProbeError(RuntimeError):
    pass


def _format_m2o(value: Any) -> Dict[str, Optional[Any]]:
    if isinstance(value, (list, tuple)) and value:
        return {"id": value[0], "name": value[1] if len(value) > 1 else None}
    if isinstance(value, dict):
        return {"id": value.get("id"), "name": value.get("name")}
    if value is None:
        return {"id": None, "name": None}
    if isinstance(value, int):
        return {"id": value, "name": None}
    return {"id": None, "name": value}


def _first(iterable: Iterable[int]) -> Optional[int]:
    for item in iterable:
        if item is not None:
            return item
    return None


@retry_odoo(max_retries=3, delay=2)
def fetch_product(odoo: ODOO, product_id: Optional[int] = None) -> Dict[str, Any]:
    product_model = odoo.env["product.product"]

    if product_id is None:
        product_id = product_model.search([], limit=1)
        if not product_id:
            raise OdooFieldProbeError("No products found in Odoo.")
        if isinstance(product_id, list):
            product_id = product_id[0]

    products = product_model.read([product_id], PRODUCT_FIELDS)
    if not products:
        raise OdooFieldProbeError(f"Product with id {product_id} not found.")

    return products[0]


@retry_odoo(max_retries=3, delay=2)
def fetch_template_brand(odoo: ODOO, template_id: int) -> Optional[Any]:
    template_model = odoo.env["product.template"]
    templates = template_model.read([template_id], PRODUCT_TEMPLATE_FIELDS)
    if not templates:
        return None
    return templates[0].get("x_studio_brand_id")


@retry_odoo(max_retries=3, delay=2)
def fetch_pricelist_fixed_price(odoo: ODOO, rule_ids: Iterable[int]) -> Optional[float]:
    ids = [rid for rid in rule_ids if isinstance(rid, int)]
    if not ids:
        return None

    rule_model_name = None
    for candidate in ["product.pricelist.item", "product.pricelist.rule"]:
        if candidate in odoo.env:
            rule_model_name = candidate
            break

    if not rule_model_name:
        return None

    model = odoo.env[rule_model_name]
    rules = model.read(ids, PRICELIST_FIELDS)
    if not rules:
        return None

    first_rule = rules[0]
    return first_rule.get("fixed_price")


def probe_product_fields(product_id: Optional[int] = None) -> Dict[str, Any]:
    odoo = get_odoo_connection()
    if odoo is None:
        raise OdooFieldProbeError("Failed to connect to Odoo. Check environment variables.")

    product = fetch_product(odoo, product_id)

    result: Dict[str, Any] = {
        "id": product.get("id"),
        "name": product.get("name"),
        "barcode": product.get("barcode"),
        "list_price": product.get("list_price"),
    }

    uom = _format_m2o(product.get("uom_id"))
    result["uom_id"] = uom["id"]
    result["uom_name"] = uom["name"]

    categ = _format_m2o(product.get("categ_id"))
    result["categ_id"] = categ["id"]
    result["categ_name"] = categ["name"]

    brand_value = product.get("x_studio_brand_id")
    template = product.get("product_variant_id")
    template_brand = None
    if isinstance(template, (list, tuple)) and template:
        template_id = template[0]
        template_brand = fetch_template_brand(odoo, template_id)

    brand_candidate = template_brand or brand_value
    formatted_brand = _format_m2o(brand_candidate)
    result["product_brand_id"] = formatted_brand["id"]
    result["product_brand"] = formatted_brand["name"]

    rule_ids = product.get("x_studio_pricelist_rules_ids") or []
    fixed_price = fetch_pricelist_fixed_price(odoo, rule_ids) if rule_ids else None
    result["pricelist_fixed_price"] = fixed_price

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe Odoo product fields for ETL validation.")
    parser.add_argument(
        "--product-id",
        type=int,
        help="Specific product ID to probe. If omitted, the first product found will be used.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output result as JSON (default is pretty-printed dict).",
    )
    args = parser.parse_args()

    try:
        result = probe_product_fields(args.product_id)
    except OdooFieldProbeError as exc:
        parser.exit(status=1, message=f"Error probing Odoo fields: {exc}\n")

    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        for key, value in result.items():
            print(f"{key}: {value}")


if __name__ == "__main__":
    main()
