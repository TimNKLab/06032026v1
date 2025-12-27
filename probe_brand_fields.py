"""
Script to probe the x_product_brand model in Odoo and display its structure and sample data.

Usage:
    python scripts/probe_brand_fields.py [--brand-id BRAND_ID] [--limit N] [--json]

Options:
    --brand-id BRAND_ID  Specific brand ID to probe
    --limit N            Maximum number of brands to fetch (default: 5)
    --json               Output as JSON
"""

import argparse
import json
from typing import Any, Dict, List, Optional

from odoorpc_connector import get_odoo_connection, retry_odoo


# Define the fields we want to fetch from x_product_brand
BRAND_FIELDS = [
    "id",
    "x_name",
    "x_studio_parent_brand_id",
    "x_studio_partner_id",
    "x_studio_entities_ids",
    "write_date",  # For checking data freshness
]


def _format_m2o(value: Any) -> Dict[str, Any]:
    """Format many2one field value."""
    if isinstance(value, (list, tuple)) and value:
        return {"id": value[0], "name": value[1] if len(value) > 1 else None}
    if isinstance(value, dict):
        return {"id": value.get("id"), "name": value.get("name")}
    if value is None:
        return {"id": None, "name": None}
    if isinstance(value, int):
        return {"id": value, "name": None}
    return {"id": None, "name": value}


def _format_m2m(value: Any) -> List[Dict[str, Any]]:
    """Format many2many field value."""
    if isinstance(value, (list, tuple)) and value:
        if all(isinstance(x, (list, tuple)) and len(x) >= 2 for x in value):
            return [{"id": x[0], "name": x[1]} for x in value if x and x[0] is not None]
    if isinstance(value, list) and value and isinstance(value[0], int):
        return [{"id": x, "name": None} for x in value if x is not None]
    return []


@retry_odoo(max_retries=3, delay=2)
def fetch_brand(odoo, brand_id: Optional[int] = None, limit: int = 5) -> List[Dict[str, Any]]:
    """Fetch brand data from Odoo."""
    brand_model = odoo.env["x_product_brand"]
    
    domain = []
    if brand_id:
        domain = [("id", "=", brand_id)]
    
    # First check if the model exists
    if "x_product_brand" not in odoo.env:
        raise RuntimeError("x_product_brand model not found in Odoo")
    
    # Check fields
    model_fields = brand_model.fields_get(BRAND_FIELDS)
    
    # Build the fields to read (only those that exist)
    fields_to_read = [f for f in BRAND_FIELDS if f in model_fields]
    
    # Add any missing fields to the result as None
    missing_fields = [f for f in BRAND_FIELDS if f not in model_fields]
    
    # Fetch the data
    brand_ids = brand_model.search(domain, limit=limit)
    if not brand_ids:
        return []
    
    brands = brand_model.read(brand_ids, fields_to_read)
    
    # Process the results
    results = []
    for brand in brands:
        result = {
            "brand_id": brand.get("id"),
            "brand_name": brand.get("x_name"),
            "parent_brand": _format_m2o(brand.get("x_studio_parent_brand_id")),
            "principal": _format_m2o(brand.get("x_studio_partner_id")),
            "entities": _format_m2m(brand.get("x_studio_entities_ids", [])),
            "write_date": brand.get("write_date"),
        }
        
        # Add missing fields as None
        for field in missing_fields:
            result[field] = None
        
        results.append(result)
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Probe x_product_brand model in Odoo")
    parser.add_argument("--brand-id", type=int, help="Specific brand ID to probe")
    parser.add_argument("--limit", type=int, default=5, help="Maximum number of brands to fetch")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()
    
    try:
        odoo = get_odoo_connection()
        if odoo is None:
            print("Failed to connect to Odoo. Check your environment variables.")
            return 1
        
        brands = fetch_brand(odoo, args.brand_id, args.limit)
        
        if not brands:
            print("No brands found.")
            return 0
        
        if args.json:
            print(json.dumps(brands, indent=2, ensure_ascii=False))
        else:
            for i, brand in enumerate(brands, 1):
                print(f"\n=== Brand {i} ===")
                print(f"ID: {brand['brand_id']}")
                print(f"Name: {brand['brand_name']}")
                print(f"Parent Brand: {brand['parent_brand']['name']} (ID: {brand['parent_brand']['id']})")
                print(f"Principal: {brand['principal']['name']} (ID: {brand['principal']['id']})")
                print("Entities:")
                for entity in brand['entities']:
                    print(f"  - {entity['name']} (ID: {entity['id']})")
                print(f"Last Updated: {brand['write_date']}")
            print(f"\nFound {len(brands)} brand(s).")
        
        return 0
    
    except Exception as e:
        print(f"Error: {str(e)}")
        if not args.json:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())