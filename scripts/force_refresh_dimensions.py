"""Force-refresh dimension tables into the data lake.

This script bypasses the incremental update logic and performs a full fetch of
all dimension tables from Odoo, saving the results into the configured
DATA_LAKE_ROOT (default: /app/data-lake).

Usage (run from project root):
    docker-compose exec celery-worker python scripts/force_refresh_dimensions.py

You can optionally limit the refresh to specific dimensions:
    docker-compose exec celery-worker python scripts/force_refresh_dimensions.py \
        --targets products categories
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence

import polars as pl

from odoorpc_connector import OdooConnectionManager
from etl_tasks import (
    atomic_write_parquet,
    batch_ids,
    format_m2m,
    format_m2o,
    safe_extract_m2o,
)

logger = logging.getLogger("force_refresh_dimensions")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _normalise_root(path: str) -> str:
    """Ensure we use an absolute path that exists inside the container."""
    if not path:
        return "/app/data-lake"
    # If a Windows drive path was provided (e.g. D:\\data-lake), fall back to
    # the mounted container path.
    if ":" in path:
        return "/app/data-lake"
    return os.path.abspath(path)


def _build_paths() -> Dict[str, str]:
    data_lake_root = _normalise_root(os.environ.get("DATA_LAKE_ROOT", "/app/data-lake"))
    star_schema_path = os.path.join(data_lake_root, "star-schema")
    metadata_path = os.path.join(data_lake_root, "metadata")

    os.makedirs(star_schema_path, exist_ok=True)
    os.makedirs(metadata_path, exist_ok=True)

    return {
        "data_lake_root": data_lake_root,
        "star_schema_path": star_schema_path,
        "metadata_file": os.path.join(metadata_path, "dimension_sync.json"),
    }


# ---------------------------------------------------------------------------
# Odoo helpers
# ---------------------------------------------------------------------------

def _read_all(model, fields: Sequence[str], domain: Optional[Sequence] = None, batch_size: int = 500) -> List[Dict]:
    domain = list(domain or [])
    ids = model.search(domain)
    if not ids:
        return []

    records: List[Dict] = []
    for chunk in batch_ids(set(ids), batch_size=batch_size):
        records.extend(model.read(chunk, list(fields)))
    return records


# ---------------------------------------------------------------------------
# Dimension loaders
# ---------------------------------------------------------------------------

def _load_products(odoo, star_schema_path: str) -> int:
    logger.info("Fetching products...")
    Product = odoo.env["product.product"]
    fields = ["id", "name", "categ_id", "x_studio_brand_id"]
    products = _read_all(Product, fields, domain=[["sale_ok", "=", True]])

    if not products:
        logger.warning("No products returned from Odoo")
        return 0

    rows: List[Dict] = []
    for prod in products:
        categ_value = prod.get("categ_id")
        categ_name = safe_extract_m2o(categ_value, get_id=False)

        parent_category = None
        leaf_category = None
        if isinstance(categ_name, str):
            parts = [p.strip() for p in categ_name.split("/") if p.strip()]
            if parts:
                parent_category = parts[0]
                leaf_category = parts[-1]

        brand_value = prod.get("x_studio_brand_id")
        rows.append({
            "product_id": prod["id"],
            "product_name": prod.get("name"),
            "product_category": leaf_category,
            "product_parent_category": parent_category,
            "product_brand": safe_extract_m2o(brand_value, get_id=False) or "Unknown",
            "product_brand_id": safe_extract_m2o(brand_value),
        })

    df = pl.DataFrame(rows)
    output_path = os.path.join(star_schema_path, "dim_products.parquet")
    atomic_write_parquet(df, output_path)
    logger.info("Wrote %s products -> %s", len(rows), output_path)
    return len(rows)


def _load_categories(odoo, star_schema_path: str) -> int:
    logger.info("Fetching categories...")
    Category = odoo.env["product.category"]
    fields = ["id", "name", "parent_id"]
    categories = _read_all(Category, fields)

    if not categories:
        logger.warning("No categories returned from Odoo")
        return 0

    rows = [
        {
            "category_id": cat["id"],
            "category_name": cat.get("name"),
            "parent_category_id": safe_extract_m2o(cat.get("parent_id")),
        }
        for cat in categories
    ]

    df = pl.DataFrame(rows)
    output_path = os.path.join(star_schema_path, "dim_categories.parquet")
    atomic_write_parquet(df, output_path)
    logger.info("Wrote %s categories -> %s", len(rows), output_path)
    return len(rows)


def _load_brands(odoo, star_schema_path: str) -> int:
    logger.info("Fetching brands...")
    if "x_product_brand" not in odoo.env:
        logger.warning("Model x_product_brand not found; skipping brands")
        return 0

    Brand = odoo.env["x_product_brand"]
    fields = [
        "id",
        "x_name",
        "x_studio_parent_brand_id",
        "x_studio_partner_id",
        "x_studio_entities_ids",
        "write_date",
    ]
    brands = _read_all(Brand, fields)

    if not brands:
        logger.warning("No brands returned from Odoo")
        return 0

    rows: List[Dict] = []
    for brand in brands:
        parent = format_m2o(brand.get("x_studio_parent_brand_id"))
        principal = format_m2o(brand.get("x_studio_partner_id"))
        entities = format_m2m(brand.get("x_studio_entities_ids", []))

        rows.append({
            "brand_id": brand["id"],
            "brand_name": brand.get("x_name"),
            "parent_brand_id": parent["id"],
            "parent_brand_name": parent["name"],
            "principal_id": principal["id"],
            "principal_name": principal["name"],
            "entity_ids": [entity["id"] for entity in entities],
            "entity_names": [entity["name"] for entity in entities],
            "write_date": brand.get("write_date"),
        })

    df = pl.DataFrame(rows)
    output_path = os.path.join(star_schema_path, "dim_brands.parquet")
    atomic_write_parquet(df, output_path)
    logger.info("Wrote %s brands -> %s", len(rows), output_path)
    return len(rows)


def _load_cashiers(odoo, star_schema_path: str) -> int:
    logger.info("Fetching cashiers...")
    if "hr.employee" not in odoo.env:
        logger.warning("Model hr.employee not found; skipping cashiers")
        return 0

    Employee = odoo.env["hr.employee"]
    domain = [["job_id", "in", ["Cashier", "Team Leader"]], ["active", "=", True]]
    fields = ["id", "name", "job_id"]
    employees = _read_all(Employee, fields, domain=domain)

    if not employees:
        logger.warning("No cashier records returned from Odoo")
        return 0

    rows = [
        {
            "id": emp["id"],
            "name": emp.get("name"),
            "job_id": safe_extract_m2o(emp.get("job_id"), get_id=False),
        }
        for emp in employees
    ]

    df = pl.DataFrame(rows)
    output_path = os.path.join(star_schema_path, "dim_cashiers.parquet")
    atomic_write_parquet(df, output_path)
    logger.info("Wrote %s cashiers -> %s", len(rows), output_path)
    return len(rows)


def _load_taxes(odoo, star_schema_path: str) -> int:
    logger.info("Fetching taxes...")
    if "account.tax" not in odoo.env:
        logger.warning("Model account.tax not found; skipping taxes")
        return 0

    Tax = odoo.env["account.tax"]
    fields = ["id", "name", "write_date"]
    taxes = _read_all(Tax, fields)

    if not taxes:
        logger.warning("No tax records returned from Odoo")
        return 0

    rows = [
        {
            "tax_id": tax["id"],
            "tax_name": tax.get("name"),
            "write_date": tax.get("write_date"),
        }
        for tax in taxes
        if tax and isinstance(tax.get("id"), int)
    ]

    df = pl.DataFrame(rows)
    output_path = os.path.join(star_schema_path, "dim_taxes.parquet")
    atomic_write_parquet(df, output_path)
    logger.info("Wrote %s taxes -> %s", len(rows), output_path)
    return len(rows)


DIMENSION_LOADERS = {
    "products": _load_products,
    "categories": _load_categories,
    "brands": _load_brands,
    "cashiers": _load_cashiers,
    "taxes": _load_taxes,
}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Force refresh Odoo dimensions into the data lake")
    parser.add_argument(
        "--targets",
        nargs="*",
        choices=sorted(DIMENSION_LOADERS.keys()),
        help="Subset of dimensions to refresh (default: all)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    paths = _build_paths()

    targets: Iterable[str]
    if args.targets:
        targets = args.targets
    else:
        targets = DIMENSION_LOADERS.keys()

    manager = OdooConnectionManager()
    odoo = manager.get_connection()

    metadata_updates: Dict[str, str] = {}
    results: Dict[str, int] = {}

    for target in targets:
        loader = DIMENSION_LOADERS.get(target)
        if not loader:
            continue
        try:
            count = loader(odoo, paths["star_schema_path"])
            results[target] = count
            metadata_updates[target] = datetime.now().isoformat()
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Failed to refresh %s: %s", target, exc)

    # Merge metadata updates
    existing_metadata: Dict[str, str] = {}
    if os.path.exists(paths["metadata_file"]):
        try:
            with open(paths["metadata_file"], "r", encoding="utf-8") as fh:
                existing_metadata = json.load(fh)
        except json.JSONDecodeError:
            logger.warning("Existing metadata file is invalid JSON; overwriting")

    existing_metadata.update(metadata_updates)
    with open(paths["metadata_file"], "w", encoding="utf-8") as fh:
        json.dump(existing_metadata, fh, indent=2, ensure_ascii=False)

    logger.info("Refreshed dimensions: %s", results)
    logger.info("Metadata written to %s", paths["metadata_file"])
    print(json.dumps({"results": results, "metadata_file": paths["metadata_file"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
