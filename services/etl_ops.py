"""Dataset partition scanner and dimension file checker.

This module provides read-only scanning of Parquet partitions and dimension
files in the data lake. It is used by the Admin UI for monitoring ETL state.

IMPORTANT: This module has ZERO ETL/Odoo dependencies. It only reads Parquet
files from the filesystem — no Celery, no odoorpc, no etl_tasks imports.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional
import os

import polars as pl

from etl.config import (
    RAW_PATH,
    CLEAN_PATH,
    RAW_SALES_INVOICE_PATH,
    RAW_PURCHASES_PATH,
    RAW_INVENTORY_MOVES_PATH,
    RAW_STOCK_QUANTS_PATH,
    CLEAN_SALES_INVOICE_PATH,
    CLEAN_PURCHASES_PATH,
    CLEAN_INVENTORY_MOVES_PATH,
    CLEAN_STOCK_QUANTS_PATH,
    STAR_SCHEMA_PATH,
    DIM_PRODUCTS_FILE,
    DIM_LOCATIONS_FILE,
    DIM_UOMS_FILE,
    DIM_PARTNERS_FILE,
    DIM_USERS_FILE,
    DIM_COMPANIES_FILE,
    DIM_LOTS_FILE,
    FACT_PRODUCT_COST_EVENTS_PATH,
    FACT_PRODUCT_COST_LATEST_DAILY_PATH,
)


@dataclass(frozen=True)
class DatasetConfig:
    """Configuration for a single dataset partition tree.

    NOTE: The `task` field has been removed (was Celery task reference).
    ETL triggering is now handled by the scheduler process via admin/etl_queue.sqlite,
    not by importing Celery tasks directly.
    """
    key: str
    label: str
    raw_base: Optional[str]
    raw_filename: Optional[str]
    clean_base: Optional[str]
    clean_filename: Optional[str]
    fact_base: Optional[str]
    fact_filename: Optional[str]


DATASETS: Dict[str, DatasetConfig] = {
    "pos": DatasetConfig(
        key="pos",
        label="POS Sales",
        raw_base=RAW_PATH,
        raw_filename="pos_order_lines_{date}.parquet",
        clean_base=CLEAN_PATH,
        clean_filename="pos_order_lines_clean_{date}.parquet",
        fact_base=f"{STAR_SCHEMA_PATH}/fact_sales",
        fact_filename="fact_sales_{date}.parquet",
    ),
    "invoice_sales": DatasetConfig(
        key="invoice_sales",
        label="Invoice Sales",
        raw_base=RAW_SALES_INVOICE_PATH,
        raw_filename="account_move_out_invoice_lines_{date}.parquet",
        clean_base=CLEAN_SALES_INVOICE_PATH,
        clean_filename="account_move_out_invoice_lines_clean_{date}.parquet",
        fact_base=f"{STAR_SCHEMA_PATH}/fact_invoice_sales",
        fact_filename="fact_invoice_sales_{date}.parquet",
    ),
    "purchases": DatasetConfig(
        key="purchases",
        label="Purchase Invoices",
        raw_base=RAW_PURCHASES_PATH,
        raw_filename="account_move_in_invoice_lines_{date}.parquet",
        clean_base=CLEAN_PURCHASES_PATH,
        clean_filename="account_move_in_invoice_lines_clean_{date}.parquet",
        fact_base=f"{STAR_SCHEMA_PATH}/fact_purchases",
        fact_filename="fact_purchases_{date}.parquet",
    ),
    "inventory_moves": DatasetConfig(
        key="inventory_moves",
        label="Inventory Moves",
        raw_base=RAW_INVENTORY_MOVES_PATH,
        raw_filename="inventory_moves_{date}.parquet",
        clean_base=CLEAN_INVENTORY_MOVES_PATH,
        clean_filename="inventory_moves_clean_{date}.parquet",
        fact_base=f"{STAR_SCHEMA_PATH}/fact_inventory_moves",
        fact_filename="fact_inventory_moves_{date}.parquet",
    ),
    "stock_quants": DatasetConfig(
        key="stock_quants",
        label="Stock Quants",
        raw_base=RAW_STOCK_QUANTS_PATH,
        raw_filename="stock_quants_{date}.parquet",
        clean_base=CLEAN_STOCK_QUANTS_PATH,
        clean_filename="stock_quants_clean_{date}.parquet",
        fact_base=f"{STAR_SCHEMA_PATH}/fact_stock_on_hand_snapshot",
        fact_filename="fact_stock_on_hand_snapshot_{date}.parquet",
    ),
    "profit": DatasetConfig(
        key="profit",
        label="Profit (Cost + Aggregates)",
        raw_base=None,
        raw_filename=None,
        clean_base=None,
        clean_filename=None,
        fact_base=f"{STAR_SCHEMA_PATH}/agg_profit_daily",
        fact_filename="agg_profit_daily_{date}.parquet",
    ),
    "product_cost_events": DatasetConfig(
        key="product_cost_events",
        label="Product Cost Events",
        raw_base=None,
        raw_filename=None,
        clean_base=None,
        clean_filename=None,
        fact_base=FACT_PRODUCT_COST_EVENTS_PATH,
        fact_filename="fact_product_cost_events_{date}.parquet",
    ),
    "product_cost_latest": DatasetConfig(
        key="product_cost_latest",
        label="Product Cost Latest Daily",
        raw_base=None,
        raw_filename=None,
        clean_base=None,
        clean_filename=None,
        fact_base=FACT_PRODUCT_COST_LATEST_DAILY_PATH,
        fact_filename="fact_product_cost_latest_daily_{date}.parquet",
    ),
}

DIMENSION_FILES = {
    "Products": DIM_PRODUCTS_FILE,
    "Locations": DIM_LOCATIONS_FILE,
    "UOMs": DIM_UOMS_FILE,
    "Partners": DIM_PARTNERS_FILE,
    "Users": DIM_USERS_FILE,
    "Companies": DIM_COMPANIES_FILE,
    "Lots": DIM_LOTS_FILE,
}


def _partition_file(base_path: str, date_value: date, filename_template: str) -> str:
    """Build partition path: base_path/year=YYYY/month=MM/day=DD/filename"""
    year = f"{date_value.year:04d}"
    month = f"{date_value.month:02d}"
    day = f"{date_value.day:02d}"
    date_str = date_value.isoformat()
    partition_path = f"{base_path}/year={year}/month={month}/day={day}"
    filename = filename_template.format(date=date_str)
    return f"{partition_path}/{filename}"


def _date_range(start: date, end: date) -> Iterable[date]:
    """Generate dates from start to end (inclusive)."""
    if end < start:
        start, end = end, start
    delta = (end - start).days
    return (start + timedelta(days=offset) for offset in range(delta + 1))


def _count_parquet_rows(path: str) -> int:
    """Count rows in a Parquet file, returns 0 on any error."""
    try:
        return pl.scan_parquet(path).collect().height
    except Exception:
        return 0


def scan_dataset_partitions(dataset_key: str, start: date, end: date) -> List[Dict[str, object]]:
    """Scan partitions for a dataset between start and end dates.

    Returns a list of dicts with raw/clean/fact status and row counts per day.
    """
    config = DATASETS.get(dataset_key)
    if not config:
        return []

    results: List[Dict[str, object]] = []
    for day in _date_range(start, end):
        raw_path = _partition_file(config.raw_base, day, config.raw_filename) if config.raw_base else None
        clean_path = _partition_file(config.clean_base, day, config.clean_filename) if config.clean_base else None
        fact_path = _partition_file(config.fact_base, day, config.fact_filename) if config.fact_base else None

        raw_exists = os.path.exists(raw_path) if raw_path else False
        clean_exists = os.path.exists(clean_path) if clean_path else False
        fact_exists = os.path.exists(fact_path) if fact_path else False

        raw_rows = _count_parquet_rows(raw_path) if raw_exists else 0
        clean_rows = _count_parquet_rows(clean_path) if clean_exists else 0
        fact_rows = _count_parquet_rows(fact_path) if fact_exists else 0

        def _status(exists: bool, rows: int) -> str:
            if not exists:
                return "Missing"
            return "Empty" if rows == 0 else "OK"

        results.append({
            "date": day.isoformat(),
            "raw": _status(raw_exists, raw_rows),
            "clean": _status(clean_exists, clean_rows),
            "fact": _status(fact_exists, fact_rows),
            "raw_rows": raw_rows,
            "clean_rows": clean_rows,
            "fact_rows": fact_rows,
        })

    return results


def scan_dimension_files() -> List[Dict[str, object]]:
    """Check existence of all dimension Parquet files.

    Returns a list of dicts with dimension name, path, and existence flag.
    """
    results: List[Dict[str, object]] = []
    for name, path in DIMENSION_FILES.items():
        results.append({
            "dimension": name,
            "path": path,
            "exists": os.path.exists(path),
        })
    return results


def parse_date(value: object) -> Optional[date]:
    """Parse a value into a date object. Handles None, date, datetime, and ISO string."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None
