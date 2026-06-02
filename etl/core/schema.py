"""Pure I/O helpers and shared schemas for the ETL pipeline.

This module contains no Celery, no Dash, no business logic — only
filesystem utilities, Parquet I/O primitives, and schema definitions
used across extract/transform/load stages.
"""
import os
from datetime import date
from typing import Dict, Optional

import polars as pl

from etl.io_parquet import atomic_write_parquet


def has_parquet_files(path: str) -> bool:
    """Check whether a directory tree or single file contains any Parquet data."""
    if os.path.isdir(path):
        for _, _, files in os.walk(path):
            if any(name.endswith('.parquet') for name in files):
                return True
        return False
    return os.path.isfile(path) and path.endswith('.parquet')


def read_parquet_or_empty(path: str, schema: Dict[str, pl.DataType]) -> pl.DataFrame:
    """Read a single Parquet file or a hive-partitioned directory; return empty
    DataFrame with *schema* when no data is found."""
    if os.path.isfile(path):
        return pl.read_parquet(path)
    if has_parquet_files(path):
        return pl.read_parquet(f"{path}/**/*.parquet")
    return pl.DataFrame(schema=schema)


def partition_path(base_path: str, target_date: str) -> str:
    """Hive-partition directory for a single calendar day."""
    year, month, day = target_date.split('-')
    return f'{base_path}/year={year}/month={month}/day={day}'


def write_partitioned(
    df: pl.DataFrame,
    base_path: str,
    target_date: str,
    filename_prefix: str,
) -> str:
    """Write a DataFrame to a hive-partitioned daily file atomically."""
    dest = partition_path(base_path, target_date)
    os.makedirs(dest, exist_ok=True)
    output_file = f'{dest}/{filename_prefix}_{target_date}.parquet'
    atomic_write_parquet(df, output_file)
    return output_file


# ---------------------------------------------------------------------------
# Re-usable Polars schemas (kept here so they are discoverable in one place)
# ---------------------------------------------------------------------------

SALES_SCHEMA = {
    'date': pl.Date,
    'order_id': pl.Int64,
    'line_id': pl.Int64,
    'move_id': pl.Int64,
    'move_line_id': pl.Int64,
    'product_id': pl.Int64,
    'quantity': pl.Float64,
    'revenue': pl.Float64,
    'price_unit': pl.Float64,
    'tax_id': pl.Int64,
    'order_ref': pl.Utf8,
    'move_name': pl.Utf8,
}

COST_SCHEMA = {
    'date': pl.Date,
    'product_id': pl.Int64,
    'cost_unit_tax_in': pl.Float64,
    'source_move_id': pl.Int64,
    'source_tax_id': pl.Int64,
}

PROFIT_SCHEMA = {
    'date': pl.Date,
    'txn_id': pl.Int64,
    'line_id': pl.Int64,
    'product_id': pl.Int64,
    'quantity': pl.Float64,
    'revenue_tax_in': pl.Float64,
    'cost_unit_tax_in': pl.Float64,
    'cogs_tax_in': pl.Float64,
    'gross_profit': pl.Float64,
    'source_cost_move_id': pl.Int64,
    'source_cost_tax_id': pl.Int64,
}

DAILY_AGG_SCHEMA = {
    'date': pl.Date,
    'revenue_tax_in': pl.Float64,
    'cogs_tax_in': pl.Float64,
    'gross_profit': pl.Float64,
    'quantity': pl.Float64,
    'transactions': pl.Int64,
    'lines': pl.Int64,
}

BY_PRODUCT_AGG_SCHEMA = {
    'date': pl.Date,
    'product_id': pl.Int64,
    'revenue_tax_in': pl.Float64,
    'cogs_tax_in': pl.Float64,
    'gross_profit': pl.Float64,
    'quantity': pl.Float64,
    'lines': pl.Int64,
}
