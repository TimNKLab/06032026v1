import os
import json
import logging
import time
import threading
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple, Iterator
from enum import Enum
from contextlib import contextmanager
from dataclasses import dataclass
import polars as pl
from pydantic import BaseModel, Field
from celery import Celery, group, chord, chain
from celery.exceptions import Ignore
from celery.schedules import crontab
from odoorpc_connector import get_odoo_connection, retry_odoo

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

os.makedirs('/app/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/app/logs/etl.log')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CELERY CONFIGURATION
# ============================================================================

redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
app = Celery('etl_tasks', broker=redis_url, backend=redis_url)

app.conf.update(
    timezone=os.environ.get('TZ', 'Asia/Jakarta'),
    enable_utc=False,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    result_expires=3600,
    task_time_limit=1800,
    task_soft_time_limit=1500,
)

# ============================================================================
# CONFIG IMPORTS (re-exported for backward compatibility)
# ============================================================================

from etl.config import (
    MAX_RETRIES, RETRY_DELAY, ODOO_BATCH_SIZE, PARQUET_COMPRESSION,
    CONNECTION_TIMEOUT, CACHE_TTL, DATA_LAKE_ROOT, RAW_PATH, CLEAN_PATH,
    RAW_SALES_INVOICE_PATH, RAW_PURCHASES_PATH, CLEAN_SALES_INVOICE_PATH,
    CLEAN_PURCHASES_PATH, RAW_INVENTORY_MOVES_PATH, CLEAN_INVENTORY_MOVES_PATH,
    STAR_SCHEMA_PATH, METADATA_PATH, DIM_PRODUCTS_FILE, DIM_LOCATIONS_FILE,
    DIM_UOMS_FILE, DIM_PARTNERS_FILE, DIM_USERS_FILE, DIM_COMPANIES_FILE,
    DIM_LOTS_FILE,
)

# ============================================================================
# HELPER FUNCTIONS (re-exported for backward compatibility)
# ============================================================================

from etl.odoo_helpers import (
    safe_float, safe_int, batch_ids, safe_extract_m2o, format_m2o,
    format_m2m, extract_o2m_ids, get_model_fields, read_all_records,
)

from etl.io_parquet import atomic_write_parquet

# ============================================================================
# INFRASTRUCTURE IMPORTS (re-exported for backward compatibility)
# ============================================================================

from etl.odoo_pool import get_pooled_odoo_connection
from etl.cache import get_redis_client, cache_get, cache_set, cache_delete
from etl.metadata import ETLMetadata
from etl.dimension_cache import DimensionLoader

# ============================================================================
# ENUMS AND MODELS
# ============================================================================

class ETLStatus(str, Enum):
    PENDING = 'PENDING'
    EXTRACTING = 'EXTRACTING'
    TRANSFORMING = 'TRANSFORMING'
    LOADING = 'LOADING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


class ETLTaskStatus(BaseModel):
    task_id: str
    status: ETLStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    records_processed: int = 0
    error: Optional[str] = None
    metadata: Dict[str, Any] = {}

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def to_local_datetime(col_name: str) -> pl.Expr:
    """Convert UTC datetime string to local timezone."""
    return (
        pl.col(col_name)
        .cast(pl.Utf8, strict=False)
        .str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S', strict=False)
        .dt.replace_time_zone('UTC')
        .dt.convert_time_zone(app.conf.timezone)
        .dt.replace_time_zone(None)
    )


# ============================================================================
# EXTRACTION TASKS
# ============================================================================

@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_pos_order_lines(self, target_date: str) -> Dict[str, Any]:
    """Extract POS order lines with optimized batched API calls."""
    from etl.extract.pos import extract_pos_order_lines_impl
    return extract_pos_order_lines_impl(target_date)


@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_sales_invoice_lines(self, target_date: str) -> Dict[str, Any]:
    """Extract posted customer invoices (out_invoice) lines."""
    from etl.extract.invoices import extract_sales_invoice_lines_impl
    return extract_sales_invoice_lines_impl(target_date)


@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_purchase_invoice_lines(self, target_date: str) -> Dict[str, Any]:
    """Extract posted vendor bills (in_invoice) lines."""
    from etl.extract.invoices import extract_purchase_invoice_lines_impl
    return extract_purchase_invoice_lines_impl(target_date)


@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_inventory_moves(self, target_date: str) -> Dict[str, Any]:
    """Extract executed inventory moves (stock.move.line) for a target date."""
    from etl.extract.inventory_moves import extract_inventory_moves_impl
    return extract_inventory_moves_impl(target_date)


# ============================================================================
# SAVE RAW DATA TASKS
# ============================================================================

@app.task
def save_raw_data(extraction_result: Dict[str, Any]) -> Optional[str]:
    """Save raw POS order line extraction result to partitioned parquet."""
    try:
        lines = extraction_result.get('lines', [])
        target_date = extraction_result.get('target_date')

        if not target_date:
            logger.warning("Missing target_date in extraction result")
            return None

        year, month, day = target_date.split('-')
        partition_path = f'{RAW_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(partition_path, exist_ok=True)

        raw_schema = {
            'order_date': pl.Utf8,
            'order_id': pl.Int64,
            'order_ref': pl.Utf8,
            'pos_config_id': pl.Int64,
            'cashier_id': pl.Int64,
            'customer_id': pl.Int64,
            'amount_total': pl.Float64,
            'payment_method_ids': pl.Utf8,
            'line_id': pl.Int64,
            'product_id': pl.Int64,
            'qty': pl.Float64,
            'price_subtotal_incl': pl.Float64,
            'discount_amount': pl.Float64,
            'product_brand': pl.Utf8,
            'product_brand_id': pl.Int64,
            'product_name': pl.Utf8,
            'product_category': pl.Utf8,
            'product_parent_category': pl.Utf8,
        }

        if not lines:
            logger.info(f"No data for {target_date} (pos_order_lines)")
            df = pl.DataFrame(schema=raw_schema)
        else:
            normalized = [
                {k: row.get(k) for k in raw_schema.keys()}
                for row in lines if isinstance(row, dict)
            ]
            df = pl.DataFrame(normalized, schema_overrides=raw_schema, strict=False)
            df = df.with_columns([
                pl.col('payment_method_ids').fill_null('[]'),
                pl.col('discount_amount').fill_null(0),
            ])

        output_file = f'{partition_path}/pos_order_lines_{target_date}.parquet'
        atomic_write_parquet(df, output_file)
        logger.info(f"Saved {len(lines)} records to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error saving raw POS for {extraction_result.get('target_date')}: {e}", exc_info=True)
        return None


def _save_raw_account_move_lines(extraction_result: Dict[str, Any], raw_base_path: str, dataset_prefix: str) -> Optional[str]:
    """Save raw account.move lines to partitioned parquet."""
    try:
        lines = extraction_result.get('lines', [])
        target_date = extraction_result['target_date']

        year, month, day = target_date.split('-')
        partition_path = f'{raw_base_path}/year={year}/month={month}/day={day}'
        os.makedirs(partition_path, exist_ok=True)

        raw_schema = {
            'move_id': pl.Int64,
            'move_name': pl.Utf8,
            'move_date': pl.Utf8,
            'customer_id': pl.Int64,
            'customer_name': pl.Utf8,
            'vendor_id': pl.Int64,
            'vendor_name': pl.Utf8,
            'purchase_order_id': pl.Int64,
            'purchase_order_name': pl.Utf8,
            'move_line_id': pl.Int64,
            'product_id': pl.Int64,
            'price_unit': pl.Float64,
            'quantity': pl.Float64,
            'tax_id': pl.Int64,
            'tax_ids_json': pl.Utf8,
        }

        if not lines:
            logger.info(f"No data for {target_date} ({dataset_prefix})")
            df = pl.DataFrame(schema=raw_schema)
        else:
            normalized = [
                {k: row.get(k) for k in raw_schema.keys()}
                for row in lines if isinstance(row, dict)
            ]
            df = pl.DataFrame(normalized, schema_overrides=raw_schema, strict=False)
            df = df.with_columns([
                pl.col('tax_ids_json').fill_null('[]'),
            ])

        output_file = f'{partition_path}/{dataset_prefix}_{target_date}.parquet'
        atomic_write_parquet(df, output_file)
        logger.info(f"Saved {len(lines)} records to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error saving raw {dataset_prefix} for {extraction_result.get('target_date')}: {e}", exc_info=True)
        return None


@app.task
def save_raw_sales_invoice_lines(extraction_result: Dict[str, Any]) -> Optional[str]:
    return _save_raw_account_move_lines(extraction_result, RAW_SALES_INVOICE_PATH, 'account_move_out_invoice_lines')


@app.task
def save_raw_purchase_invoice_lines(extraction_result: Dict[str, Any]) -> Optional[str]:
    return _save_raw_account_move_lines(extraction_result, RAW_PURCHASES_PATH, 'account_move_in_invoice_lines')


@app.task
def save_raw_inventory_moves(extraction_result: Dict[str, Any]) -> Optional[str]:
    try:
        lines = extraction_result.get('lines', [])
        target_date = extraction_result.get('target_date')
        if not target_date:
            logger.warning("Missing target_date in extraction result")
            return None

        year, month, day = target_date.split('-')
        partition_path = f'{RAW_INVENTORY_MOVES_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(partition_path, exist_ok=True)

        raw_schema = {
            'move_id': pl.Int64,
            'move_line_id': pl.Int64,
            'movement_date': pl.Utf8,
            'product_id': pl.Int64,
            'location_src_id': pl.Int64,
            'location_dest_id': pl.Int64,
            'qty_moved': pl.Float64,
            'uom_id': pl.Int64,
            'picking_id': pl.Int64,
            'picking_type_code': pl.Utf8,
            'reference': pl.Utf8,
            'origin_reference': pl.Utf8,
            'source_partner_id': pl.Int64,
            'destination_partner_id': pl.Int64,
            'created_by_user': pl.Int64,
            'create_date': pl.Utf8,
        }

        if not lines:
            logger.info(f"No data for {target_date} (inventory_moves)")
            df = pl.DataFrame(schema=raw_schema)
        else:
            normalized = [
                {k: row.get(k) for k in raw_schema.keys()}
                for row in lines if isinstance(row, dict)
            ]
            df = pl.DataFrame(normalized, schema_overrides=raw_schema, strict=False)
            df = df.with_columns([
                pl.col('qty_moved').fill_null(0),
            ])

        output_file = f'{partition_path}/inventory_moves_{target_date}.parquet'
        atomic_write_parquet(df, output_file)
        logger.info(f"Saved {len(lines)} records to {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"Error saving raw inventory moves for {extraction_result.get('target_date')}: {e}", exc_info=True)
        return None

# CLEAN DATA TASKS
# ============================================================================

def _clean_account_move_lines(
    raw_file_path: Optional[str],
    target_date: str,
    clean_base_path: str,
    dataset_prefix: str,
) -> Optional[str]:
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None

        df = pl.scan_parquet(raw_file_path)
        existing_cols = set(df.collect_schema().names())
        if 'purchase_order_id' not in existing_cols:
            df = df.with_columns(pl.lit(None, dtype=pl.Int64).alias('purchase_order_id'))
        if 'purchase_order_name' not in existing_cols:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias('purchase_order_name'))
        if 'tax_ids_json' in existing_cols:
            df = df.with_columns(pl.col('tax_ids_json').cast(pl.Utf8, strict=False).fill_null('[]').alias('tax_ids_json'))
        else:
            df = df.with_columns(pl.lit('[]', dtype=pl.Utf8).alias('tax_ids_json'))

        df_clean = (
            df
            .filter(
                (pl.col('product_id').is_not_null()) &
                (pl.col('quantity').is_not_null()) &
                (pl.col('quantity') != 0) &
                (pl.col('price_unit').is_not_null())
            )
            .with_columns([
                pl.col('move_id', 'customer_id', 'vendor_id', 'purchase_order_id', 'move_line_id', 'product_id', 'tax_id')
                    .cast(pl.Int64, strict=False),
                pl.col('move_name', 'move_date', 'customer_name', 'vendor_name', 'purchase_order_name')
                    .cast(pl.Utf8, strict=False),
                pl.col('price_unit', 'quantity').cast(pl.Float64, strict=False),
                pl.col('tax_ids_json').alias('tax_ids_json'),
            ])
        )

        year, month, day = target_date.split('-')
        clean_path = f'{clean_base_path}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)

        output_file = f'{clean_path}/{dataset_prefix}_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(streaming=True), output_file)

        logger.info(f"Cleaned invoice data saved to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error cleaning {dataset_prefix} for {target_date}: {e}", exc_info=True)
        return None


@app.task
def clean_sales_invoice_lines(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    return _clean_account_move_lines(raw_file_path, target_date, CLEAN_SALES_INVOICE_PATH, 'account_move_out_invoice_lines')


@app.task
def clean_purchase_invoice_lines(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    return _clean_account_move_lines(raw_file_path, target_date, CLEAN_PURCHASES_PATH, 'account_move_in_invoice_lines')


@app.task
def clean_inventory_moves(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None

        df_clean = (
            pl.scan_parquet(raw_file_path)
            .filter(pl.col('qty_moved').is_not_null() & (pl.col('qty_moved') != 0))
            .with_columns(
                pl.col('move_id', 'move_line_id', 'product_id', 'location_src_id',
                       'location_dest_id', 'uom_id', 'picking_id', 'source_partner_id',
                       'destination_partner_id', 'created_by_user').cast(pl.Int64, strict=False),
                pl.col('qty_moved').cast(pl.Float64, strict=False),
                pl.col('picking_type_code', 'reference', 'origin_reference').cast(pl.Utf8, strict=False),
                to_local_datetime('movement_date'),
                to_local_datetime('create_date'),
            )
        )

        # Load dimensions once
        dim_products = DimensionLoader.get(DIM_PRODUCTS_FILE)
        dim_locations = DimensionLoader.get(DIM_LOCATIONS_FILE)
        dim_uoms = DimensionLoader.get(DIM_UOMS_FILE)
        dim_partners = DimensionLoader.get(DIM_PARTNERS_FILE)

        # Products join
        if dim_products is not None:
            df_clean = df_clean.join(
                dim_products.select('product_id', 'product_name', 'product_brand'),
                on='product_id', how='left'
            )
        else:
            df_clean = df_clean.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias('product_name'),
                pl.lit(None, dtype=pl.Utf8).alias('product_brand'),
            )

        # Locations join (single load, two projections)
        if dim_locations is not None:
            df_clean = (
                df_clean
                .join(
                    dim_locations.select(
                        pl.col('location_id').alias('location_src_id'),
                        pl.col('location_name').alias('location_src_name'),
                        pl.col('location_usage').alias('location_src_usage'),
                    ),
                    on='location_src_id', how='left'
                )
                .join(
                    dim_locations.select(
                        pl.col('location_id').alias('location_dest_id'),
                        pl.col('location_name').alias('location_dest_name'),
                        pl.col('location_usage').alias('location_dest_usage'),
                    ),
                    on='location_dest_id', how='left'
                )
            )
        else:
            df_clean = df_clean.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias('location_src_name'),
                pl.lit(None, dtype=pl.Utf8).alias('location_src_usage'),
                pl.lit(None, dtype=pl.Utf8).alias('location_dest_name'),
                pl.lit(None, dtype=pl.Utf8).alias('location_dest_usage'),
            )

        # UOMs join
        if dim_uoms is not None:
            df_clean = df_clean.join(
                dim_uoms.select('uom_id', 'uom_name', 'uom_category'),
                on='uom_id', how='left'
            )
        else:
            df_clean = df_clean.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias('uom_name'),
                pl.lit(None, dtype=pl.Utf8).alias('uom_category'),
            )

        # Partners join (single load, two projections)
        if dim_partners is not None:
            df_clean = (
                df_clean
                .join(
                    dim_partners.select(
                        pl.col('partner_id').alias('source_partner_id'),
                        pl.col('partner_name').alias('source_partner_name'),
                    ),
                    on='source_partner_id', how='left'
                )
                .join(
                    dim_partners.select(
                        pl.col('partner_id').alias('destination_partner_id'),
                        pl.col('partner_name').alias('destination_partner_name'),
                    ),
                    on='destination_partner_id', how='left'
                )
            )
        else:
            df_clean = df_clean.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias('source_partner_name'),
                pl.lit(None, dtype=pl.Utf8).alias('destination_partner_name'),
            )

        # Filter and select
        output_columns = [
            'move_id', 'move_line_id', 'movement_date', 'product_id', 'product_name',
            'product_brand', 'location_src_id', 'location_src_name', 'location_dest_id',
            'location_dest_name', 'qty_moved', 'uom_id', 'uom_name', 'uom_category',
            'picking_id', 'picking_type_code', 'reference', 'origin_reference',
            'source_partner_id', 'source_partner_name', 'destination_partner_id',
            'destination_partner_name', 'created_by_user', 'create_date',
        ]

        df_clean = (
            df_clean
            .filter(~(
                (pl.col('location_src_usage') == 'internal') &
                (pl.col('location_dest_usage') == 'internal')
            ))
            .select(output_columns)
        )

        year, month, day = target_date.split('-')
        clean_path = f'{CLEAN_INVENTORY_MOVES_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)

        output_file = f'{clean_path}/inventory_moves_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(streaming=True), output_file)

        logger.info(f"Cleaned inventory moves saved to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error cleaning inventory moves for {target_date}: {e}", exc_info=True)
        return None


@app.task
def clean_pos_data(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    """Clean and validate POS data with lazy evaluation."""
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None

        df_clean = (
            pl.scan_parquet(raw_file_path)
            .filter(
                (pl.col('product_id').is_not_null()) &
                (pl.col('qty').is_not_null()) &
                (pl.col('qty') != 0) &
                (pl.col('price_subtotal_incl').is_not_null())
            )
            .with_columns(
                pl.col('order_date')
                    .str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S')
                    .dt.replace_time_zone('UTC')
                    .dt.convert_time_zone(app.conf.timezone)
                    .dt.replace_time_zone(None),
                pl.col('order_id', 'pos_config_id', 'cashier_id', 'customer_id',
                       'line_id', 'product_brand_id').cast(pl.Int64, strict=False),
                pl.col('order_ref').cast(pl.Utf8, strict=False),
                pl.col('payment_method_ids').cast(pl.Utf8, strict=False).fill_null('[]'),
                pl.col('amount_total', 'qty', 'price_subtotal_incl').cast(pl.Float64, strict=False),
                pl.col('discount_amount').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('product_brand').fill_null('Unknown'),
                pl.col('product_category').fill_null('Unknown'),
                pl.col('product_parent_category').fill_null('Unknown'),
            )
        )

        year, month, day = target_date.split('-')
        clean_path = f'{CLEAN_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)

        output_file = f'{clean_path}/pos_order_lines_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(streaming=True), output_file)

        logger.info(f"Cleaned data saved to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error cleaning data for {target_date}: {e}", exc_info=True)
        return None


# ============================================================================
# STAR SCHEMA UPDATE TASKS
# ============================================================================

def _update_fact_invoice_sales(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('move_date').alias('date'),
        'move_id', 'move_name', 'customer_id', 'customer_name',
        'move_line_id', 'product_id', 'price_unit', 'quantity', 'tax_ids_json',
        pl.lit(False).alias('is_free_item'),
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_invoice_sales'
    os.makedirs(fact_path, exist_ok=True)

    fact_output = f'{fact_path}/fact_invoice_sales_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)
    return fact_output


def _update_fact_purchases(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('move_date').alias('date'),
        'move_id', 'move_name', 'vendor_id',
        pl.col('vendor_name')
            .cast(pl.Utf8, strict=False)
            .str.replace(r'[,，].*$', '')
            .str.replace(r'^\s+', '')
            .str.replace(r'\s+$', '')
            .alias('vendor_name'),
        'purchase_order_id', 'purchase_order_name',
        'move_line_id', 'product_id', 'price_unit', 'quantity',
        'tax_id', 'tax_ids_json',
        pl.lit(False).cast(pl.Boolean).alias('is_free_item'),
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_purchases'
    os.makedirs(fact_path, exist_ok=True)

    fact_output = f'{fact_path}/fact_purchases_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)
    return fact_output


def _update_fact_sales_pos(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('order_date').alias('date'),
        'order_id', 'order_ref', 'pos_config_id', 'cashier_id',
        'customer_id', 'payment_method_ids', 'line_id', 'product_id',
        pl.col('qty').alias('quantity'),
        pl.col('price_subtotal_incl').alias('revenue'),
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_sales'
    os.makedirs(fact_path, exist_ok=True)

    fact_output = f'{fact_path}/fact_sales_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)
    return fact_output


def _update_fact_inventory_moves(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('movement_date').alias('date'),
        'move_id', 'move_line_id', 'product_id', 'product_name', 'product_brand',
        'location_src_id', 'location_src_name', 'location_dest_id', 'location_dest_name',
        'qty_moved', 'uom_id', 'uom_name', 'uom_category', 'picking_id', 'picking_type_code',
        'reference', 'origin_reference', 'source_partner_id', 'source_partner_name',
        'destination_partner_id', 'destination_partner_name', 'created_by_user', 'create_date',
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_inventory_moves'
    os.makedirs(fact_path, exist_ok=True)

    fact_output = f'{fact_path}/fact_inventory_moves_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)
    return fact_output


@app.task
def update_star_schema(clean_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not clean_file_path or not os.path.isfile(clean_file_path):
            logger.warning(f"Invalid file path: {clean_file_path}")
            return None

        df = pl.read_parquet(clean_file_path)
        output = _update_fact_sales_pos(df, target_date)
        ETLMetadata.set_last_processed_date(date.fromisoformat(target_date))
        return output

    except Exception as e:
        logger.error(f"Error updating star schema for {target_date}: {e}", exc_info=True)
        return None


@app.task
def update_invoice_sales_star_schema(clean_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not clean_file_path or not os.path.isfile(clean_file_path):
            logger.warning(f"Invalid file path: {clean_file_path}")
            return None
        df = pl.read_parquet(clean_file_path)
        return _update_fact_invoice_sales(df, target_date)
    except Exception as e:
        logger.error(f"Error updating invoice sales star schema for {target_date}: {e}", exc_info=True)
        return None


@app.task
def update_purchase_star_schema(clean_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not clean_file_path or not os.path.isfile(clean_file_path):
            logger.warning(f"Invalid file path: {clean_file_path}")
            return None
        df = pl.read_parquet(clean_file_path)
        return _update_fact_purchases(df, target_date)
    except Exception as e:
        logger.error(f"Error updating purchases star schema for {target_date}: {e}", exc_info=True)
        return None


@app.task
def update_inventory_moves_star_schema(clean_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not clean_file_path or not os.path.isfile(clean_file_path):
            logger.warning(f"Invalid file path: {clean_file_path}")
            return None
        df = pl.read_parquet(clean_file_path)
        return _update_fact_inventory_moves(df, target_date)
    except Exception as e:
        logger.error(f"Error updating inventory moves star schema for {target_date}: {e}", exc_info=True)
        return None


# ============================================================================
# DIMENSION REFRESH
# ============================================================================

@app.task
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
                    fields = get_model_fields(odoo, 'product.product', ['id', 'name', 'categ_id', 'x_studio_brand_id'])
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


# ============================================================================
# PIPELINE ORCHESTRATION
# ============================================================================

@app.task
def daily_etl_pipeline(target_date: Optional[str] = None) -> str:
    """Optimized daily ETL pipeline."""
    from etl.pipelines.daily import daily_etl_pipeline_impl
    return daily_etl_pipeline_impl(target_date)


@app.task
def daily_invoice_sales_pipeline(target_date: Optional[str] = None) -> str:
    """Daily pipeline for invoice-based sales (out_invoice)."""
    from etl.pipelines.daily import daily_invoice_sales_pipeline_impl
    return daily_invoice_sales_pipeline_impl(target_date)


@app.task
def daily_invoice_purchases_pipeline(target_date: Optional[str] = None) -> str:
    """Daily pipeline for purchases (vendor bills, in_invoice)."""
    from etl.pipelines.daily import daily_invoice_purchases_pipeline_impl
    return daily_invoice_purchases_pipeline_impl(target_date)


@app.task
def daily_inventory_moves_pipeline(target_date: Optional[str] = None) -> str:
    """Daily pipeline for inventory moves (stock.move.line)."""
    from etl.pipelines.daily import daily_inventory_moves_pipeline_impl
    return daily_inventory_moves_pipeline_impl(target_date)


@app.task
def date_range_etl_pipeline(start_date: str, end_date: Optional[str] = None) -> Dict[str, Any]:
    """Process date range in parallel."""
    if end_date is None:
        end_date = start_date
    from etl.pipelines.ranges import date_range_etl_pipeline_impl
    return date_range_etl_pipeline_impl(start_date, end_date)


@app.task
def catch_up_etl() -> Dict[str, Any]:
    """Auto-catch up missed dates."""
    from etl.pipelines.health import catch_up_etl_impl
    return catch_up_etl_impl()


@app.task
def health_check() -> Dict[str, Any]:
    """Health check with auto-recovery."""
    from etl.pipelines.health import health_check_impl
    return health_check_impl()


# ============================================================================
# CELERY BEAT SCHEDULE
# ============================================================================

app.conf.beat_schedule = {
    'daily-etl': {
        'task': 'etl_tasks.daily_etl_pipeline',
        'schedule': crontab(hour=2, minute=0),
    },
    'daily-invoice-sales-etl': {
        'task': 'etl_tasks.daily_invoice_sales_pipeline',
        'schedule': crontab(hour=2, minute=5),
    },
    'daily-invoice-purchases-etl': {
        'task': 'etl_tasks.daily_invoice_purchases_pipeline',
        'schedule': crontab(hour=2, minute=10),
    },
    'daily-inventory-moves-etl': {
        'task': 'etl_tasks.daily_inventory_moves_pipeline',
        'schedule': crontab(hour=2, minute=15),
    },
    'incremental-dimension-refresh': {
        'task': 'etl_tasks.refresh_dimensions_incremental',
        'schedule': crontab(hour='*/4', minute=0),
    },
    'health-check': {
        'task': 'etl_tasks.health_check',
        'schedule': crontab(hour='*/6', minute=0),
    },
}

app.conf.task_routes = {
    'etl_tasks.extract_pos_order_lines': {'queue': 'extraction'},
    'etl_tasks.extract_sales_invoice_lines': {'queue': 'extraction'},
    'etl_tasks.extract_purchase_invoice_lines': {'queue': 'extraction'},
    'etl_tasks.extract_inventory_moves': {'queue': 'extraction'},
    'etl_tasks.clean_pos_data': {'queue': 'transformation'},
    'etl_tasks.clean_sales_invoice_lines': {'queue': 'transformation'},
    'etl_tasks.clean_purchase_invoice_lines': {'queue': 'transformation'},
    'etl_tasks.clean_inventory_moves': {'queue': 'transformation'},
    'etl_tasks.update_star_schema': {'queue': 'loading'},
    'etl_tasks.update_invoice_sales_star_schema': {'queue': 'loading'},
    'etl_tasks.update_purchase_star_schema': {'queue': 'loading'},
    'etl_tasks.update_inventory_moves_star_schema': {'queue': 'loading'},
    'etl_tasks.save_raw_data': {'queue': 'loading'},
    'etl_tasks.save_raw_sales_invoice_lines': {'queue': 'loading'},
    'etl_tasks.save_raw_purchase_invoice_lines': {'queue': 'loading'},
    'etl_tasks.save_raw_inventory_moves': {'queue': 'loading'},
    'etl_tasks.refresh_dimensions_incremental': {'queue': 'dimensions'},
}