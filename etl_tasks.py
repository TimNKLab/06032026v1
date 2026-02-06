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


@app.task(bind=True)
def force_refresh_day(self, dataset_key: str, target_date: str, refresh_dims: bool = False) -> Dict[str, Any]:
    if dataset_key in {"inventory_moves", "stock_quants"} and refresh_dims:
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "refresh_dimensions", "step_name": "Refresh dimensions", "pct": 5})
        if dataset_key == "inventory_moves":
            refresh_dimensions_incremental.run(["products", "locations", "uoms", "partners", "users", "companies", "lots"])
        else:
            refresh_dimensions_incremental.run(["products", "locations", "lots"])

    if dataset_key == "pos":
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "extract", "step_name": "Extract POS", "pct": 10})
        extraction = extract_pos_order_lines.run(target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "save_raw", "step_name": "Save raw", "pct": 35})
        raw_path = save_raw_data.run(extraction)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "clean", "step_name": "Clean POS", "pct": 60})
        clean_path = clean_pos_data.run(raw_path, target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "update_fact", "step_name": "Update fact", "pct": 85})
        fact_path = update_star_schema.run(clean_path, target_date)
        records = extraction.get("count", 0) if isinstance(extraction, dict) else 0
        return {"dataset": dataset_key, "date": target_date, "records": records, "raw_path": raw_path, "clean_path": clean_path, "fact_path": fact_path}

    if dataset_key == "invoice_sales":
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "extract", "step_name": "Extract invoice sales", "pct": 10})
        extraction = extract_sales_invoice_lines.run(target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "save_raw", "step_name": "Save raw", "pct": 35})
        raw_path = save_raw_sales_invoice_lines.run(extraction)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "clean", "step_name": "Clean invoice sales", "pct": 60})
        clean_path = clean_sales_invoice_lines.run(raw_path, target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "update_fact", "step_name": "Update fact", "pct": 85})
        fact_path = update_invoice_sales_star_schema.run(clean_path, target_date)
        records = extraction.get("count", 0) if isinstance(extraction, dict) else 0
        return {"dataset": dataset_key, "date": target_date, "records": records, "raw_path": raw_path, "clean_path": clean_path, "fact_path": fact_path}

    if dataset_key == "purchases":
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "extract", "step_name": "Extract purchases", "pct": 10})
        extraction = extract_purchase_invoice_lines.run(target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "save_raw", "step_name": "Save raw", "pct": 35})
        raw_path = save_raw_purchase_invoice_lines.run(extraction)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "clean", "step_name": "Clean purchases", "pct": 60})
        clean_path = clean_purchase_invoice_lines.run(raw_path, target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "update_fact", "step_name": "Update fact", "pct": 85})
        fact_path = update_purchase_star_schema.run(clean_path, target_date)
        records = extraction.get("count", 0) if isinstance(extraction, dict) else 0
        return {"dataset": dataset_key, "date": target_date, "records": records, "raw_path": raw_path, "clean_path": clean_path, "fact_path": fact_path}

    if dataset_key == "inventory_moves":
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "extract", "step_name": "Extract inventory moves", "pct": 10})
        extraction = extract_inventory_moves.run(target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "save_raw", "step_name": "Save raw", "pct": 35})
        raw_path = save_raw_inventory_moves.run(extraction)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "clean", "step_name": "Clean inventory moves", "pct": 60})
        clean_path = clean_inventory_moves.run(raw_path, target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "update_fact", "step_name": "Update fact", "pct": 85})
        fact_path = update_inventory_moves_star_schema.run(clean_path, target_date)
        records = extraction.get("count", 0) if isinstance(extraction, dict) else 0
        return {"dataset": dataset_key, "date": target_date, "records": records, "raw_path": raw_path, "clean_path": clean_path, "fact_path": fact_path}

    if dataset_key == "stock_quants":
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "extract", "step_name": "Extract stock quants", "pct": 10})
        extraction = extract_stock_quants.run(target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "save_raw", "step_name": "Save raw", "pct": 35})
        raw_path = save_raw_stock_quants.run(extraction)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "clean", "step_name": "Clean stock quants", "pct": 60})
        clean_path = clean_stock_quants.run(raw_path, target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "update_fact", "step_name": "Update fact", "pct": 85})
        fact_path = update_stock_quants_star_schema.run(clean_path, target_date)
        records = extraction.get("count", 0) if isinstance(extraction, dict) else 0
        return {"dataset": dataset_key, "date": target_date, "records": records, "raw_path": raw_path, "clean_path": clean_path, "fact_path": fact_path}

    if dataset_key == "profit":
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "cost_events", "step_name": "Cost events", "pct": 25})
        cost_events_path = update_product_cost_events.run(target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "cost_snapshot", "step_name": "Cost snapshot", "pct": 50})
        cost_snapshot_path = update_product_cost_latest_daily.run(target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "sales_profit", "step_name": "Sales profit", "pct": 75})
        profit_lines_path = update_sales_lines_profit.run(target_date)
        self.update_state(state="PROGRESS", meta={"dataset": dataset_key, "date": target_date, "step": "aggregates", "step_name": "Profit aggregates", "pct": 90})
        agg_paths = update_profit_aggregates.run(target_date)
        return {
            "dataset": dataset_key,
            "date": target_date,
            "cost_events_path": cost_events_path,
            "cost_snapshot_path": cost_snapshot_path,
            "profit_lines_path": profit_lines_path,
            "aggregate_paths": agg_paths,
        }

    raise ValueError(f"Unsupported dataset_key: {dataset_key}")

# ============================================================================
# CONFIG IMPORTS (re-exported for backward compatibility)
# ============================================================================

from etl.config import (
    MAX_RETRIES, RETRY_DELAY, ODOO_BATCH_SIZE, PARQUET_COMPRESSION,
    CONNECTION_TIMEOUT, CACHE_TTL, DATA_LAKE_ROOT, RAW_PATH, CLEAN_PATH,
    RAW_SALES_INVOICE_PATH, RAW_PURCHASES_PATH, RAW_INVENTORY_MOVES_PATH, RAW_STOCK_QUANTS_PATH,
    CLEAN_SALES_INVOICE_PATH, CLEAN_PURCHASES_PATH, CLEAN_INVENTORY_MOVES_PATH, CLEAN_STOCK_QUANTS_PATH,
    STAR_SCHEMA_PATH, METADATA_PATH, DIM_PRODUCTS_FILE, DIM_LOCATIONS_FILE,
    DIM_UOMS_FILE, DIM_PARTNERS_FILE, DIM_USERS_FILE, DIM_COMPANIES_FILE,
    DIM_LOTS_FILE, FACT_PRODUCT_COST_EVENTS_PATH, FACT_PRODUCT_COST_LATEST_DAILY_PATH,
    FACT_SALES_LINES_PROFIT_PATH, AGG_PROFIT_DAILY_PATH, AGG_PROFIT_DAILY_BY_PRODUCT_PATH,
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


def _tax_multiplier_expr(tax_col: str) -> pl.Expr:
    return (
        pl.when(pl.col(tax_col).is_in([5, 2]))
        .then(1.0)
        .when(pl.col(tax_col).is_in([7, 6]))
        .then(1.11)
        .otherwise(1.0)
    )


def _has_parquet_files(path: str) -> bool:
    if os.path.isdir(path):
        for _, _, files in os.walk(path):
            if any(name.endswith('.parquet') for name in files):
                return True
        return False
    return os.path.isfile(path) and path.endswith('.parquet')


def _read_parquet_or_empty(path: str, schema: Dict[str, pl.DataType]) -> pl.DataFrame:
    if os.path.isfile(path):
        return pl.read_parquet(path)
    if _has_parquet_files(path):
        return pl.read_parquet(f"{path}/**/*.parquet")
    return pl.DataFrame(schema=schema)


def _partition_path(base_path: str, target_date: str) -> str:
    year, month, day = target_date.split('-')
    return f'{base_path}/year={year}/month={month}/day={day}'


def _write_partitioned(df: pl.DataFrame, base_path: str, target_date: str, filename_prefix: str) -> str:
    partition_path = _partition_path(base_path, target_date)
    os.makedirs(partition_path, exist_ok=True)
    output_file = f'{partition_path}/{filename_prefix}_{target_date}.parquet'
    atomic_write_parquet(df, output_file)
    return output_file


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


@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_stock_quants(self, target_date: str) -> Dict[str, Any]:
    """Extract stock quant snapshot for a target date."""
    from etl.extract.stock_quants import extract_stock_quants_impl
    return extract_stock_quants_impl(target_date)


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


@app.task
def clean_pos_data(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None

        df_clean = (
            pl.scan_parquet(raw_file_path)
            .with_columns(
                to_local_datetime('order_date').alias('date'),
                pl.col('order_id', 'pos_config_id', 'cashier_id', 'customer_id', 'line_id', 'product_id')
                    .cast(pl.Int64, strict=False),
                pl.col('order_ref').cast(pl.Utf8, strict=False),
                pl.col('payment_method_ids').cast(pl.Utf8, strict=False).fill_null('[]'),
                pl.col('qty').cast(pl.Float64, strict=False).fill_null(0).alias('quantity'),
                (
                    pl.col('price_subtotal_incl').cast(pl.Float64, strict=False).fill_null(0)
                    - pl.col('discount_amount').cast(pl.Float64, strict=False).fill_null(0)
                ).alias('revenue'),
            )
            .select([
                'date',
                'order_id',
                'order_ref',
                'pos_config_id',
                'cashier_id',
                'customer_id',
                'payment_method_ids',
                'line_id',
                'product_id',
                'quantity',
                'revenue',
            ])
        )

        year, month, day = target_date.split('-')
        clean_path = f'{CLEAN_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)

        output_file = f'{clean_path}/pos_order_lines_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(streaming=True), output_file)
        logger.info(f"Cleaned POS data saved to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error cleaning POS data for {target_date}: {e}", exc_info=True)
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
            'movement_type': pl.Utf8,
            'inventory_adjustment_flag': pl.Boolean,
            'manufacturing_order_id': pl.Int64,
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
                pl.col('inventory_adjustment_flag').fill_null(False),
            ])

        output_file = f'{partition_path}/inventory_moves_{target_date}.parquet'
        atomic_write_parquet(df, output_file)
        logger.info(f"Saved {len(lines)} records to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error saving raw inventory moves for {extraction_result.get('target_date')}: {e}", exc_info=True)
        return None


@app.task
def save_raw_stock_quants(extraction_result: Dict[str, Any]) -> Optional[str]:
    try:
        lines = extraction_result.get('lines', [])
        target_date = extraction_result.get('target_date')
        if not target_date:
            logger.warning("Missing target_date in extraction result")
            return None

        year, month, day = target_date.split('-')
        partition_path = f'{RAW_STOCK_QUANTS_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(partition_path, exist_ok=True)

        raw_schema = {
            'quant_id': pl.Int64,
            'snapshot_date': pl.Utf8,
            'product_id': pl.Int64,
            'location_id': pl.Int64,
            'lot_id': pl.Int64,
            'owner_id': pl.Int64,
            'company_id': pl.Int64,
            'quantity': pl.Float64,
            'reserved_quantity': pl.Float64,
        }

        if not lines:
            logger.info(f"No data for {target_date} (stock_quants)")
            df = pl.DataFrame(schema=raw_schema)
        else:
            normalized = [
                {k: row.get(k) for k in raw_schema.keys()}
                for row in lines if isinstance(row, dict)
            ]
            df = pl.DataFrame(normalized, schema_overrides=raw_schema, strict=False)
            df = df.with_columns([
                pl.col('quantity').fill_null(0),
                pl.col('reserved_quantity').fill_null(0),
            ])

        output_file = f'{partition_path}/stock_quants_{target_date}.parquet'
        atomic_write_parquet(df, output_file)
        logger.info(f"Saved {len(lines)} records to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error saving raw stock quants for {extraction_result.get('target_date')}: {e}", exc_info=True)
        return None


@app.task
def clean_sales_invoice_lines(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None

        df_clean = (
            pl.scan_parquet(raw_file_path)
            .with_columns(
                pl.col('move_date')
                    .cast(pl.Utf8, strict=False)
                    .str.strptime(pl.Date, '%Y-%m-%d', strict=False)
                    .alias('date'),
                pl.col('move_id', 'customer_id', 'move_line_id', 'product_id', 'tax_id')
                    .cast(pl.Int64, strict=False),
                pl.col('move_name', 'customer_name').cast(pl.Utf8, strict=False).fill_null(''),
                pl.col('price_unit', 'quantity').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('tax_ids_json').cast(pl.Utf8, strict=False).fill_null('[]'),
            )
            .select([
                'date',
                'move_id',
                'move_name',
                'customer_id',
                'customer_name',
                'move_line_id',
                'product_id',
                'price_unit',
                'quantity',
                'tax_id',
                'tax_ids_json',
            ])
        )

        year, month, day = target_date.split('-')
        clean_path = f'{CLEAN_SALES_INVOICE_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)

        output_file = f'{clean_path}/account_move_out_invoice_lines_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(streaming=True), output_file)
        logger.info(f"Cleaned invoice sales lines saved to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error cleaning invoice sales for {target_date}: {e}", exc_info=True)
        return None


@app.task
def clean_purchase_invoice_lines(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None

        base = (
            pl.scan_parquet(raw_file_path)
            .with_columns(
                pl.col('move_date')
                    .cast(pl.Utf8, strict=False)
                    .str.strptime(pl.Date, '%Y-%m-%d', strict=False)
                    .alias('date'),
                pl.col('move_id', 'vendor_id', 'purchase_order_id', 'move_line_id', 'product_id', 'tax_id')
                    .cast(pl.Int64, strict=False),
                pl.col('move_name', 'vendor_name', 'purchase_order_name').cast(pl.Utf8, strict=False).fill_null(''),
                pl.col('price_unit', 'quantity').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('tax_ids_json').cast(pl.Utf8, strict=False).fill_null('[]'),
            )
            .select([
                'date',
                'move_id',
                'move_name',
                'vendor_id',
                'vendor_name',
                'purchase_order_id',
                'purchase_order_name',
                'move_line_id',
                'product_id',
                'price_unit',
                'quantity',
                'tax_id',
                'tax_ids_json',
            ])
        )

        line_totals = base.with_columns([
            (pl.col('price_unit') * pl.col('quantity')).alias('line_total'),
        ])

        discount_by_move = line_totals.group_by('move_id').agg([
            pl.when(pl.col('price_unit') >= 0)
            .then(pl.col('line_total'))
            .otherwise(0)
            .sum()
            .alias('gross_amount'),
            pl.when(pl.col('price_unit') < 0)
            .then(pl.col('line_total'))
            .otherwise(0)
            .sum()
            .alias('discount_amount'),
        ])

        df_clean = (
            line_totals
            .join(discount_by_move, on='move_id', how='left')
            .with_columns(
                pl.when(pl.col('gross_amount') != 0)
                .then(pl.col('discount_amount') / pl.col('gross_amount'))
                .otherwise(0.0)
                .alias('discount_pct')
            )
            .with_columns(
                pl.when(pl.col('price_unit') < 0)
                .then(0.0)
                .otherwise(pl.col('price_unit') * (1 + pl.col('discount_pct')))
                .alias('actual_price')
            )
            .select([
                'date',
                'move_id',
                'move_name',
                'vendor_id',
                'vendor_name',
                'purchase_order_id',
                'purchase_order_name',
                'move_line_id',
                'product_id',
                'price_unit',
                'actual_price',
                'quantity',
                'tax_id',
                'tax_ids_json',
            ])
        )

        year, month, day = target_date.split('-')
        clean_path = f'{CLEAN_PURCHASES_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)

        output_file = f'{clean_path}/account_move_in_invoice_lines_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(streaming=True), output_file)
        logger.info(f"Cleaned purchase invoice lines saved to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error cleaning purchases for {target_date}: {e}", exc_info=True)
        return None


@app.task
def clean_stock_quants(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None

        df_clean = (
            pl.scan_parquet(raw_file_path)
            .filter(pl.col('product_id').is_not_null())
            .with_columns(
                pl.col('quant_id', 'product_id', 'location_id', 'lot_id', 'owner_id', 'company_id')
                    .cast(pl.Int64, strict=False),
                pl.col('quantity', 'reserved_quantity').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('snapshot_date')
                    .cast(pl.Utf8, strict=False)
                    .str.strptime(pl.Date, '%Y-%m-%d', strict=False)
                    .alias('snapshot_date'),
            )
        )

        output_columns = [
            'snapshot_date', 'quant_id', 'product_id', 'location_id',
            'lot_id', 'owner_id', 'company_id', 'quantity', 'reserved_quantity',
        ]

        df_clean = df_clean.select(output_columns)

        year, month, day = target_date.split('-')
        clean_path = f'{CLEAN_STOCK_QUANTS_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)

        output_file = f'{clean_path}/stock_quants_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(streaming=True), output_file)
        logger.info(f"Cleaned stock quants saved to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error cleaning stock quants for {target_date}: {e}", exc_info=True)
        return None


@app.task
def clean_inventory_moves(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None

        dim_products_exists = os.path.isfile(DIM_PRODUCTS_FILE)
        dim_locations_exists = os.path.isfile(DIM_LOCATIONS_FILE)
        dim_uoms_exists = os.path.isfile(DIM_UOMS_FILE)
        dim_partners_exists = os.path.isfile(DIM_PARTNERS_FILE)

        base = (
            pl.scan_parquet(raw_file_path)
            .filter(pl.col('product_id').is_not_null())
            .with_columns(
                pl.col(
                    'move_id', 'move_line_id', 'product_id',
                    'location_src_id', 'location_dest_id',
                    'uom_id', 'picking_id',
                    'source_partner_id', 'destination_partner_id',
                    'created_by_user', 'manufacturing_order_id',
                ).cast(pl.Int64, strict=False),
                pl.col('qty_moved').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('movement_type').cast(pl.Utf8, strict=False),
                pl.col('inventory_adjustment_flag').cast(pl.Boolean, strict=False).fill_null(False),
                pl.col('movement_date').cast(pl.Utf8, strict=False).alias('date'),
                pl.col('create_date').cast(pl.Utf8, strict=False),
            )
        )

        if dim_products_exists:
            dim_products = pl.scan_parquet(DIM_PRODUCTS_FILE).select([
                'product_id', 'product_name', 'product_brand',
            ])
        else:
            dim_products = pl.DataFrame(schema={
                'product_id': pl.Int64,
                'product_name': pl.Utf8,
                'product_brand': pl.Utf8,
            }).lazy()

        if dim_locations_exists:
            dim_locations = pl.scan_parquet(DIM_LOCATIONS_FILE).select([
                'location_id', 'location_name', 'location_usage', 'scrap_location',
            ])
        else:
            dim_locations = pl.DataFrame(schema={
                'location_id': pl.Int64,
                'location_name': pl.Utf8,
                'location_usage': pl.Utf8,
                'scrap_location': pl.Boolean,
            }).lazy()

        if dim_uoms_exists:
            dim_uoms = pl.scan_parquet(DIM_UOMS_FILE).select([
                'uom_id', 'uom_name', 'uom_category',
            ])
        else:
            dim_uoms = pl.DataFrame(schema={
                'uom_id': pl.Int64,
                'uom_name': pl.Utf8,
                'uom_category': pl.Utf8,
            }).lazy()

        if dim_partners_exists:
            dim_partners = pl.scan_parquet(DIM_PARTNERS_FILE).select([
                'partner_id', 'partner_name',
            ])
        else:
            dim_partners = pl.DataFrame(schema={
                'partner_id': pl.Int64,
                'partner_name': pl.Utf8,
            }).lazy()

        df_clean = (
            base
            .join(dim_products, on='product_id', how='left')
            .join(
                dim_locations.rename({
                    'location_id': 'location_src_id',
                    'location_name': 'location_src_name',
                    'location_usage': 'location_src_usage',
                    'scrap_location': 'location_src_scrap',
                }),
                on='location_src_id',
                how='left',
            )
            .join(
                dim_locations.rename({
                    'location_id': 'location_dest_id',
                    'location_name': 'location_dest_name',
                    'location_usage': 'location_dest_usage',
                    'scrap_location': 'location_dest_scrap',
                }),
                on='location_dest_id',
                how='left',
            )
            .join(dim_uoms, on='uom_id', how='left')
            .join(
                dim_partners.rename({
                    'partner_id': 'source_partner_id',
                    'partner_name': 'source_partner_name',
                }),
                on='source_partner_id',
                how='left',
            )
            .join(
                dim_partners.rename({
                    'partner_id': 'destination_partner_id',
                    'partner_name': 'destination_partner_name',
                }),
                on='destination_partner_id',
                how='left',
            )
            .with_columns(
                pl.col('product_name').cast(pl.Utf8, strict=False).fill_null(''),
                pl.col('product_brand').cast(pl.Utf8, strict=False).fill_null(''),
                pl.col('location_src_name').cast(pl.Utf8, strict=False).fill_null(''),
                pl.col('location_dest_name').cast(pl.Utf8, strict=False).fill_null(''),
                pl.col('source_partner_name').cast(pl.Utf8, strict=False).fill_null(''),
                pl.col('destination_partner_name').cast(pl.Utf8, strict=False).fill_null(''),
                pl.col('uom_name').cast(pl.Utf8, strict=False).fill_null(''),
                pl.col('uom_category').cast(pl.Utf8, strict=False).fill_null(''),
            )
        )

        output_columns = [
            'date',
            'move_id',
            'move_line_id',
            'product_id',
            'product_name',
            'product_brand',
            'location_src_id',
            'location_src_name',
            'location_src_usage',
            'location_dest_id',
            'location_dest_name',
            'location_dest_usage',
            'qty_moved',
            'uom_id',
            'uom_name',
            'uom_category',
            'movement_type',
            'inventory_adjustment_flag',
            'manufacturing_order_id',
            'picking_id',
            'picking_type_code',
            'reference',
            'origin_reference',
            'source_partner_id',
            'source_partner_name',
            'destination_partner_id',
            'destination_partner_name',
            'created_by_user',
            'create_date',
        ]

        df_clean = df_clean.select(output_columns)

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


def _update_fact_inventory_moves(df: pl.DataFrame, target_date: str) -> str:
    fact_path = f'{STAR_SCHEMA_PATH}/fact_inventory_moves'
    year, month, day = target_date.split('-')
    fact_partition = f'{fact_path}/year={year}/month={month}/day={day}'
    os.makedirs(fact_partition, exist_ok=True)

    fact_output = f'{fact_partition}/fact_inventory_moves_{target_date}.parquet'
    atomic_write_parquet(df, fact_output)
    return fact_output


def _update_fact_sales_pos(df: pl.DataFrame, target_date: str) -> str:
    if 'date' not in df.columns and 'order_date' in df.columns:
        df = df.with_columns(to_local_datetime('order_date').alias('date'))

    fact_df = df.select([
        pl.col('date'),
        pl.col('order_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('order_ref').cast(pl.Utf8, strict=False).fill_null(''),
        pl.col('pos_config_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('cashier_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('customer_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('payment_method_ids').cast(pl.Utf8, strict=False).fill_null('[]'),
        pl.col('line_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('product_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0),
        pl.col('revenue').cast(pl.Float64, strict=False).fill_null(0),
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_sales'
    year, month, day = target_date.split('-')
    fact_partition = f'{fact_path}/year={year}/month={month}/day={day}'
    os.makedirs(fact_partition, exist_ok=True)

    fact_output = f'{fact_partition}/fact_sales_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)
    return fact_output


def _update_fact_invoice_sales(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('date'),
        pl.col('move_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('move_name').cast(pl.Utf8, strict=False).fill_null(''),
        pl.col('customer_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('customer_name').cast(pl.Utf8, strict=False).fill_null(''),
        pl.col('move_line_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('product_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('price_unit').cast(pl.Float64, strict=False).fill_null(0),
        pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0),
        pl.col('tax_id').cast(pl.Int64, strict=False),
        pl.col('tax_ids_json').cast(pl.Utf8, strict=False).fill_null('[]'),
        pl.lit(False).alias('is_free_item'),
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_invoice_sales'
    year, month, day = target_date.split('-')
    fact_partition = f'{fact_path}/year={year}/month={month}/day={day}'
    os.makedirs(fact_partition, exist_ok=True)

    fact_output = f'{fact_partition}/fact_invoice_sales_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)
    return fact_output


def _update_fact_purchases(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('date'),
        pl.col('move_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('move_name').cast(pl.Utf8, strict=False).fill_null(''),
        pl.col('vendor_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('vendor_name').cast(pl.Utf8, strict=False).fill_null(''),
        pl.col('purchase_order_id').cast(pl.Int64, strict=False),
        pl.col('purchase_order_name').cast(pl.Utf8, strict=False).fill_null(''),
        pl.col('move_line_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('product_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('price_unit').cast(pl.Float64, strict=False).fill_null(0),
        pl.col('actual_price').cast(pl.Float64, strict=False).fill_null(0),
        pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0),
        pl.col('tax_id').cast(pl.Int64, strict=False),
        pl.lit('').alias('tax_name'),
        pl.col('tax_ids_json').cast(pl.Utf8, strict=False).fill_null('[]'),
        pl.lit(False).alias('is_free_item'),
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_purchases'
    year, month, day = target_date.split('-')
    fact_partition = f'{fact_path}/year={year}/month={month}/day={day}'
    os.makedirs(fact_partition, exist_ok=True)

    fact_output = f'{fact_partition}/fact_purchases_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)
    return fact_output


def _update_fact_stock_on_hand_snapshot(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        'snapshot_date', 'quant_id', 'product_id', 'location_id',
        'lot_id', 'owner_id', 'company_id', 'quantity', 'reserved_quantity',
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_stock_on_hand_snapshot'
    year, month, day = target_date.split('-')
    fact_partition = f'{fact_path}/year={year}/month={month}/day={day}'
    os.makedirs(fact_partition, exist_ok=True)

    fact_output = f'{fact_partition}/fact_stock_on_hand_snapshot_{target_date}.parquet'
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


@app.task
def update_stock_quants_star_schema(clean_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not clean_file_path or not os.path.isfile(clean_file_path):
            logger.warning(f"Invalid file path: {clean_file_path}")
            return None
        df = pl.read_parquet(clean_file_path)
        return _update_fact_stock_on_hand_snapshot(df, target_date)
    except Exception as e:
        logger.error(f"Error updating stock quants star schema for {target_date}: {e}", exc_info=True)
        return None


# ============================================================================
# COST + PROFIT MATERIALIZATION
# ============================================================================

def _build_product_cost_events(target_date: str) -> pl.DataFrame:
    purchases_schema = {
        'date': pl.Date,
        'move_id': pl.Int64,
        'move_line_id': pl.Int64,
        'product_id': pl.Int64,
        'actual_price': pl.Float64,
        'quantity': pl.Float64,
        'tax_id': pl.Int64,
    }
    cost_schema = {
        'date': pl.Date,
        'product_id': pl.Int64,
        'cost_unit_tax_in': pl.Float64,
        'source_move_id': pl.Int64,
        'source_tax_id': pl.Int64,
    }

    partition_path = _partition_path(f'{STAR_SCHEMA_PATH}/fact_purchases', target_date)
    df = _read_parquet_or_empty(partition_path, purchases_schema)
    if df.is_empty():
        return pl.DataFrame(schema=cost_schema)

    target_dt = date.fromisoformat(target_date)
    df = (
        df.with_columns(
            pl.lit(target_dt).alias('date'),
            pl.col('product_id').cast(pl.Int64, strict=False),
            pl.col('actual_price').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('tax_id').cast(pl.Int64, strict=False).fill_null(0),
        )
        .filter(
            (pl.col('product_id').is_not_null())
            & (pl.col('product_id') != 0)
            & (pl.col('actual_price') > 0)
            & (pl.col('quantity') > 0)
        )
        .with_columns(
            (pl.col('actual_price') * _tax_multiplier_expr('tax_id')).alias('cost_unit_tax_in')
        )
        .select([
            'date',
            'product_id',
            'cost_unit_tax_in',
            pl.col('move_id').alias('source_move_id'),
            pl.col('tax_id').alias('source_tax_id'),
        ])
    )

    if df.is_empty():
        return pl.DataFrame(schema=cost_schema)
    return df


def _latest_cost_by_product(events: pl.DataFrame) -> pl.DataFrame:
    if events.is_empty():
        return events

    return (
        events.sort('source_move_id')
        .group_by('product_id')
        .agg([
            pl.last('cost_unit_tax_in').alias('cost_unit_tax_in'),
            pl.last('source_move_id').alias('source_move_id'),
            pl.last('source_tax_id').alias('source_tax_id'),
        ])
    )


def _build_cost_snapshot_from_events(target_date: str) -> pl.DataFrame:
    cost_schema = {
        'date': pl.Date,
        'product_id': pl.Int64,
        'cost_unit_tax_in': pl.Float64,
        'source_move_id': pl.Int64,
        'source_tax_id': pl.Int64,
    }

    events = _read_parquet_or_empty(FACT_PRODUCT_COST_EVENTS_PATH, cost_schema)
    if events.is_empty():
        return pl.DataFrame(schema=cost_schema)

    target_dt = date.fromisoformat(target_date)
    events = events.with_columns(
        pl.col('date').cast(pl.Date, strict=False),
        pl.col('product_id').cast(pl.Int64, strict=False),
        pl.col('source_move_id').cast(pl.Int64, strict=False),
        pl.col('source_tax_id').cast(pl.Int64, strict=False),
        pl.col('cost_unit_tax_in').cast(pl.Float64, strict=False).fill_null(0),
    )
    events = events.filter(pl.col('date') <= pl.lit(target_dt))
    if events.is_empty():
        return pl.DataFrame(schema=cost_schema)

    latest = (
        events.sort(['date', 'source_move_id'])
        .group_by('product_id')
        .agg([
            pl.last('cost_unit_tax_in').alias('cost_unit_tax_in'),
            pl.last('source_move_id').alias('source_move_id'),
            pl.last('source_tax_id').alias('source_tax_id'),
        ])
        .with_columns(pl.lit(target_dt).alias('date'))
        .select(['date', 'product_id', 'cost_unit_tax_in', 'source_move_id', 'source_tax_id'])
    )

    if latest.is_empty():
        return pl.DataFrame(schema=cost_schema)
    return latest


def _build_product_cost_latest_daily(target_date: str) -> pl.DataFrame:
    cost_schema = {
        'date': pl.Date,
        'product_id': pl.Int64,
        'cost_unit_tax_in': pl.Float64,
        'source_move_id': pl.Int64,
        'source_tax_id': pl.Int64,
    }

    target_dt = date.fromisoformat(target_date)
    prev_date = (target_dt - timedelta(days=1)).isoformat()
    prev_partition = _partition_path(FACT_PRODUCT_COST_LATEST_DAILY_PATH, prev_date)

    if not _has_parquet_files(prev_partition):
        return _build_cost_snapshot_from_events(target_date)

    prev_df = _read_parquet_or_empty(prev_partition, cost_schema)
    prev_df = prev_df.select([
        'product_id', 'cost_unit_tax_in', 'source_move_id', 'source_tax_id',
    ])

    today_partition = _partition_path(FACT_PRODUCT_COST_EVENTS_PATH, target_date)
    today_events = _read_parquet_or_empty(today_partition, cost_schema)
    today_events = today_events.with_columns(
        pl.col('product_id').cast(pl.Int64, strict=False),
        pl.col('source_move_id').cast(pl.Int64, strict=False),
        pl.col('source_tax_id').cast(pl.Int64, strict=False),
        pl.col('cost_unit_tax_in').cast(pl.Float64, strict=False).fill_null(0),
    )
    today_latest = _latest_cost_by_product(today_events)

    if prev_df.is_empty() and today_latest.is_empty():
        return pl.DataFrame(schema=cost_schema)

    merged = prev_df.join(today_latest, on='product_id', how='outer', suffix='_today')
    snapshot = (
        merged.with_columns(
            pl.coalesce([pl.col('cost_unit_tax_in_today'), pl.col('cost_unit_tax_in')]).alias('cost_unit_tax_in'),
            pl.coalesce([pl.col('source_move_id_today'), pl.col('source_move_id')]).alias('source_move_id'),
            pl.coalesce([pl.col('source_tax_id_today'), pl.col('source_tax_id')]).alias('source_tax_id'),
        )
        .select(['product_id', 'cost_unit_tax_in', 'source_move_id', 'source_tax_id'])
        .with_columns(pl.lit(target_dt).alias('date'))
        .select(['date', 'product_id', 'cost_unit_tax_in', 'source_move_id', 'source_tax_id'])
    )

    if snapshot.is_empty():
        return pl.DataFrame(schema=cost_schema)
    return snapshot


def _build_sales_lines_profit(target_date: str) -> pl.DataFrame:
    sales_schema = {
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
    }
    profit_schema = {
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

    target_dt = date.fromisoformat(target_date)
    pos_partition = _partition_path(f'{STAR_SCHEMA_PATH}/fact_sales', target_date)
    invoice_partition = _partition_path(f'{STAR_SCHEMA_PATH}/fact_invoice_sales', target_date)
    cost_partition = _partition_path(FACT_PRODUCT_COST_LATEST_DAILY_PATH, target_date)

    pos_df = _read_parquet_or_empty(pos_partition, sales_schema)
    invoice_df = _read_parquet_or_empty(invoice_partition, sales_schema)
    cost_df = _read_parquet_or_empty(cost_partition, {
        'date': pl.Date,
        'product_id': pl.Int64,
        'cost_unit_tax_in': pl.Float64,
        'source_move_id': pl.Int64,
        'source_tax_id': pl.Int64,
    })

    pos_lines = (
        pos_df.with_columns(
            pl.lit(target_dt).alias('date'),
            pl.col('order_id').cast(pl.Int64, strict=False).fill_null(0).alias('txn_id'),
            pl.col('line_id').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('product_id').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('revenue').cast(pl.Float64, strict=False).fill_null(0).alias('revenue_tax_in'),
        )
        .select(['date', 'txn_id', 'line_id', 'product_id', 'quantity', 'revenue_tax_in'])
    )

    invoice_lines = (
        invoice_df.with_columns(
            pl.lit(target_dt).alias('date'),
            pl.col('move_id').cast(pl.Int64, strict=False).fill_null(0).alias('txn_id'),
            pl.col('move_line_id').cast(pl.Int64, strict=False).fill_null(0).alias('line_id'),
            pl.col('product_id').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('price_unit').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('tax_id').cast(pl.Int64, strict=False).fill_null(0),
        )
        .with_columns(
            (
                pl.col('price_unit')
                * pl.col('quantity')
                * _tax_multiplier_expr('tax_id')
            ).alias('revenue_tax_in')
        )
        .select(['date', 'txn_id', 'line_id', 'product_id', 'quantity', 'revenue_tax_in'])
    )

    sales_lines = pl.concat([pos_lines, invoice_lines], how='vertical')
    if sales_lines.is_empty():
        return pl.DataFrame(schema=profit_schema)

    cost_df = cost_df.select([
        'product_id',
        'cost_unit_tax_in',
        pl.col('source_move_id').alias('source_cost_move_id'),
        pl.col('source_tax_id').alias('source_cost_tax_id'),
    ])

    merged = sales_lines.join(cost_df, on='product_id', how='left')
    merged = merged.with_columns(
        pl.col('cost_unit_tax_in').cast(pl.Float64, strict=False).fill_null(0),
        pl.col('source_cost_move_id').cast(pl.Int64, strict=False).fill_null(0),
        pl.col('source_cost_tax_id').cast(pl.Int64, strict=False).fill_null(0),
        (pl.col('cost_unit_tax_in') * pl.col('quantity')).alias('cogs_tax_in'),
    )
    merged = merged.with_columns(
        (pl.col('revenue_tax_in') - pl.col('cogs_tax_in')).alias('gross_profit')
    )

    profit_df = merged.select([
        'date',
        'txn_id',
        'line_id',
        'product_id',
        'quantity',
        'revenue_tax_in',
        'cost_unit_tax_in',
        'cogs_tax_in',
        'gross_profit',
        'source_cost_move_id',
        'source_cost_tax_id',
    ])

    if profit_df.is_empty():
        return pl.DataFrame(schema=profit_schema)
    return profit_df


def _build_profit_aggregates(profit_df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    daily_schema = {
        'date': pl.Date,
        'revenue_tax_in': pl.Float64,
        'cogs_tax_in': pl.Float64,
        'gross_profit': pl.Float64,
        'quantity': pl.Float64,
        'transactions': pl.Int64,
        'lines': pl.Int64,
    }
    by_product_schema = {
        'date': pl.Date,
        'product_id': pl.Int64,
        'revenue_tax_in': pl.Float64,
        'cogs_tax_in': pl.Float64,
        'gross_profit': pl.Float64,
        'quantity': pl.Float64,
        'lines': pl.Int64,
    }

    if profit_df.is_empty():
        return pl.DataFrame(schema=daily_schema), pl.DataFrame(schema=by_product_schema)

    daily = profit_df.group_by('date').agg([
        pl.sum('revenue_tax_in').alias('revenue_tax_in'),
        pl.sum('cogs_tax_in').alias('cogs_tax_in'),
        pl.sum('gross_profit').alias('gross_profit'),
        pl.sum('quantity').alias('quantity'),
        pl.col('txn_id').n_unique().alias('transactions'),
        pl.len().alias('lines'),
    ])

    by_product = profit_df.group_by(['date', 'product_id']).agg([
        pl.sum('revenue_tax_in').alias('revenue_tax_in'),
        pl.sum('cogs_tax_in').alias('cogs_tax_in'),
        pl.sum('gross_profit').alias('gross_profit'),
        pl.sum('quantity').alias('quantity'),
        pl.len().alias('lines'),
    ])

    return daily, by_product


@app.task
def update_product_cost_events(target_date: str) -> Optional[str]:
    try:
        df = _build_product_cost_events(target_date)
        return _write_partitioned(df, FACT_PRODUCT_COST_EVENTS_PATH, target_date, 'fact_product_cost_events')
    except Exception as exc:
        logger.error(f"Error updating product cost events for {target_date}: {exc}", exc_info=True)
        return None


@app.task
def update_product_cost_latest_daily(target_date: str) -> Optional[str]:
    try:
        df = _build_product_cost_latest_daily(target_date)
        return _write_partitioned(df, FACT_PRODUCT_COST_LATEST_DAILY_PATH, target_date, 'fact_product_cost_latest_daily')
    except Exception as exc:
        logger.error(f"Error updating latest daily cost for {target_date}: {exc}", exc_info=True)
        return None


@app.task
def update_sales_lines_profit(target_date: str) -> Optional[str]:
    try:
        df = _build_sales_lines_profit(target_date)
        return _write_partitioned(df, FACT_SALES_LINES_PROFIT_PATH, target_date, 'fact_sales_lines_profit')
    except Exception as exc:
        logger.error(f"Error updating sales-line profit for {target_date}: {exc}", exc_info=True)
        return None


@app.task
def update_profit_aggregates(target_date: str) -> Optional[Dict[str, str]]:
    try:
        profit_df = _build_sales_lines_profit(target_date)
        daily_df, by_product_df = _build_profit_aggregates(profit_df)
        daily_path = _write_partitioned(daily_df, AGG_PROFIT_DAILY_PATH, target_date, 'agg_profit_daily')
        by_product_path = _write_partitioned(by_product_df, AGG_PROFIT_DAILY_BY_PRODUCT_PATH, target_date, 'agg_profit_daily_by_product')
        return {'daily': daily_path, 'by_product': by_product_path}
    except Exception as exc:
        logger.error(f"Error updating profit aggregates for {target_date}: {exc}", exc_info=True)
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
def daily_stock_quants_pipeline(target_date: Optional[str] = None) -> str:
    """Daily pipeline for stock quant snapshots (stock.quant)."""
    from etl.pipelines.daily import daily_stock_quants_pipeline_impl
    return daily_stock_quants_pipeline_impl(target_date)


@app.task
def daily_profit_pipeline(target_date: Optional[str] = None) -> str:
    """Daily pipeline for cost/profit materialization."""
    from etl.pipelines.daily import daily_profit_pipeline_impl
    return daily_profit_pipeline_impl(target_date)


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


@app.task(name="catch_up_etl")
def catch_up_etl_legacy() -> Dict[str, Any]:
    """Backward-compatible task name alias for catch-up ETL."""
    return catch_up_etl()


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
    'daily-stock-quants-etl': {
        'task': 'etl_tasks.daily_stock_quants_pipeline',
        'schedule': crontab(hour=7, minute=0),
    },
    'daily-profit-etl': {
        'task': 'etl_tasks.daily_profit_pipeline',
        'schedule': crontab(hour=2, minute=20),
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
    'etl_tasks.extract_stock_quants': {'queue': 'extraction'},
    'etl_tasks.clean_pos_data': {'queue': 'transformation'},
    'etl_tasks.clean_sales_invoice_lines': {'queue': 'transformation'},
    'etl_tasks.clean_purchase_invoice_lines': {'queue': 'transformation'},
    'etl_tasks.clean_inventory_moves': {'queue': 'transformation'},
    'etl_tasks.clean_stock_quants': {'queue': 'transformation'},
    'etl_tasks.update_star_schema': {'queue': 'loading'},
    'etl_tasks.update_invoice_sales_star_schema': {'queue': 'loading'},
    'etl_tasks.update_purchase_star_schema': {'queue': 'loading'},
    'etl_tasks.update_inventory_moves_star_schema': {'queue': 'loading'},
    'etl_tasks.update_stock_quants_star_schema': {'queue': 'loading'},
    'etl_tasks.save_raw_data': {'queue': 'loading'},
    'etl_tasks.save_raw_sales_invoice_lines': {'queue': 'loading'},
    'etl_tasks.save_raw_purchase_invoice_lines': {'queue': 'loading'},
    'etl_tasks.save_raw_inventory_moves': {'queue': 'loading'},
    'etl_tasks.save_raw_stock_quants': {'queue': 'loading'},
    'etl_tasks.update_product_cost_events': {'queue': 'loading'},
    'etl_tasks.update_product_cost_latest_daily': {'queue': 'loading'},
    'etl_tasks.update_sales_lines_profit': {'queue': 'loading'},
    'etl_tasks.update_profit_aggregates': {'queue': 'loading'},
    'etl_tasks.refresh_dimensions_incremental': {'queue': 'dimensions'},
}