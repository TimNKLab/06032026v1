"""Raw data persistence layer.
Writes raw extraction results to the data lake.
"""
import logging
import os
from typing import Any, Dict, Optional

import polars as pl

from etl.config import (
    RAW_PATH,
    RAW_SALES_INVOICE_PATH,
    RAW_PURCHASES_PATH,
    RAW_INVENTORY_MOVES_PATH,
    RAW_STOCK_QUANTS_PATH,
)
from etl.io_parquet import atomic_write_parquet

logger = logging.getLogger(__name__)


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


def save_raw_sales_invoice_lines(extraction_result: Dict[str, Any]) -> Optional[str]:
    return _save_raw_account_move_lines(extraction_result, RAW_SALES_INVOICE_PATH, 'account_move_out_invoice_lines')


def save_raw_purchase_invoice_lines(extraction_result: Dict[str, Any]) -> Optional[str]:
    return _save_raw_account_move_lines(extraction_result, RAW_PURCHASES_PATH, 'account_move_in_invoice_lines')


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
            'product_id': pl.Int64,
            'location_id': pl.Int64,
            'lot_id': pl.Int64,
            'owner_id': pl.Int64,
            'company_id': pl.Int64,
            'quantity': pl.Float64,
            'reserved_quantity': pl.Float64,
            'snapshot_date': pl.Utf8,
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
