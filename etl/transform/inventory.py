"""Inventory (stock moves & quants) cleaning transforms."""
import logging
import os
from typing import Optional

import polars as pl

from etl.config import (
    CLEAN_INVENTORY_MOVES_PATH,
    CLEAN_STOCK_QUANTS_PATH,
    DIM_PRODUCTS_FILE,
    DIM_LOCATIONS_FILE,
    DIM_UOMS_FILE,
    DIM_PARTNERS_FILE,
)
from etl.io_parquet import atomic_write_parquet

logger = logging.getLogger(__name__)


def clean_stock_quants(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    """Read raw stock-quant parquet, cast types, write clean partition."""
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


def clean_inventory_moves(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    """Read raw inventory-move parquet, cast types, join dimensions, write clean partition."""
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
