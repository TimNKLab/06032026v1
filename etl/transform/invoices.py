"""Invoice (sales & purchase) line cleaning transforms."""
import logging
import os
from typing import Optional

import polars as pl

from etl.config import CLEAN_SALES_INVOICE_PATH, CLEAN_PURCHASES_PATH
from etl.io_parquet import atomic_write_parquet

logger = logging.getLogger(__name__)


def clean_sales_invoice_lines(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    """Read raw customer-invoice parquet, cast types, write clean partition."""
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


def clean_purchase_invoice_lines(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    """Read raw vendor-bill parquet, cast types, apply discount logic, write clean partition."""
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
