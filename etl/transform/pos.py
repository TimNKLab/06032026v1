"""POS order-line cleaning transform."""
import logging
import os
from typing import Optional

import polars as pl

from etl.config import CLEAN_PATH
from etl.io_parquet import atomic_write_parquet
from etl.transform._utils import to_local_datetime

logger = logging.getLogger(__name__)


def clean_pos_data(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    """Read raw POS parquet, cast types, compute revenue, write clean partition."""
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
