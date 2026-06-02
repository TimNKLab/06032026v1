"""Star-schema persistence layer.
Writes cleaned and transformed data into final fact tables.
"""
import os  
from typing import Optional  
  
import polars as pl  
  
from etl.config import STAR_SCHEMA_PATH  
from etl.io_parquet import atomic_write_parquet  
from etl.transform._utils import to_local_datetime  
  
  
def update_fact_inventory_moves(df: pl.DataFrame, target_date: str) -> str:  
    """Write cleaned inventory moves to the star-schema."""  
    fact_path = f'{STAR_SCHEMA_PATH}/fact_inventory_moves'  
    year, month, day = target_date.split('-')  
    fact_partition = f'{fact_path}/year={year}/month={month}/day={day}'  
    os.makedirs(fact_partition, exist_ok=True)  
  
    fact_output = f'{fact_partition}/fact_inventory_moves_{target_date}.parquet'  
    atomic_write_parquet(df, fact_output)  
    return fact_output  
  
  
def update_fact_sales_pos(df: pl.DataFrame, target_date: str) -> str:  
    """Write cleaned POS sales to the star-schema."""  
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
  
  
def update_fact_invoice_sales(df: pl.DataFrame, target_date: str) -> str:  
    """Write cleaned invoice sales to the star-schema."""  
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
  
  
def update_fact_purchases(df: pl.DataFrame, target_date: str) -> str:  
    """Write cleaned purchase invoices to the star-schema."""  
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
  
  
def update_fact_stock_on_hand_snapshot(df: pl.DataFrame, target_date: str) -> str:  
    """Write cleaned stock snapshots to the star-schema."""  
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
